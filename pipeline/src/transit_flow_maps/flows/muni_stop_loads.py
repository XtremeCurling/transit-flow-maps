"""Muni stop-load to segment throughput conversion."""

from __future__ import annotations

import json
import math
import re
from bisect import bisect_left, bisect_right
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import SupportsFloat, cast

import pandas as pd
from shapely.geometry import LineString, Point

from transit_flow_maps.conflation.h3_index import latlng_to_cell
from transit_flow_maps.conflation.segment_keys import (
    SegmentKeyConfig,
    build_segment_keys_for_shape,
)
from transit_flow_maps.conflation.simplify import clean_cell_sequence
from transit_flow_maps.gtfs.load_gtfs import (
    read_gtfs_table,
    read_optional_gtfs_table,
)
from transit_flow_maps.gtfs.routes import route_mode_lookup
from transit_flow_maps.util.config import RuntimeConfig, ensure_output_directories
from transit_flow_maps.util.crs import CRSContext, build_crs_context, choose_metric_crs
from transit_flow_maps.util.logging import get_logger


@dataclass(frozen=True)
class MuniFlowArtifacts:
    """Output paths produced by build-flows-muni."""

    segment_flows_path: Path
    unsnapped_path: Path
    degenerate_spans_path: Path
    excluded_route_directions_path: Path
    sanity_totals_path: Path
    shape_cache_path: Path
    rows_written: int


@dataclass
class CanonicalShape:
    """Cached canonical shape info for one route-direction."""

    route_id: str
    direction_id: str
    shape_id: str
    representative_trip_id: str
    trip_count: int
    mode: str
    densified_lonlat: list[tuple[float, float]]
    densified_xy: list[tuple[float, float]]
    measures_m: list[float]
    total_length_m: float
    line_xy: LineString
    crs_context: CRSContext


@dataclass(frozen=True)
class ShapeTransitionTrace:
    """Precomputed segment transitions for fast span allocation."""

    midpoint_measures_m: list[float]
    segment_ids: list[str]


def _segment_key_config(runtime_config: RuntimeConfig) -> SegmentKeyConfig:
    settings = runtime_config.settings
    return SegmentKeyConfig(
        h3_resolution=settings.h3_resolution,
        edge_pos_bins=settings.edge_pos_bins,
        bearing_bucket_count=settings.bearing_bucket_count,
        densify_spacing_m=settings.densify_spacing_m,
        non_neighbor_max_path_cells=settings.non_neighbor_max_path_cells,
        non_neighbor_max_recursion_depth=settings.non_neighbor_max_recursion_depth,
        crs_metric_default=settings.crs_metric_default,
    )


def _safe_float(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return 0.0
    try:
        parsed = float(cast(SupportsFloat | str, text))
    except ValueError:
        return 0.0
    if math.isnan(parsed):
        return 0.0
    return float(parsed)


def _safe_str(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() == "nan":
        return ""
    return text


def _canonicalize_column(name: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower())
    normalized = re.sub(r"_+", "_", normalized)
    return normalized.strip("_")


def _normalize_route_alpha(route_alpha: str) -> str:
    text = _safe_str(route_alpha).upper()
    match = re.match(r"^(\d+)(.*)$", text)
    if not match:
        return text

    digits = match.group(1)
    suffix = match.group(2)
    normalized_digits = str(int(digits))
    return f"{normalized_digits}{suffix}"


def _normalize_direction_label(direction_value: str) -> str:
    text = _safe_str(direction_value).lower()
    if text.startswith("inbound"):
        return "inbound"
    if text.startswith("outbound"):
        return "outbound"
    if text.startswith("north"):
        return "north"
    if text.startswith("south"):
        return "south"
    return text


def _normalize_stop_name(name: str) -> str:
    text = _safe_str(name).lower()
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _normalize_stop_identifier(value: object) -> str:
    text = _safe_str(value)
    if text == "":
        return ""
    if re.fullmatch(r"-?\d+\.0+", text):
        return str(int(float(text)))
    return text


def _token_overlap_ratio(a: str, b: str) -> float:
    a_tokens = set(token for token in a.split(" ") if token)
    b_tokens = set(token for token in b.split(" ") if token)
    if not a_tokens or not b_tokens:
        return 0.0
    return len(a_tokens & b_tokens) / float(max(len(a_tokens), len(b_tokens)))


def _haversine_m(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    r = 6_371_008.8
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (
        math.sin(dphi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    )
    return 2.0 * r * math.asin(min(1.0, math.sqrt(a)))


def _weekday_service_ids(calendar_df: pd.DataFrame | None) -> set[str]:
    if calendar_df is None or calendar_df.empty:
        return set()

    weekday_cols = ["monday", "tuesday", "wednesday", "thursday", "friday"]
    missing = [col for col in weekday_cols if col not in calendar_df.columns]
    if missing:
        return set()

    normalized = calendar_df.fillna("0").copy()
    for col in weekday_cols:
        normalized[col] = normalized[col].astype(str)

    mask = (
        (normalized["monday"] == "1")
        | (normalized["tuesday"] == "1")
        | (normalized["wednesday"] == "1")
        | (normalized["thursday"] == "1")
        | (normalized["friday"] == "1")
    )
    if "service_id" not in normalized.columns:
        return set()
    return set(normalized.loc[mask, "service_id"].astype(str))


def _shape_points_by_id(shapes_df: pd.DataFrame) -> dict[str, list[tuple[float, float]]]:
    output: dict[str, list[tuple[float, float]]] = {}
    for shape_id, group in shapes_df.groupby("shape_id", sort=True):
        ordered = group.sort_values("shape_pt_sequence")
        points = [
            (float(row["shape_pt_lon"]), float(row["shape_pt_lat"]))
            for _, row in ordered.iterrows()
        ]
        deduped: list[tuple[float, float]] = []
        for point in points:
            if not deduped or deduped[-1] != point:
                deduped.append(point)
        if len(deduped) >= 2:
            output[str(shape_id)] = deduped
    return output


def _densify_shape_cache(
    route_id: str,
    direction_id: str,
    shape_id: str,
    representative_trip_id: str,
    trip_count: int,
    mode: str,
    points_lonlat: list[tuple[float, float]],
    segment_config: SegmentKeyConfig,
) -> CanonicalShape:
    metric_crs = choose_metric_crs(points_lonlat, segment_config.crs_metric_default)
    crs_context = build_crs_context(metric_crs)

    from transit_flow_maps.conflation.densify import densify_linestring_lonlat

    densified = densify_linestring_lonlat(
        points_lonlat,
        spacing_m=segment_config.densify_spacing_m,
        to_metric=crs_context.to_metric,
        to_wgs84=crs_context.to_wgs84,
    )

    densified_xy: list[tuple[float, float]] = []
    for lon, lat in densified:
        x, y = crs_context.to_metric.transform(lon, lat)
        densified_xy.append((float(x), float(y)))

    measures = [0.0]
    for idx in range(1, len(densified_xy)):
        dx = densified_xy[idx][0] - densified_xy[idx - 1][0]
        dy = densified_xy[idx][1] - densified_xy[idx - 1][1]
        measures.append(measures[-1] + float(math.hypot(dx, dy)))

    return CanonicalShape(
        route_id=route_id,
        direction_id=direction_id,
        shape_id=shape_id,
        representative_trip_id=representative_trip_id,
        trip_count=trip_count,
        mode=mode,
        densified_lonlat=densified,
        densified_xy=densified_xy,
        measures_m=measures,
        total_length_m=measures[-1],
        line_xy=LineString(densified_xy),
        crs_context=crs_context,
    )


def _build_canonical_shapes(
    trips: pd.DataFrame,
    shapes_df: pd.DataFrame,
    routes_df: pd.DataFrame,
    calendar_df: pd.DataFrame | None,
    segment_config: SegmentKeyConfig,
) -> tuple[dict[tuple[str, str], CanonicalShape], pd.DataFrame]:
    weekday_services = _weekday_service_ids(calendar_df)

    candidate_trips = trips.copy()
    if weekday_services and "service_id" in candidate_trips.columns:
        filtered = candidate_trips[candidate_trips["service_id"].astype(str).isin(weekday_services)]
        if not filtered.empty:
            candidate_trips = filtered

    candidate_trips = candidate_trips.dropna(subset=["route_id", "direction_id", "shape_id"])
    grouped = (
        candidate_trips.groupby(["route_id", "direction_id", "shape_id"], as_index=False)
        .agg(trip_count=("trip_id", "size"))
        .sort_values(
            ["route_id", "direction_id", "trip_count", "shape_id"],
            ascending=[True, True, False, True],
        )
    )

    canonical = grouped.drop_duplicates(subset=["route_id", "direction_id"], keep="first")
    representative_trip_lookup: dict[tuple[str, str, str], str] = (
        candidate_trips.groupby(["route_id", "direction_id", "shape_id"])["trip_id"]
        .min()
        .astype(str)
        .to_dict()
    )

    mode_lookup = route_mode_lookup(routes_df)
    shape_points = _shape_points_by_id(shapes_df)

    cache: dict[tuple[str, str], CanonicalShape] = {}
    cache_rows: list[dict[str, object]] = []

    for _, row in canonical.iterrows():
        route_id = str(row["route_id"])
        direction_id = str(row["direction_id"])
        shape_id = str(row["shape_id"])
        representative_trip_id = representative_trip_lookup.get(
            (route_id, direction_id, shape_id),
            "",
        )
        trip_count = int(row["trip_count"])

        points = shape_points.get(shape_id)
        if points is None:
            continue

        shape_cache = _densify_shape_cache(
            route_id=route_id,
            direction_id=direction_id,
            shape_id=shape_id,
            representative_trip_id=representative_trip_id,
            trip_count=trip_count,
            mode=mode_lookup.get(route_id, "unknown"),
            points_lonlat=points,
            segment_config=segment_config,
        )
        cache[(route_id, direction_id)] = shape_cache

        h3_cells = [
            latlng_to_cell(lat, lon, segment_config.h3_resolution)
            for lon, lat in shape_cache.densified_lonlat
        ]
        clean_cells = clean_cell_sequence(h3_cells)

        lookup_sample = [
            [round(shape_cache.measures_m[i], 3), i]
            for i in range(0, len(shape_cache.measures_m), 5)
        ]
        if lookup_sample[-1][1] != len(shape_cache.measures_m) - 1:
            lookup_sample.append(
                [round(shape_cache.measures_m[-1], 3), len(shape_cache.measures_m) - 1]
            )

        cache_rows.append(
            {
                "route_id": route_id,
                "direction_id": direction_id,
                "shape_id": shape_id,
                "trip_count": trip_count,
                "mode": shape_cache.mode,
                "densified_lonlat_json": json.dumps(shape_cache.densified_lonlat),
                "measure_m_json": json.dumps(shape_cache.measures_m),
                "h3_cells_json": json.dumps(clean_cells),
                "measure_index_lookup_json": json.dumps(lookup_sample),
            }
        )

    cache_df = pd.DataFrame(cache_rows).sort_values(["route_id", "direction_id"])
    return cache, cache_df


def _prepare_stops(
    stops_df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, dict[str, object]], dict[str, str]]:
    required = ["stop_id", "stop_name", "stop_lat", "stop_lon"]
    subset = stops_df[required + [col for col in ["stop_code"] if col in stops_df.columns]].copy()
    subset = subset.dropna(subset=["stop_id", "stop_name", "stop_lat", "stop_lon"])
    subset["stop_id"] = subset["stop_id"].astype(str)
    if "stop_code" not in subset.columns:
        subset["stop_code"] = ""
    subset["stop_code"] = subset["stop_code"].fillna("").astype(str)
    subset["stop_name"] = subset["stop_name"].astype(str)
    subset["stop_lat"] = pd.to_numeric(subset["stop_lat"], errors="coerce")
    subset["stop_lon"] = pd.to_numeric(subset["stop_lon"], errors="coerce")
    subset = subset.dropna(subset=["stop_lat", "stop_lon"])

    subset["stop_name_norm"] = subset["stop_name"].map(_normalize_stop_name)

    stop_by_id: dict[str, dict[str, object]] = {}
    for _, row in subset.iterrows():
        stop_id = str(row["stop_id"])
        stop_by_id[stop_id] = {
            "stop_id": stop_id,
            "stop_code": str(row["stop_code"]),
            "stop_name": str(row["stop_name"]),
            "stop_name_norm": str(row["stop_name_norm"]),
            "stop_lat": float(row["stop_lat"]),
            "stop_lon": float(row["stop_lon"]),
        }

    stop_code_to_id: dict[str, str] = {}
    for _, row in subset.iterrows():
        stop_code = str(row["stop_code"]).strip()
        if stop_code == "":
            continue
        if stop_code not in stop_code_to_id:
            stop_code_to_id[stop_code] = str(row["stop_id"])

    return subset, stop_by_id, stop_code_to_id


def _resolve_direction_id(
    route_id: str,
    direction_label: str,
    directions_df: pd.DataFrame,
) -> str | None:
    route_rows = directions_df[directions_df["route_id"].astype(str) == route_id]
    if route_rows.empty:
        return None

    labeled = route_rows.copy()
    labeled["direction_norm"] = labeled["direction"].astype(str).map(_normalize_direction_label)
    match = labeled[labeled["direction_norm"] == direction_label]
    if not match.empty:
        values = sorted(match["direction_id"].astype(str).unique())
        return str(values[0])

    candidates = sorted(labeled["direction_id"].astype(str).unique())
    if len(candidates) == 1:
        return str(candidates[0])

    if direction_label in {"outbound", "north"} and "0" in candidates:
        return "0"
    if direction_label in {"inbound", "south"} and "1" in candidates:
        return "1"

    return None


def _interpolate_xy_on_measure(shape: CanonicalShape, measure: float) -> tuple[float, float]:
    m = max(0.0, min(shape.total_length_m, measure))
    measures = shape.measures_m

    idx = bisect_right(measures, m) - 1
    idx = max(0, min(idx, len(measures) - 2))

    m0 = measures[idx]
    m1 = measures[idx + 1]
    p0 = shape.densified_xy[idx]
    p1 = shape.densified_xy[idx + 1]

    if m1 <= m0:
        return p0

    frac = (m - m0) / (m1 - m0)
    x = p0[0] + (p1[0] - p0[0]) * frac
    y = p0[1] + (p1[1] - p0[1]) * frac
    return (x, y)


def _interpolate_lonlat_on_measure(shape: CanonicalShape, measure: float) -> tuple[float, float]:
    x, y = _interpolate_xy_on_measure(shape, measure)
    lon, lat = shape.crs_context.to_wgs84.transform(x, y)
    return (float(lon), float(lat))


def _slice_shape_subpath(
    shape: CanonicalShape,
    measure_a: float,
    measure_b: float,
) -> list[tuple[float, float]]:
    lo = max(0.0, min(measure_a, measure_b))
    hi = min(shape.total_length_m, max(measure_a, measure_b))

    if hi <= lo:
        return []

    measures = shape.measures_m
    start_idx = bisect_left(measures, lo)
    end_idx = bisect_right(measures, hi)

    points: list[tuple[float, float]] = [_interpolate_lonlat_on_measure(shape, lo)]
    points.extend(shape.densified_lonlat[start_idx:end_idx])
    points.append(_interpolate_lonlat_on_measure(shape, hi))

    deduped: list[tuple[float, float]] = []
    for point in points:
        if not deduped or deduped[-1] != point:
            deduped.append(point)
    return deduped


def _project_stop_measure(
    shape: CanonicalShape,
    stop_lon: float,
    stop_lat: float,
) -> tuple[float, float]:
    x, y = shape.crs_context.to_metric.transform(stop_lon, stop_lat)
    stop_point = Point((float(x), float(y)))
    measure = float(shape.line_xy.project(stop_point))
    nearest = shape.line_xy.interpolate(measure)
    distance = float(stop_point.distance(nearest))
    return measure, distance


def _choose_column(columns: set[str], candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def _build_route_direction_metrics(
    representative_stop_times: pd.DataFrame | None,
    stop_by_id: dict[str, dict[str, object]],
    shape_cache: CanonicalShape,
) -> tuple[int, int]:
    if representative_stop_times is None or representative_stop_times.empty:
        return (0, 0)

    representative = representative_stop_times.copy()

    representative["stop_sequence"] = pd.to_numeric(
        representative["stop_sequence"],
        errors="coerce",
    )
    representative = representative.dropna(subset=["stop_sequence", "stop_id"])
    representative = representative.sort_values("stop_sequence")

    if "shape_dist_traveled" in representative.columns:
        dist = pd.to_numeric(representative["shape_dist_traveled"], errors="coerce")
        dist_values = [float(v) for v in dist.dropna().tolist()]
        if len(dist_values) >= 2:
            positive = 0
            negative = 0
            for idx in range(len(dist_values) - 1):
                delta = dist_values[idx + 1] - dist_values[idx]
                if delta > 1e-6:
                    positive += 1
                elif delta < -1e-6:
                    negative += 1
            if positive != negative:
                return positive, negative

    measures: list[float] = []
    for _, row in representative.iterrows():
        stop_id = str(row["stop_id"])
        stop = stop_by_id.get(stop_id)
        if stop is None:
            continue
        measure, _ = _project_stop_measure(
            shape_cache,
            stop_lon=_safe_float(stop["stop_lon"]),
            stop_lat=_safe_float(stop["stop_lat"]),
        )
        measures.append(measure)

    positive = 0
    negative = 0
    for idx in range(len(measures) - 1):
        delta = measures[idx + 1] - measures[idx]
        if delta > 1.0:
            positive += 1
        elif delta < -1.0:
            negative += 1

    return positive, negative


def _build_shape_transition_trace(
    shape_cache: CanonicalShape,
    segment_config: SegmentKeyConfig,
) -> ShapeTransitionTrace:
    result = build_segment_keys_for_shape(
        shape_id=shape_cache.shape_id,
        shape_points_lonlat=shape_cache.densified_lonlat,
        config=segment_config,
        pre_densified=True,
    )

    pairs = sorted(
        [
            (_safe_float(row["midpoint_measure_m"]), str(row["segment_id"]))
            for row in result.transition_records
        ],
        key=lambda item: item[0],
    )
    return ShapeTransitionTrace(
        midpoint_measures_m=[item[0] for item in pairs],
        segment_ids=[item[1] for item in pairs],
    )


def build_muni_flows(runtime_config: RuntimeConfig, input_file: Path) -> MuniFlowArtifacts:
    """Build Muni segment throughput from stop load observations."""
    logger = get_logger(__name__)
    ensure_output_directories(runtime_config.paths)

    sfmta_zip = runtime_config.paths.sfmta_gtfs_zip
    if not sfmta_zip.exists():
        raise FileNotFoundError(f"SFMTA GTFS zip missing: {sfmta_zip}")

    logger.info("Loading SFMTA GTFS tables for Muni flow allocation")
    shapes = read_gtfs_table(sfmta_zip, "shapes.txt")
    trips = read_gtfs_table(sfmta_zip, "trips.txt")
    routes = read_gtfs_table(sfmta_zip, "routes.txt")
    stops = read_gtfs_table(sfmta_zip, "stops.txt")
    stop_times = read_gtfs_table(sfmta_zip, "stop_times.txt")
    directions = read_optional_gtfs_table(sfmta_zip, "directions.txt")
    calendar = read_optional_gtfs_table(sfmta_zip, "calendar.txt")

    if directions is None:
        directions = pd.DataFrame(columns=["route_id", "direction_id", "direction"])

    segment_config = _segment_key_config(runtime_config)

    shape_cache_by_route_dir, shape_cache_df = _build_canonical_shapes(
        trips=trips,
        shapes_df=shapes,
        routes_df=routes,
        calendar_df=calendar,
        segment_config=segment_config,
    )
    shape_cache_path = runtime_config.paths.interim_dir / "shape_cache.parquet"
    shape_cache_df.to_parquet(shape_cache_path, index=False)

    logger.info("Precomputing shape transition traces")
    shape_transition_traces: dict[tuple[str, str], ShapeTransitionTrace] = {}
    for key, shape_cache in shape_cache_by_route_dir.items():
        shape_transition_traces[key] = _build_shape_transition_trace(shape_cache, segment_config)

    logger.info("Loading Muni stop-load CSV: %s", input_file)
    raw = pd.read_csv(input_file)
    canonical_columns = {_canonicalize_column(col): col for col in raw.columns}
    lowered_columns = set(canonical_columns.keys())

    route_col = _choose_column(lowered_columns, ["route_alpha", "route_id", "route"])
    direction_col = _choose_column(lowered_columns, ["direction", "dir", "direction_name"])
    stop_id_col = _choose_column(lowered_columns, ["stop_id", "bs_id"])
    stop_code_col = _choose_column(lowered_columns, ["stop_code", "stopcode"])
    stop_name_col = _choose_column(lowered_columns, ["stop_name", "stop"])
    lat_col = _choose_column(lowered_columns, ["stop_lat", "lat", "latitude"])
    lon_col = _choose_column(lowered_columns, ["stop_long", "stop_lon", "lon", "longitude"])
    ons_col = _choose_column(lowered_columns, ["avg_veh_ons", "board", "boards"])
    offs_col = _choose_column(lowered_columns, ["avg_veh_offs", "alight", "alights"])
    dep_load_col = _choose_column(lowered_columns, ["avg_all_dep_loads", "avg_veh_dep_load"])

    if route_col is None or direction_col is None or stop_id_col is None:
        raise ValueError(
            "Missing required Muni columns. Need route/direction/stop identifier columns."
        )

    route_src = canonical_columns[route_col]
    direction_src = canonical_columns[direction_col]
    stop_id_src = canonical_columns[stop_id_col]
    stop_code_src = canonical_columns[stop_code_col] if stop_code_col else None
    stop_name_src = canonical_columns[stop_name_col] if stop_name_col else None
    lat_src = canonical_columns[lat_col] if lat_col else None
    lon_src = canonical_columns[lon_col] if lon_col else None
    ons_src = canonical_columns[ons_col] if ons_col else None
    offs_src = canonical_columns[offs_col] if offs_col else None
    dep_load_src = canonical_columns[dep_load_col] if dep_load_col else None

    prepared_stops, stop_by_id, stop_code_to_id = _prepare_stops(stops)

    representative_trip_ids = sorted(
        {
            shape_cache.representative_trip_id
            for shape_cache in shape_cache_by_route_dir.values()
            if shape_cache.representative_trip_id
        }
    )
    representative_stop_times = stop_times[
        stop_times["trip_id"].astype(str).isin(set(representative_trip_ids))
    ].copy()
    representative_by_trip: dict[str, pd.DataFrame] = {
        str(trip_id): group.copy()
        for trip_id, group in representative_stop_times.groupby("trip_id", sort=False)
    }

    orientation_sign_by_route_dir: dict[tuple[str, str], int] = {}
    for key, shape_cache in shape_cache_by_route_dir.items():
        rep_df = representative_by_trip.get(shape_cache.representative_trip_id)
        positive_deltas, negative_deltas = _build_route_direction_metrics(
            rep_df,
            stop_by_id,
            shape_cache,
        )
        if positive_deltas == negative_deltas:
            continue
        orientation_sign_by_route_dir[key] = 1 if positive_deltas > negative_deltas else -1

    unsnapped_rows: list[dict[str, object]] = []
    mapped_rows: list[dict[str, object]] = []

    for _, row in raw.iterrows():
        route_id = _normalize_route_alpha(_safe_str(row[route_src]))
        direction_label = _normalize_direction_label(_safe_str(row[direction_src]))

        raw_stop_id = _normalize_stop_identifier(row[stop_id_src])
        raw_stop_code = (
            _normalize_stop_identifier(row[stop_code_src]) if stop_code_src else ""
        )
        raw_stop_name = _safe_str(row[stop_name_src]) if stop_name_src else ""
        raw_stop_lat = _safe_float(row[lat_src]) if lat_src else float("nan")
        raw_stop_lon = _safe_float(row[lon_src]) if lon_src else float("nan")

        gtfs_stop_id: str | None = None
        mapping_method = ""

        if raw_stop_id and raw_stop_id in stop_by_id:
            gtfs_stop_id = raw_stop_id
            mapping_method = "stop_id"

        if gtfs_stop_id is None and raw_stop_code:
            matched = stop_code_to_id.get(raw_stop_code)
            if matched is not None:
                gtfs_stop_id = matched
                mapping_method = "stop_code"

        if gtfs_stop_id is None and raw_stop_id:
            matched = stop_code_to_id.get(raw_stop_id)
            if matched is not None:
                gtfs_stop_id = matched
                mapping_method = "stop_code_from_bs_id"

        if (
            gtfs_stop_id is None
            and raw_stop_name
            and not math.isnan(raw_stop_lat)
            and not math.isnan(raw_stop_lon)
        ):
            name_norm = _normalize_stop_name(raw_stop_name)
            candidate: tuple[str, float] | None = None
            for _, stop_row in prepared_stops.iterrows():
                stop_name_norm = str(stop_row["stop_name_norm"])
                overlap = _token_overlap_ratio(name_norm, stop_name_norm)
                exact_match = name_norm == stop_name_norm
                if not exact_match and overlap < 0.8:
                    continue

                stop_lon = float(stop_row["stop_lon"])
                stop_lat = float(stop_row["stop_lat"])
                distance = _haversine_m(raw_stop_lon, raw_stop_lat, stop_lon, stop_lat)
                if distance > runtime_config.settings.snap_hard_cap_m:
                    continue

                stop_id = str(stop_row["stop_id"])
                score = distance
                if candidate is None or score < candidate[1] or (
                    score == candidate[1] and stop_id < candidate[0]
                ):
                    candidate = (stop_id, score)

            if candidate is not None:
                gtfs_stop_id = candidate[0]
                mapping_method = "stop_name_nearest"

        if gtfs_stop_id is None:
            unsnapped_rows.append(
                {
                    "route_id": route_id,
                    "direction": direction_label,
                    "source_stop_id": raw_stop_id,
                    "source_stop_name": raw_stop_name,
                    "reason": "no_gtfs_match",
                    "distance_m": "",
                }
            )
            continue

        stop_info = stop_by_id[gtfs_stop_id]
        distance_m: object = ""
        if not math.isnan(raw_stop_lat) and not math.isnan(raw_stop_lon):
            distance = _haversine_m(
                raw_stop_lon,
                raw_stop_lat,
                _safe_float(stop_info["stop_lon"]),
                _safe_float(stop_info["stop_lat"]),
            )
            distance_m = round(distance, 3)
            if distance > runtime_config.settings.snap_hard_cap_m:
                unsnapped_rows.append(
                    {
                        "route_id": route_id,
                        "direction": direction_label,
                        "source_stop_id": raw_stop_id,
                        "source_stop_name": raw_stop_name,
                        "reason": "too_far",
                        "distance_m": distance_m,
                    }
                )
                continue

        mapped_rows.append(
            {
                "route_id": route_id,
                "direction_label": direction_label,
                "gtfs_stop_id": gtfs_stop_id,
                "board": _safe_float(row[ons_src]) if ons_src else 0.0,
                "alight": _safe_float(row[offs_src]) if offs_src else 0.0,
                "dep_load": _safe_float(row[dep_load_src]) if dep_load_src else float("nan"),
                "mapping_method": mapping_method,
                "snap_distance_m": distance_m,
            }
        )

    if not mapped_rows:
        raise RuntimeError("No Muni stop rows could be mapped to GTFS stops")

    mapped_df = pd.DataFrame(mapped_rows)
    agg = (
        mapped_df.groupby(["route_id", "direction_label", "gtfs_stop_id"], as_index=False)
        .agg(
            board=("board", "sum"),
            alight=("alight", "sum"),
            dep_load=("dep_load", "sum"),
            snap_distance_m=("snap_distance_m", "first"),
        )
        .sort_values(["route_id", "direction_label", "gtfs_stop_id"])
    )

    excluded_rows: list[dict[str, object]] = []
    degenerate_rows: list[dict[str, object]] = []
    segment_contrib: dict[str, float] = defaultdict(float)
    segment_routes: dict[str, set[str]] = defaultdict(set)
    segment_modes: dict[str, set[str]] = defaultdict(set)

    all_route_directions = sorted(
        {
            (str(r), str(d))
            for r, d in zip(agg["route_id"], agg["direction_label"], strict=False)
        }
    )
    included_count = 0

    for route_id, direction_label in all_route_directions:
        direction_id = _resolve_direction_id(route_id, direction_label, directions)
        if direction_id is None:
            excluded_rows.append(
                {
                    "route_id": route_id,
                    "direction_label": direction_label,
                    "direction_id": "",
                    "reason": "direction_unresolved",
                }
            )
            continue

        route_shape_cache = shape_cache_by_route_dir.get((route_id, direction_id))
        if route_shape_cache is None:
            excluded_rows.append(
                {
                    "route_id": route_id,
                    "direction_label": direction_label,
                    "direction_id": direction_id,
                    "reason": "canonical_shape_missing",
                }
            )
            continue
        shape_trace = shape_transition_traces.get((route_id, direction_id))
        if shape_trace is None:
            excluded_rows.append(
                {
                    "route_id": route_id,
                    "direction_label": direction_label,
                    "direction_id": direction_id,
                    "reason": "shape_trace_missing",
                }
            )
            continue

        orientation_sign = orientation_sign_by_route_dir.get((route_id, direction_id))
        if orientation_sign is None:
            excluded_rows.append(
                {
                    "route_id": route_id,
                    "direction_label": direction_label,
                    "direction_id": direction_id,
                    "reason": "orientation_ambiguous",
                }
            )
            continue

        local = agg[
            (agg["route_id"] == route_id)
            & (agg["direction_label"] == direction_label)
        ].copy()

        projected_rows: list[dict[str, object]] = []
        for _, stop_row in local.iterrows():
            stop_id = str(stop_row["gtfs_stop_id"])
            local_stop_info = stop_by_id.get(stop_id)
            if local_stop_info is None:
                unsnapped_rows.append(
                    {
                        "route_id": route_id,
                        "direction": direction_label,
                        "source_stop_id": stop_id,
                        "source_stop_name": "",
                        "reason": "shape_projection_error",
                        "distance_m": "",
                    }
                )
                continue

            try:
                measure_m, projection_distance = _project_stop_measure(
                    route_shape_cache,
                    stop_lon=_safe_float(local_stop_info["stop_lon"]),
                    stop_lat=_safe_float(local_stop_info["stop_lat"]),
                )
            except Exception:
                unsnapped_rows.append(
                    {
                        "route_id": route_id,
                        "direction": direction_label,
                        "source_stop_id": stop_id,
                        "source_stop_name": str(local_stop_info["stop_name"]),
                        "reason": "shape_projection_error",
                        "distance_m": "",
                    }
                )
                continue

            oriented_measure = measure_m
            if orientation_sign < 0:
                oriented_measure = route_shape_cache.total_length_m - measure_m

            projected_rows.append(
                {
                    "stop_id": stop_id,
                    "stop_name": str(local_stop_info["stop_name"]),
                    "measure_m": measure_m,
                    "oriented_measure_m": oriented_measure,
                    "projection_distance_m": projection_distance,
                    "board": _safe_float(stop_row["board"]),
                    "alight": _safe_float(stop_row["alight"]),
                    "dep_load": _safe_float(stop_row["dep_load"]),
                }
            )

        if not projected_rows:
            excluded_rows.append(
                {
                    "route_id": route_id,
                    "direction_label": direction_label,
                    "direction_id": direction_id,
                    "reason": "no_projectable_stops",
                }
            )
            continue

        projected_df = pd.DataFrame(projected_rows).sort_values(["oriented_measure_m", "stop_id"])

        deduped_rows: list[dict[str, object]] = []
        for _, stop_row in projected_df.iterrows():
            if not deduped_rows:
                deduped_rows.append(stop_row.to_dict())
                continue

            prev = deduped_rows[-1]
            if (
                abs(
                    _safe_float(stop_row["oriented_measure_m"])
                    - _safe_float(prev["oriented_measure_m"])
                )
                <= runtime_config.settings.canonical_stop_dedupe_m
            ):
                continue
            deduped_rows.append(stop_row.to_dict())

        if len(deduped_rows) < 2:
            excluded_rows.append(
                {
                    "route_id": route_id,
                    "direction_label": direction_label,
                    "direction_id": direction_id,
                    "reason": "insufficient_ordered_stops",
                }
            )
            continue

        # Fallback onboard trajectory in canonical stop order.
        onboard_after: dict[str, float] = {}
        onboard = 0.0
        for stop_row in deduped_rows:
            onboard += _safe_float(stop_row["board"]) - _safe_float(stop_row["alight"])
            onboard_after[str(stop_row["stop_id"])] = onboard

        included_count += 1

        for idx in range(len(deduped_rows) - 1):
            current = deduped_rows[idx]
            nxt = deduped_rows[idx + 1]
            current_oriented = _safe_float(current["oriented_measure_m"])
            next_oriented = _safe_float(nxt["oriented_measure_m"])

            if (
                next_oriented
                <= current_oriented + runtime_config.settings.degenerate_span_epsilon_m
            ):
                degenerate_rows.append(
                    {
                        "route_id": route_id,
                        "direction_label": direction_label,
                        "direction_id": direction_id,
                        "shape_id": route_shape_cache.shape_id,
                        "from_stop_id": str(current["stop_id"]),
                        "to_stop_id": str(nxt["stop_id"]),
                        "from_measure_m": round(_safe_float(current["measure_m"]), 3),
                        "to_measure_m": round(_safe_float(nxt["measure_m"]), 3),
                        "reason": "degenerate_span",
                    }
                )
                unsnapped_rows.append(
                    {
                        "route_id": route_id,
                        "direction": direction_label,
                        "source_stop_id": str(current["stop_id"]),
                        "source_stop_name": str(current["stop_name"]),
                        "reason": "degenerate_span",
                        "distance_m": "",
                    }
                )
                continue

            lo_m = min(_safe_float(current["measure_m"]), _safe_float(nxt["measure_m"]))
            hi_m = max(_safe_float(current["measure_m"]), _safe_float(nxt["measure_m"]))
            if hi_m <= lo_m:
                degenerate_rows.append(
                    {
                        "route_id": route_id,
                        "direction_label": direction_label,
                        "direction_id": direction_id,
                        "shape_id": route_shape_cache.shape_id,
                        "from_stop_id": str(current["stop_id"]),
                        "to_stop_id": str(nxt["stop_id"]),
                        "from_measure_m": round(_safe_float(current["measure_m"]), 3),
                        "to_measure_m": round(_safe_float(nxt["measure_m"]), 3),
                        "reason": "shape_projection_error",
                    }
                )
                continue

            span_start_idx = bisect_left(shape_trace.midpoint_measures_m, lo_m)
            span_end_idx = bisect_right(shape_trace.midpoint_measures_m, hi_m)
            span_segment_ids = shape_trace.segment_ids[span_start_idx:span_end_idx]
            if not span_segment_ids:
                continue

            if math.isnan(_safe_float(current["dep_load"])):
                span_throughput = onboard_after.get(str(current["stop_id"]), 0.0)
            else:
                span_throughput = _safe_float(current["dep_load"])
            span_throughput = max(0.0, span_throughput)

            for segment_id in span_segment_ids:
                segment_contrib[segment_id] += span_throughput
                segment_routes[segment_id].add(route_id)
                segment_modes[segment_id].add(route_shape_cache.mode)

    segment_rows: list[dict[str, object]] = []
    for segment_id in sorted(segment_contrib):
        modes = sorted(segment_modes[segment_id])
        mode_value = modes[0] if len(modes) == 1 else "mixed"
        routes = sorted(segment_routes[segment_id])
        segment_rows.append(
            {
                "segment_id": segment_id,
                "daily_riders": float(segment_contrib[segment_id]),
                "agency": "SFMTA",
                "mode": mode_value,
                "routes_json": json.dumps(routes),
                "time_basis": "weekday_average",
            }
        )

    if not segment_rows:
        raise RuntimeError("No Muni segment flow rows were generated")

    muni_segment_df = pd.DataFrame(segment_rows)

    segment_flows_path = runtime_config.paths.interim_dir / "segment_flows.parquet"
    if segment_flows_path.exists():
        existing = pd.read_parquet(segment_flows_path)
        if "agency" in existing.columns:
            existing = existing[existing["agency"].astype(str) != "SFMTA"]
        merged = pd.concat([existing, muni_segment_df], ignore_index=True)
    else:
        merged = muni_segment_df

    merged = merged[
        ["segment_id", "daily_riders", "agency", "mode", "routes_json", "time_basis"]
    ].sort_values(["agency", "segment_id"])
    merged.to_parquet(segment_flows_path, index=False)

    unsnapped_path = runtime_config.paths.debug_dir / "unsnapped_or_far_snaps.csv"
    unsnapped_df = pd.DataFrame(unsnapped_rows)
    if unsnapped_df.empty:
        unsnapped_df = pd.DataFrame(
            columns=[
                "route_id",
                "direction",
                "source_stop_id",
                "source_stop_name",
                "reason",
                "distance_m",
            ]
        )
    unsnapped_df.to_csv(unsnapped_path, index=False)

    degenerate_spans_path = runtime_config.paths.debug_dir / "degenerate_spans.csv"
    degenerate_df = pd.DataFrame(degenerate_rows)
    if degenerate_df.empty:
        degenerate_df = pd.DataFrame(
            columns=[
                "route_id",
                "direction_label",
                "direction_id",
                "shape_id",
                "from_stop_id",
                "to_stop_id",
                "from_measure_m",
                "to_measure_m",
                "reason",
            ]
        )
    degenerate_df.to_csv(degenerate_spans_path, index=False)

    excluded_route_directions_path = (
        runtime_config.paths.debug_dir / "excluded_route_directions.csv"
    )
    excluded_df = pd.DataFrame(excluded_rows)
    if excluded_df.empty:
        excluded_df = pd.DataFrame(
            columns=["route_id", "direction_label", "direction_id", "reason"]
        )
    excluded_df.to_csv(excluded_route_directions_path, index=False)

    sanity_totals_path = runtime_config.paths.debug_dir / "sanity_totals.csv"
    top_segments = (
        muni_segment_df.sort_values("daily_riders", ascending=False)
        .head(10)
        .reset_index(drop=True)
    )

    sanity_rows: list[dict[str, object]] = [
        {
            "metric": "route_direction_total",
            "value": len(all_route_directions),
            "segment_id": "",
            "daily_riders": "",
        },
        {
            "metric": "route_direction_included",
            "value": included_count,
            "segment_id": "",
            "daily_riders": "",
        },
        {
            "metric": "route_direction_excluded",
            "value": int(len(excluded_df)),
            "segment_id": "",
            "daily_riders": "",
        },
        {
            "metric": "dropped_stops",
            "value": (
                int((unsnapped_df["reason"] == "no_gtfs_match").sum())
                if not unsnapped_df.empty
                else 0
            ),
            "segment_id": "",
            "daily_riders": "",
        },
        {
            "metric": "muni_segment_rows",
            "value": int(len(muni_segment_df)),
            "segment_id": "",
            "daily_riders": "",
        },
    ]

    for idx, row in top_segments.iterrows():
        sanity_rows.append(
            {
                "metric": f"top_segment_{idx + 1}",
                "value": "",
                "segment_id": str(row["segment_id"]),
                "daily_riders": round(float(row["daily_riders"]), 3),
            }
        )

    pd.DataFrame(sanity_rows).to_csv(sanity_totals_path, index=False)

    logger.info("Muni segment flow rows written: %s", len(muni_segment_df))

    return MuniFlowArtifacts(
        segment_flows_path=segment_flows_path,
        unsnapped_path=unsnapped_path,
        degenerate_spans_path=degenerate_spans_path,
        excluded_route_directions_path=excluded_route_directions_path,
        sanity_totals_path=sanity_totals_path,
        shape_cache_path=shape_cache_path,
        rows_written=int(len(muni_segment_df)),
    )
