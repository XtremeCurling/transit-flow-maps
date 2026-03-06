"""GeoJSON export helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import SupportsFloat, cast

import pandas as pd
from shapely import wkb
from shapely.geometry import LineString

from transit_flow_maps.util.config import RuntimeConfig, ensure_output_directories
from transit_flow_maps.util.logging import get_logger


@dataclass(frozen=True)
class JoinCoverageStats:
    """Join coverage stats between segment keys and segment flows."""

    key_count: int
    flow_count: int
    intersection_count: int
    flow_match_rate: float
    key_match_rate: float


@dataclass(frozen=True)
class ExportGeoJSONArtifacts:
    """Output path metadata for export-geojson."""

    output_path: Path
    rows_written: int
    additional_output_paths: tuple[Path, ...] = ()
    join_coverage_path: Path | None = None
    join_coverage_stats: JoinCoverageStats | None = None


@dataclass
class PhysicalAggregateState:
    """Aggregation state for physical segment export."""

    daily_riders: float = 0.0
    agencies: set[str] = field(default_factory=set)
    modes: set[str] = field(default_factory=set)
    routes: set[str] = field(default_factory=set)
    time_bases: set[str] = field(default_factory=set)
    source_breakdown: dict[tuple[str, str], dict[str, object]] = field(default_factory=dict)


@dataclass(frozen=True)
class SegmentKeySummary:
    """Aggregated segment-key metadata used by exports."""

    geom_wkb: bytes
    agencies: list[str]
    modes: list[str]
    routes: list[str]


def _parse_json_list(value: object) -> list[str]:
    if value is None:
        return []
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return []
    try:
        decoded = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(decoded, list):
        return []
    return [str(item) for item in decoded]


def _safe_float(value: object) -> float:
    if value is None:
        return 0.0
    try:
        return float(cast(SupportsFloat | str | bytes | bytearray, value))
    except (TypeError, ValueError):
        return 0.0


def _line_coordinates_from_wkb(geom_wkb: bytes) -> list[list[float]]:
    geom = wkb.loads(geom_wkb)
    if not isinstance(geom, LineString):
        return []
    return [[float(x), float(y)] for x, y in geom.coords]


def _segment_key_summary(segment_keys_path: Path) -> dict[str, SegmentKeySummary]:
    columns = ["segment_id", "geom_wkb", "agency", "mode", "route_id", "shape_id"]
    segment_keys = pd.read_parquet(segment_keys_path, columns=columns)
    if segment_keys.empty:
        return {}

    ordered = segment_keys.sort_values(["segment_id", "agency", "route_id", "shape_id"])
    grouped = (
        ordered.groupby("segment_id", as_index=False)
        .agg(
            geom_wkb=("geom_wkb", "first"),
            agencies=("agency", lambda s: sorted(set(str(v) for v in s))),
            modes=("mode", lambda s: sorted(set(str(v) for v in s))),
            routes=("route_id", lambda s: sorted(set(str(v) for v in s))),
        )
        .sort_values("segment_id")
    )
    return {
        str(row["segment_id"]): SegmentKeySummary(
            geom_wkb=bytes(row["geom_wkb"]),
            agencies=list(row["agencies"]),
            modes=list(row["modes"]),
            routes=list(row["routes"]),
        )
        for _, row in grouped.iterrows()
    }


def _determine_time_basis(values: set[str]) -> str:
    if len(values) == 1:
        return sorted(values)[0]
    if len(values) > 1:
        return "mixed"
    return "weekday_average"


def _write_geojson(features: list[dict[str, object]], output_path: Path) -> None:
    payload = {"type": "FeatureCollection", "features": features}
    output_path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")


def _write_join_coverage(
    *,
    key_segment_ids: set[str],
    flow_segment_ids: set[str],
    output_path: Path,
) -> JoinCoverageStats:
    matched = key_segment_ids & flow_segment_ids
    rows: list[dict[str, str]] = []
    for segment_id in sorted(key_segment_ids | flow_segment_ids):
        if segment_id in matched:
            status = "matched"
        elif segment_id in key_segment_ids:
            status = "key_only"
        else:
            status = "flow_only"
        rows.append({"segment_id": segment_id, "status": status})

    pd.DataFrame(rows, columns=["segment_id", "status"]).to_csv(output_path, index=False)

    key_count = len(key_segment_ids)
    flow_count = len(flow_segment_ids)
    intersection_count = len(matched)
    flow_match_rate = (intersection_count / flow_count) if flow_count > 0 else 1.0
    key_match_rate = (intersection_count / key_count) if key_count > 0 else 1.0
    return JoinCoverageStats(
        key_count=key_count,
        flow_count=flow_count,
        intersection_count=intersection_count,
        flow_match_rate=flow_match_rate,
        key_match_rate=key_match_rate,
    )


def _export_physical(runtime_config: RuntimeConfig) -> ExportGeoJSONArtifacts:
    logger = get_logger(__name__)
    segment_flows_path = runtime_config.paths.interim_dir / "segment_flows.parquet"
    segment_keys_path = runtime_config.paths.interim_dir / "segment_keys.parquet"
    if not segment_flows_path.exists():
        raise FileNotFoundError(f"Missing segment flows parquet: {segment_flows_path}")
    if not segment_keys_path.exists():
        raise FileNotFoundError(f"Missing segment keys parquet: {segment_keys_path}")

    flows = pd.read_parquet(segment_flows_path)
    required = {"segment_id", "daily_riders", "agency", "mode", "routes_json", "time_basis"}
    missing = sorted(required - set(flows.columns))
    if missing:
        raise ValueError(f"segment_flows.parquet missing required columns: {missing}")

    key_summary = _segment_key_summary(segment_keys_path)
    if not key_summary:
        raise RuntimeError("No segment geometry available for physical export")

    key_segment_ids = set(key_summary)
    flow_segment_ids = set(flows["segment_id"].astype(str).tolist())
    join_coverage_path = runtime_config.paths.debug_dir / "join_coverage.csv"
    join_stats = _write_join_coverage(
        key_segment_ids=key_segment_ids,
        flow_segment_ids=flow_segment_ids,
        output_path=join_coverage_path,
    )
    logger.info(
        "Physical join coverage |keys|=%s |flows|=%s |intersection|=%s flow_match_rate=%.4f key_match_rate=%.4f",
        join_stats.key_count,
        join_stats.flow_count,
        join_stats.intersection_count,
        join_stats.flow_match_rate,
        join_stats.key_match_rate,
    )

    states: dict[str, PhysicalAggregateState] = {}
    for _, row in flows.iterrows():
        segment_id = str(row["segment_id"])
        state = states.setdefault(segment_id, PhysicalAggregateState())
        agency = str(row["agency"])
        mode = str(row["mode"])
        routes = _parse_json_list(row["routes_json"])
        riders = _safe_float(row["daily_riders"])
        time_basis = str(row["time_basis"])

        state.daily_riders += riders
        state.agencies.add(agency)
        state.modes.add(mode)
        state.routes.update(routes)
        state.time_bases.add(time_basis)

        breakdown_key = (agency, mode)
        breakdown = state.source_breakdown.setdefault(
            breakdown_key,
            {"daily_riders": 0.0, "routes": set(), "time_basis": set()},
        )
        breakdown["daily_riders"] = _safe_float(breakdown.get("daily_riders")) + riders
        breakdown_routes = breakdown.get("routes")
        if isinstance(breakdown_routes, set):
            breakdown_routes.update(routes)
        breakdown_time_bases = breakdown.get("time_basis")
        if isinstance(breakdown_time_bases, set):
            breakdown_time_bases.add(time_basis)

    physical_all_features: list[dict[str, object]] = []
    for segment_id in sorted(key_summary):
        summary = key_summary[segment_id]
        coordinates = _line_coordinates_from_wkb(summary.geom_wkb)
        if len(coordinates) < 2:
            continue

        physical_all_features.append(
            {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coordinates},
                "properties": {
                    "segment_id": segment_id,
                    "daily_riders": 0,
                    "time_basis": "",
                    "routes": summary.routes,
                    "agencies": summary.agencies,
                    "modes": summary.modes,
                    "has_flow": False,
                    "source_breakdown": [],
                },
            }
        )

    physical_flow_features: list[dict[str, object]] = []
    for segment_id in sorted(states):
        summary = key_summary.get(segment_id)
        if summary is None:
            continue

        coordinates = _line_coordinates_from_wkb(summary.geom_wkb)
        if len(coordinates) < 2:
            continue

        state = states[segment_id]
        breakdown_rows: list[dict[str, object]] = []
        for agency, mode in sorted(state.source_breakdown):
            breakdown = state.source_breakdown[(agency, mode)]
            breakdown_routes = breakdown.get("routes")
            routes_list = sorted(breakdown_routes) if isinstance(breakdown_routes, set) else []
            breakdown_time_basis = breakdown.get("time_basis")
            time_basis_values = (
                breakdown_time_basis if isinstance(breakdown_time_basis, set) else set()
            )
            breakdown_rows.append(
                {
                    "agency": agency,
                    "mode": mode,
                    "daily_riders": round(_safe_float(breakdown.get("daily_riders")), 6),
                    "routes": routes_list,
                    "time_basis": _determine_time_basis(time_basis_values),
                }
            )

        physical_flow_features.append(
            {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coordinates},
                "properties": {
                    "segment_id": segment_id,
                    "daily_riders": round(float(state.daily_riders), 6),
                    "time_basis": _determine_time_basis(state.time_bases),
                    "routes": sorted(state.routes),
                    "agencies": sorted(state.agencies),
                    "modes": sorted(state.modes),
                    "has_flow": True,
                    "source_breakdown": breakdown_rows,
                },
            }
        )

    physical_all_path = runtime_config.paths.web_dir / "physical_all.geojson"
    physical_flows_path = runtime_config.paths.web_dir / "physical_flows.geojson"
    legacy_path = runtime_config.paths.web_dir / "physical.geojson"

    _write_geojson(physical_all_features, physical_all_path)
    _write_geojson(physical_flow_features, physical_flows_path)
    _write_geojson(physical_flow_features, legacy_path)

    logger.info(
        "Exported physical GeoJSON all=%s flows=%s",
        len(physical_all_features),
        len(physical_flow_features),
    )
    return ExportGeoJSONArtifacts(
        output_path=legacy_path,
        rows_written=len(physical_flow_features),
        additional_output_paths=(physical_all_path, physical_flows_path),
        join_coverage_path=join_coverage_path,
        join_coverage_stats=join_stats,
    )


def _export_corridor(runtime_config: RuntimeConfig) -> ExportGeoJSONArtifacts:
    logger = get_logger(__name__)
    corridor_path = runtime_config.paths.interim_dir / "corridor_flows.parquet"
    if not corridor_path.exists():
        raise FileNotFoundError(f"Missing corridor flows parquet: {corridor_path}")

    corridor_df = pd.read_parquet(corridor_path)
    required = {
        "corridor_id",
        "corridor_segment_id",
        "daily_riders",
        "agencies_json",
        "routes_json",
        "geom_wkb",
        "time_basis",
    }
    missing = sorted(required - set(corridor_df.columns))
    if missing:
        raise ValueError(f"corridor_flows.parquet missing required columns: {missing}")

    corridor_df = corridor_df.sort_values(["corridor_id", "corridor_segment_id"])
    corridor_all_features: list[dict[str, object]] = []
    corridor_flow_features: list[dict[str, object]] = []

    for _, row in corridor_df.iterrows():
        coordinates = _line_coordinates_from_wkb(bytes(row["geom_wkb"]))
        if len(coordinates) < 2:
            continue

        agencies = _parse_json_list(row["agencies_json"])
        routes = _parse_json_list(row["routes_json"])
        modes = _parse_json_list(row["modes_json"]) if "modes_json" in corridor_df.columns else []
        source_breakdown: object
        if "source_breakdown_json" in corridor_df.columns:
            try:
                source_breakdown = json.loads(str(row["source_breakdown_json"]))
            except json.JSONDecodeError:
                source_breakdown = []
        else:
            source_breakdown = []

        corridor_segment_id = str(row["corridor_segment_id"])
        daily_riders = round(_safe_float(row["daily_riders"]), 6)
        has_flow = daily_riders > 0.0

        feature = {
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": coordinates},
            "properties": {
                "corridor_id": str(row["corridor_id"]),
                "corridor_segment_id": corridor_segment_id,
                "segment_id": corridor_segment_id,
                "daily_riders": daily_riders,
                "time_basis": str(row["time_basis"]),
                "routes": routes,
                "agencies": agencies,
                "modes": modes,
                "has_flow": has_flow,
                "source_breakdown": source_breakdown,
            },
        }
        corridor_all_features.append(feature)
        if has_flow:
            corridor_flow_features.append(feature)

    corridor_all_path = runtime_config.paths.web_dir / "corridor_all.geojson"
    corridor_flows_path = runtime_config.paths.web_dir / "corridor_flows.geojson"
    legacy_path = runtime_config.paths.web_dir / "corridor.geojson"

    _write_geojson(corridor_all_features, corridor_all_path)
    _write_geojson(corridor_flow_features, corridor_flows_path)
    _write_geojson(corridor_all_features, legacy_path)

    logger.info(
        "Exported corridor GeoJSON all=%s flows=%s",
        len(corridor_all_features),
        len(corridor_flow_features),
    )
    return ExportGeoJSONArtifacts(
        output_path=legacy_path,
        rows_written=len(corridor_all_features),
        additional_output_paths=(corridor_all_path, corridor_flows_path),
    )


def export_geojson(runtime_config: RuntimeConfig, *, view: str) -> ExportGeoJSONArtifacts:
    """Export web GeoJSON for the requested view."""
    ensure_output_directories(runtime_config.paths)
    normalized = view.lower()
    if normalized == "physical":
        return _export_physical(runtime_config)
    if normalized == "corridor":
        return _export_corridor(runtime_config)
    raise ValueError(f"Unsupported view: {view}")
