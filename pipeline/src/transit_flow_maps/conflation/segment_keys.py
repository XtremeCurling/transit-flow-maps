"""Deterministic segment key generation."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal

from shapely.geometry import LineString, Point

from transit_flow_maps.conflation.densify import (
    bearing_degrees_xy,
    densify_linestring_lonlat,
    fold_undirected_bearing,
)
from transit_flow_maps.conflation.h3_index import (
    are_neighbor_cells,
    cell_to_latlng,
    directed_edge_boundary,
    grid_path_cells,
    latlng_to_cell,
)
from transit_flow_maps.conflation.simplify import cell_transitions, clean_cell_sequence
from transit_flow_maps.util.crs import CRSContext, build_crs_context, choose_metric_crs


@dataclass(frozen=True)
class SegmentKeyConfig:
    """Config values required to build deterministic segment keys."""

    h3_resolution: int
    edge_pos_bins: int
    bearing_bucket_count: int
    densify_spacing_m: float
    non_neighbor_max_path_cells: int
    non_neighbor_max_recursion_depth: int
    crs_metric_default: str


@dataclass(frozen=True)
class RawObservation:
    """Geometry context tied to a directional cell transition."""

    from_cell: str
    to_cell: str
    start_lonlat: tuple[float, float]
    end_lonlat: tuple[float, float]
    start_xy: tuple[float, float]
    end_xy: tuple[float, float]
    midpoint_lonlat: tuple[float, float]
    bearing_deg: float | None
    length_m: float
    start_measure_m: float
    end_measure_m: float
    midpoint_measure_m: float

    def reversed(self) -> RawObservation:
        flipped_bearing = None
        if self.bearing_deg is not None:
            flipped_bearing = (self.bearing_deg + 180.0) % 360.0

        return RawObservation(
            from_cell=self.to_cell,
            to_cell=self.from_cell,
            start_lonlat=self.end_lonlat,
            end_lonlat=self.start_lonlat,
            start_xy=self.end_xy,
            end_xy=self.start_xy,
            midpoint_lonlat=self.midpoint_lonlat,
            bearing_deg=flipped_bearing,
            length_m=self.length_m,
            start_measure_m=self.end_measure_m,
            end_measure_m=self.start_measure_m,
            midpoint_measure_m=self.midpoint_measure_m,
        )


@dataclass(frozen=True)
class Transition:
    """Resolved directional transition used for segment-key fields."""

    from_cell: str
    to_cell: str
    midpoint_lonlat: tuple[float, float]
    bearing_deg: float | None
    length_m: float
    is_repaired: bool
    midpoint_measure_m: float


@dataclass(frozen=True)
class SegmentKeyBuildResult:
    """Segment-key rows and diagnostics for one shape polyline."""

    records: list[dict[str, object]]
    repair_logs: list[dict[str, object]]
    transition_records: list[dict[str, object]]


def _distance_xy(a: tuple[float, float], b: tuple[float, float]) -> float:
    return float(math.hypot(b[0] - a[0], b[1] - a[1]))


def _quantize_bearing_bucket(undirected_bearing: float, bucket_count: int) -> int:
    if bucket_count <= 0:
        raise ValueError("bucket_count must be > 0")

    width = 180.0 / float(bucket_count)
    bucket = int(math.floor((undirected_bearing / width) + 0.5)) % bucket_count
    return bucket


def _interpolate_midpoint_lonlat(
    start_xy: tuple[float, float],
    end_xy: tuple[float, float],
    frac: float,
    crs_context: CRSContext,
) -> tuple[float, float]:
    x = start_xy[0] + (end_xy[0] - start_xy[0]) * frac
    y = start_xy[1] + (end_xy[1] - start_xy[1]) * frac
    lon, lat = crs_context.to_wgs84.transform(x, y)
    return float(lon), float(lat)


def _to_metric_xy(lonlat: tuple[float, float], crs_context: CRSContext) -> tuple[float, float]:
    x, y = crs_context.to_metric.transform(lonlat[0], lonlat[1])
    return float(x), float(y)


def _build_raw_observations(
    densified_lonlat: list[tuple[float, float]],
    config: SegmentKeyConfig,
    crs_context: CRSContext,
) -> tuple[list[str], dict[tuple[str, str], list[RawObservation]]]:
    cells = [latlng_to_cell(lat, lon, config.h3_resolution) for lon, lat in densified_lonlat]
    metric_points = [_to_metric_xy(point, crs_context) for point in densified_lonlat]
    measures_m = [0.0]
    for idx in range(1, len(metric_points)):
        measures_m.append(measures_m[-1] + _distance_xy(metric_points[idx - 1], metric_points[idx]))

    observations: dict[tuple[str, str], list[RawObservation]] = {}

    for i in range(len(cells) - 1):
        from_cell = cells[i]
        to_cell = cells[i + 1]
        if from_cell == to_cell:
            continue

        start_lonlat = densified_lonlat[i]
        end_lonlat = densified_lonlat[i + 1]
        start_xy = metric_points[i]
        end_xy = metric_points[i + 1]
        midpoint_lonlat = _interpolate_midpoint_lonlat(start_xy, end_xy, 0.5, crs_context)
        bearing_deg = bearing_degrees_xy(start_xy, end_xy)
        length_m = _distance_xy(start_xy, end_xy)
        start_measure_m = measures_m[i]
        end_measure_m = measures_m[i + 1]
        midpoint_measure_m = (start_measure_m + end_measure_m) / 2.0

        obs = RawObservation(
            from_cell=from_cell,
            to_cell=to_cell,
            start_lonlat=start_lonlat,
            end_lonlat=end_lonlat,
            start_xy=start_xy,
            end_xy=end_xy,
            midpoint_lonlat=midpoint_lonlat,
            bearing_deg=bearing_deg,
            length_m=length_m,
            start_measure_m=start_measure_m,
            end_measure_m=end_measure_m,
            midpoint_measure_m=midpoint_measure_m,
        )
        observations.setdefault((from_cell, to_cell), []).append(obs)

    return cells, observations


def _consume_observation(
    from_cell: str,
    to_cell: str,
    pools: dict[tuple[str, str], list[RawObservation]],
    counters: dict[tuple[str, str], int],
) -> RawObservation | None:
    direct_key = (from_cell, to_cell)
    direct_index = counters.get(direct_key, 0)
    direct_pool = pools.get(direct_key, [])
    if direct_index < len(direct_pool):
        counters[direct_key] = direct_index + 1
        return direct_pool[direct_index]

    reverse_key = (to_cell, from_cell)
    reverse_index = counters.get(reverse_key, 0)
    reverse_pool = pools.get(reverse_key, [])
    if reverse_index < len(reverse_pool):
        counters[reverse_key] = reverse_index + 1
        return reverse_pool[reverse_index].reversed()

    return None


def _fallback_observation(from_cell: str, to_cell: str, crs_context: CRSContext) -> RawObservation:
    from_lat, from_lon = cell_to_latlng(from_cell)
    to_lat, to_lon = cell_to_latlng(to_cell)

    start_lonlat = (from_lon, from_lat)
    end_lonlat = (to_lon, to_lat)
    start_xy = _to_metric_xy(start_lonlat, crs_context)
    end_xy = _to_metric_xy(end_lonlat, crs_context)
    midpoint_lonlat = _interpolate_midpoint_lonlat(start_xy, end_xy, 0.5, crs_context)
    bearing_deg = bearing_degrees_xy(start_xy, end_xy)
    length_m = _distance_xy(start_xy, end_xy)

    return RawObservation(
        from_cell=from_cell,
        to_cell=to_cell,
        start_lonlat=start_lonlat,
        end_lonlat=end_lonlat,
        start_xy=start_xy,
        end_xy=end_xy,
        midpoint_lonlat=midpoint_lonlat,
        bearing_deg=bearing_deg,
        length_m=length_m,
        start_measure_m=0.0,
        end_measure_m=length_m,
        midpoint_measure_m=length_m / 2.0,
    )


def _local_redensify_path(
    from_cell: str,
    to_cell: str,
    observation: RawObservation,
    config: SegmentKeyConfig,
    crs_context: CRSContext,
    depth: int,
) -> list[str] | None:
    spacing = max(1.0, config.densify_spacing_m / float(2 ** (depth + 1)))

    span_points = densify_linestring_lonlat(
        [observation.start_lonlat, observation.end_lonlat],
        spacing_m=spacing,
        to_metric=crs_context.to_metric,
        to_wgs84=crs_context.to_wgs84,
    )
    cells = [latlng_to_cell(lat, lon, config.h3_resolution) for lon, lat in span_points]
    cells = clean_cell_sequence(cells)

    if len(cells) < 2:
        return None

    # Enforce intended endpoints for deterministic repair semantics.
    cells[0] = from_cell
    cells[-1] = to_cell
    cells = clean_cell_sequence(cells)

    if len(cells) < 2:
        return None

    if (len(cells) - 1) > config.non_neighbor_max_path_cells:
        return None

    if all(are_neighbor_cells(a, b) for a, b in cell_transitions(cells)):
        return cells

    if depth + 1 >= config.non_neighbor_max_recursion_depth:
        return None

    return _local_redensify_path(
        from_cell=from_cell,
        to_cell=to_cell,
        observation=observation,
        config=config,
        crs_context=crs_context,
        depth=depth + 1,
    )


def _repair_transition(
    transition: tuple[str, str],
    observation: RawObservation,
    config: SegmentKeyConfig,
    crs_context: CRSContext,
    shape_id: str,
) -> tuple[list[Transition], dict[str, object]]:
    from_cell, to_cell = transition

    path_cells: list[str] | None = None
    method: Literal["grid_path", "local_redensify", "dropped"] = "dropped"

    try:
        path_candidate = grid_path_cells(from_cell, to_cell)
        if (
            len(path_candidate) >= 2
            and (len(path_candidate) - 1) <= config.non_neighbor_max_path_cells
        ):
            path_cells = path_candidate
            method = "grid_path"
    except Exception:
        path_cells = None

    if path_cells is None:
        repaired_path = _local_redensify_path(
            from_cell=from_cell,
            to_cell=to_cell,
            observation=observation,
            config=config,
            crs_context=crs_context,
            depth=0,
        )
        if repaired_path is not None:
            path_cells = repaired_path
            method = "local_redensify"

    if path_cells is None:
        repair_log = {
            "shape_id": shape_id,
            "from_cell": from_cell,
            "to_cell": to_cell,
            "status": "dropped",
            "method": method,
            "path_len": 0,
            "path": "",
        }
        return [], repair_log

    edge_count = len(path_cells) - 1
    repaired: list[Transition] = []

    for idx in range(edge_count):
        frac = (idx + 0.5) / float(edge_count)
        midpoint_lonlat = _interpolate_midpoint_lonlat(
            observation.start_xy,
            observation.end_xy,
            frac,
            crs_context,
        )
        repaired.append(
            Transition(
                from_cell=path_cells[idx],
                to_cell=path_cells[idx + 1],
                midpoint_lonlat=midpoint_lonlat,
                bearing_deg=observation.bearing_deg,
                length_m=observation.length_m / float(edge_count),
                is_repaired=True,
                midpoint_measure_m=(
                    observation.start_measure_m
                    + (observation.end_measure_m - observation.start_measure_m) * frac
                ),
            )
        )

    repair_log = {
        "shape_id": shape_id,
        "from_cell": from_cell,
        "to_cell": to_cell,
        "status": "repaired",
        "method": method,
        "path_len": edge_count,
        "path": ">".join(path_cells),
    }
    return repaired, repair_log


def _rotate(seq: list[tuple[float, float]], index: int) -> list[tuple[float, float]]:
    return seq[index:] + seq[:index]


def _canonicalize_boundary(points_lonlat: list[tuple[float, float]]) -> list[tuple[float, float]]:
    if not points_lonlat:
        return []

    cleaned = [points_lonlat[0]]
    for point in points_lonlat[1:]:
        if point != cleaned[-1]:
            cleaned.append(point)

    if len(cleaned) <= 2:
        forward = cleaned
        backward = list(reversed(cleaned))
        return forward if tuple(forward) <= tuple(backward) else backward

    min_vertex = min(cleaned)
    forward_candidates = [
        _rotate(cleaned, idx)
        for idx, point in enumerate(cleaned)
        if point == min_vertex
    ]

    reversed_cleaned = list(reversed(cleaned))
    reverse_candidates = [
        _rotate(reversed_cleaned, idx)
        for idx, point in enumerate(reversed_cleaned)
        if point == min_vertex
    ]

    candidates = forward_candidates + reverse_candidates
    candidates.sort(key=lambda item: tuple(item))
    return candidates[0]


def _edge_pos_bin(
    cell_lo: str,
    cell_hi: str,
    midpoint_lonlat: tuple[float, float],
    edge_pos_bins: int,
    crs_context: CRSContext,
) -> int:
    try:
        raw_boundary_latlng = directed_edge_boundary(cell_lo, cell_hi)
        boundary_lonlat = [(lng, lat) for lat, lng in raw_boundary_latlng]
    except Exception:
        lo_lat, lo_lon = cell_to_latlng(cell_lo)
        hi_lat, hi_lon = cell_to_latlng(cell_hi)
        boundary_lonlat = [(lo_lon, lo_lat), (hi_lon, hi_lat)]
    canonical_boundary = _canonicalize_boundary(boundary_lonlat)

    if len(canonical_boundary) < 2:
        return 0

    boundary_xy = [_to_metric_xy(point, crs_context) for point in canonical_boundary]
    boundary_line = LineString(boundary_xy)

    if boundary_line.length == 0:
        return 0

    midpoint_xy = _to_metric_xy(midpoint_lonlat, crs_context)
    t = float(boundary_line.project(Point(midpoint_xy), normalized=True))
    t = max(0.0, min(1.0 - 1e-9, t))
    return int(math.floor(t * edge_pos_bins))


def _bearing_bucket(
    transition: Transition,
    cell_lo: str,
    cell_hi: str,
    bucket_count: int,
    crs_context: CRSContext,
) -> int:
    bearing = transition.bearing_deg

    if bearing is None:
        from_lat, from_lon = cell_to_latlng(cell_lo)
        to_lat, to_lon = cell_to_latlng(cell_hi)
        start_xy = _to_metric_xy((from_lon, from_lat), crs_context)
        end_xy = _to_metric_xy((to_lon, to_lat), crs_context)
        bearing = bearing_degrees_xy(start_xy, end_xy)

    if bearing is None:
        if transition.length_m < 5.0 and not transition.is_repaired:
            return -1
        # Deterministic fallback if geometry remains degenerate.
        return 0

    undirected = fold_undirected_bearing(bearing)
    return _quantize_bearing_bucket(undirected, bucket_count)


def _segment_id(
    h3_resolution: int,
    cell_lo: str,
    cell_hi: str,
    edge_pos_bin: int,
    bearing_bucket: int,
) -> str:
    return f"r{h3_resolution}:{cell_lo}:{cell_hi}:e{edge_pos_bin}:b{bearing_bucket}"


def _representative_geometry_cache_key(cell_lo: str, cell_hi: str) -> tuple[str, str]:
    return cell_lo, cell_hi


def _representative_geom_wkb(
    cell_lo: str,
    cell_hi: str,
    cache: dict[tuple[str, str], bytes],
) -> bytes:
    key = _representative_geometry_cache_key(cell_lo, cell_hi)
    if key in cache:
        return cache[key]

    lo_lat, lo_lon = cell_to_latlng(cell_lo)
    hi_lat, hi_lon = cell_to_latlng(cell_hi)
    line = LineString([(lo_lon, lo_lat), (hi_lon, hi_lat)])
    cache[key] = bytes(line.wkb)
    return cache[key]


def build_segment_keys_for_shape(
    shape_id: str,
    shape_points_lonlat: list[tuple[float, float]],
    config: SegmentKeyConfig,
    *,
    pre_densified: bool = False,
) -> SegmentKeyBuildResult:
    """Build deterministic segment-key rows and repair diagnostics for one shape."""
    if len(shape_points_lonlat) < 2:
        return SegmentKeyBuildResult(records=[], repair_logs=[], transition_records=[])

    metric_crs = choose_metric_crs(shape_points_lonlat, config.crs_metric_default)
    crs_context = build_crs_context(metric_crs)

    if pre_densified:
        densified = shape_points_lonlat
    else:
        densified = densify_linestring_lonlat(
            shape_points_lonlat,
            spacing_m=config.densify_spacing_m,
            to_metric=crs_context.to_metric,
            to_wgs84=crs_context.to_wgs84,
        )
    raw_cells, observation_pools = _build_raw_observations(densified, config, crs_context)
    cleaned_cells = clean_cell_sequence(raw_cells)
    transitions = cell_transitions(cleaned_cells)

    consumed_counters: dict[tuple[str, str], int] = {}
    resolved_transitions: list[Transition] = []
    repair_logs: list[dict[str, object]] = []

    for from_cell, to_cell in transitions:
        observation = _consume_observation(from_cell, to_cell, observation_pools, consumed_counters)
        if observation is None:
            observation = _fallback_observation(from_cell, to_cell, crs_context)

        if are_neighbor_cells(from_cell, to_cell):
            resolved_transitions.append(
                Transition(
                    from_cell=from_cell,
                    to_cell=to_cell,
                    midpoint_lonlat=observation.midpoint_lonlat,
                    bearing_deg=observation.bearing_deg,
                    length_m=observation.length_m,
                    is_repaired=False,
                    midpoint_measure_m=observation.midpoint_measure_m,
                )
            )
            continue

        repaired, repair_log = _repair_transition(
            transition=(from_cell, to_cell),
            observation=observation,
            config=config,
            crs_context=crs_context,
            shape_id=shape_id,
        )
        repair_logs.append(repair_log)
        resolved_transitions.extend(repaired)

    representative_cache: dict[tuple[str, str], bytes] = {}
    records: list[dict[str, object]] = []
    transition_records: list[dict[str, object]] = []

    for transition in resolved_transitions:
        cell_lo, cell_hi = sorted([transition.from_cell, transition.to_cell])
        edge_pos_bin = _edge_pos_bin(
            cell_lo=cell_lo,
            cell_hi=cell_hi,
            midpoint_lonlat=transition.midpoint_lonlat,
            edge_pos_bins=config.edge_pos_bins,
            crs_context=crs_context,
        )
        bearing_bucket = _bearing_bucket(
            transition=transition,
            cell_lo=cell_lo,
            cell_hi=cell_hi,
            bucket_count=config.bearing_bucket_count,
            crs_context=crs_context,
        )
        segment_id = _segment_id(
            h3_resolution=config.h3_resolution,
            cell_lo=cell_lo,
            cell_hi=cell_hi,
            edge_pos_bin=edge_pos_bin,
            bearing_bucket=bearing_bucket,
        )

        records.append(
            {
                "segment_id": segment_id,
                "cell_lo": cell_lo,
                "cell_hi": cell_hi,
                "edge_pos_bin": edge_pos_bin,
                "bearing_bucket": bearing_bucket,
                "geom_wkb": _representative_geom_wkb(cell_lo, cell_hi, representative_cache),
                "is_repaired": bool(transition.is_repaired),
            }
        )
        transition_records.append(
            {
                "segment_id": segment_id,
                "midpoint_measure_m": transition.midpoint_measure_m,
                "is_repaired": bool(transition.is_repaired),
            }
        )

    return SegmentKeyBuildResult(
        records=records,
        repair_logs=repair_logs,
        transition_records=transition_records,
    )
