"""Physical-to-corridor segment assignment and aggregation."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import SupportsFloat, cast

import pandas as pd
from shapely import wkb
from shapely.geometry import LineString

from transit_flow_maps.conflation.densify import bearing_degrees_xy, fold_undirected_bearing
from transit_flow_maps.corridors.corridors import (
    CorridorPlan,
    CorridorSegment,
    build_corridor_plans,
)
from transit_flow_maps.util.config import RuntimeConfig, ensure_output_directories
from transit_flow_maps.util.logging import get_logger


@dataclass(frozen=True)
class CorridorArtifacts:
    """Output paths produced by build-corridors."""

    corridor_flows_path: Path
    assignment_debug_path: Path
    rows_written: int


@dataclass
class CorridorAggregateState:
    """Mutable aggregation state for one corridor segment."""

    daily_riders: float = 0.0
    agencies: set[str] = field(default_factory=set)
    routes: set[str] = field(default_factory=set)
    modes: set[str] = field(default_factory=set)
    time_bases: set[str] = field(default_factory=set)
    source_breakdown: dict[tuple[str, str], dict[str, object]] = field(default_factory=dict)


@dataclass(frozen=True)
class SegmentAssignment:
    """Best corridor assignment for one physical segment id."""

    corridor_id: str
    corridor_segment_id: str
    corridor_index: int
    distance_m: float
    bearing_delta_deg: float


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
        parsed = float(cast(SupportsFloat | str | bytes | bytearray, value))
    except (TypeError, ValueError):
        return 0.0
    return parsed


def _bearing_for_metric_line(line: LineString) -> float:
    coords = list(line.coords)
    if len(coords) < 2:
        return 0.0
    start = (float(coords[0][0]), float(coords[0][1]))
    end = (float(coords[-1][0]), float(coords[-1][1]))
    bearing = bearing_degrees_xy(start, end)
    if bearing is None:
        return 0.0
    return float(fold_undirected_bearing(bearing))


def _bearing_delta_deg(a: float, b: float) -> float:
    delta = abs(a - b) % 180.0
    return min(delta, 180.0 - delta)


def _metric_line_for_plan(geom_wkb: bytes, plan: CorridorPlan) -> LineString | None:
    geom = wkb.loads(geom_wkb)
    if not isinstance(geom, LineString):
        return None
    metric_coords = [
        tuple(float(v) for v in plan.crs_context.to_metric.transform(float(x), float(y)))
        for x, y in geom.coords
    ]
    if len(metric_coords) < 2:
        return None
    return LineString(metric_coords)


def _load_segment_geom_lookup(segment_keys_path: Path) -> dict[str, bytes]:
    columns = ["segment_id", "geom_wkb", "agency", "route_id", "shape_id"]
    segment_keys = pd.read_parquet(segment_keys_path, columns=columns)
    if segment_keys.empty:
        return {}

    ordered = segment_keys.sort_values(["segment_id", "agency", "route_id", "shape_id"])
    grouped = ordered.groupby("segment_id", as_index=False).agg(geom_wkb=("geom_wkb", "first"))
    return {
        str(row["segment_id"]): bytes(row["geom_wkb"])
        for _, row in grouped.iterrows()
    }


def _select_best_segment_for_plan(
    line_metric: LineString,
    bearing_undirected: float,
    plan: CorridorPlan,
    *,
    max_distance_m: float,
) -> SegmentAssignment | None:
    best_key: tuple[float, float, int] | None = None
    best_segment: CorridorSegment | None = None
    for corridor_segment in plan.segments:
        distance = float(line_metric.distance(corridor_segment.metric_line))
        if distance > max_distance_m:
            continue

        bearing_delta = _bearing_delta_deg(
            bearing_undirected,
            corridor_segment.bearing_undirected_deg,
        )
        candidate_key = (distance, bearing_delta, corridor_segment.corridor_index)
        if best_key is None or candidate_key < best_key:
            best_key = candidate_key
            best_segment = corridor_segment

    if best_key is None or best_segment is None:
        return None

    return SegmentAssignment(
        corridor_id=plan.corridor_id,
        corridor_segment_id=best_segment.corridor_segment_id,
        corridor_index=best_segment.corridor_index,
        distance_m=best_key[0],
        bearing_delta_deg=best_key[1],
    )


def build_corridors(runtime_config: RuntimeConfig, *, include_bart: bool) -> CorridorArtifacts:
    """Build corridor-level flows from physical segment flows."""
    logger = get_logger(__name__)
    ensure_output_directories(runtime_config.paths)

    segment_flows_path = runtime_config.paths.interim_dir / "segment_flows.parquet"
    if not segment_flows_path.exists():
        raise FileNotFoundError(f"Missing segment flows parquet: {segment_flows_path}")

    segment_keys_path = runtime_config.paths.interim_dir / "segment_keys.parquet"
    if not segment_keys_path.exists():
        raise FileNotFoundError(f"Missing segment keys parquet: {segment_keys_path}")

    plans = build_corridor_plans(runtime_config)
    if not plans:
        raise RuntimeError("No corridor plans available")

    flows = pd.read_parquet(segment_flows_path)
    required = {"segment_id", "daily_riders", "agency", "mode", "routes_json", "time_basis"}
    missing = sorted(required - set(flows.columns))
    if missing:
        raise ValueError(f"segment_flows.parquet missing required columns: {missing}")

    if not include_bart:
        flows = flows[flows["agency"].astype(str) == "SFMTA"].copy()
    if flows.empty:
        raise RuntimeError("No segment flow rows after agency filtering")

    geom_lookup = _load_segment_geom_lookup(segment_keys_path)
    if not geom_lookup:
        raise RuntimeError("No segment geometry could be loaded from segment_keys.parquet")

    unique_segment_ids = sorted(set(flows["segment_id"].astype(str)))
    assignments: dict[str, SegmentAssignment] = {}
    assignment_rows: list[dict[str, object]] = []
    corridor_buffer_m = float(runtime_config.settings.corridor_buffer_m)
    assignment_max_m = float(runtime_config.settings.corridor_assignment_max_distance_m)

    for segment_id in unique_segment_ids:
        geom_wkb = geom_lookup.get(segment_id)
        if geom_wkb is None:
            continue

        best_overall_key: tuple[float, float, str, int] | None = None
        best_assignment: SegmentAssignment | None = None

        for corridor_id in sorted(plans):
            plan = plans[corridor_id]
            metric_line = _metric_line_for_plan(geom_wkb, plan)
            if metric_line is None:
                continue

            centerline_distance = float(metric_line.distance(plan.centerline_metric))
            if centerline_distance > corridor_buffer_m:
                continue

            bearing_undirected = _bearing_for_metric_line(metric_line)
            plan_assignment = _select_best_segment_for_plan(
                metric_line,
                bearing_undirected,
                plan,
                max_distance_m=assignment_max_m,
            )
            if plan_assignment is None:
                continue

            candidate_key = (
                plan_assignment.distance_m,
                plan_assignment.bearing_delta_deg,
                plan_assignment.corridor_id,
                plan_assignment.corridor_index,
            )
            if best_overall_key is None or candidate_key < best_overall_key:
                best_overall_key = candidate_key
                best_assignment = plan_assignment

        if best_assignment is None:
            continue

        assignments[segment_id] = best_assignment
        assignment_rows.append(
            {
                "segment_id": segment_id,
                "corridor_id": best_assignment.corridor_id,
                "corridor_segment_id": best_assignment.corridor_segment_id,
                "distance_m": round(best_assignment.distance_m, 3),
                "bearing_delta_deg": round(best_assignment.bearing_delta_deg, 3),
            }
        )

    if not assignments:
        raise RuntimeError("No physical segments were assigned to any corridor segment")

    segment_index: dict[str, CorridorSegment] = {}
    segment_states: dict[str, CorridorAggregateState] = {}
    for plan in plans.values():
        for corridor_segment in plan.segments:
            segment_index[corridor_segment.corridor_segment_id] = corridor_segment
            segment_states[corridor_segment.corridor_segment_id] = CorridorAggregateState()

    for _, row in flows.iterrows():
        segment_id = str(row["segment_id"])
        assignment = assignments.get(segment_id)
        if assignment is None:
            continue

        state = segment_states[assignment.corridor_segment_id]
        agency = str(row["agency"])
        mode = str(row["mode"])
        time_basis = str(row["time_basis"])
        routes = _parse_json_list(row["routes_json"])
        riders = _safe_float(row["daily_riders"])

        state.daily_riders += riders
        state.agencies.add(agency)
        state.modes.add(mode)
        state.time_bases.add(time_basis)
        state.routes.update(routes)

        breakdown_key = (agency, mode)
        breakdown = state.source_breakdown.setdefault(
            breakdown_key,
            {"daily_riders": 0.0, "routes": set()},
        )
        breakdown["daily_riders"] = _safe_float(breakdown.get("daily_riders")) + riders
        cast_routes = breakdown.get("routes")
        if isinstance(cast_routes, set):
            cast_routes.update(routes)

    corridor_rows: list[dict[str, object]] = []
    ordered_segments = sorted(
        segment_index.values(),
        key=lambda segment: (segment.corridor_id, segment.corridor_index),
    )
    for corridor_segment in ordered_segments:
        state = segment_states[corridor_segment.corridor_segment_id]

        if len(state.time_bases) == 1:
            time_basis = sorted(state.time_bases)[0]
        elif len(state.time_bases) > 1:
            time_basis = "mixed"
        else:
            time_basis = "weekday_average"

        breakdown_rows: list[dict[str, object]] = []
        for agency, mode in sorted(state.source_breakdown):
            breakdown = state.source_breakdown[(agency, mode)]
            breakdown_routes = breakdown.get("routes")
            routes_list = sorted(breakdown_routes) if isinstance(breakdown_routes, set) else []
            breakdown_rows.append(
                {
                    "agency": agency,
                    "mode": mode,
                    "daily_riders": round(_safe_float(breakdown.get("daily_riders")), 6),
                    "routes": routes_list,
                }
            )

        corridor_rows.append(
            {
                "corridor_id": corridor_segment.corridor_id,
                "corridor_segment_id": corridor_segment.corridor_segment_id,
                "daily_riders": float(state.daily_riders),
                "agencies_json": json.dumps(sorted(state.agencies)),
                "routes_json": json.dumps(sorted(state.routes)),
                "modes_json": json.dumps(sorted(state.modes)),
                "source_breakdown_json": json.dumps(
                    breakdown_rows,
                    separators=(",", ":"),
                ),
                "geom_wkb": corridor_segment.geom_wkb,
                "time_basis": time_basis,
            }
        )

    corridor_df = pd.DataFrame(corridor_rows).sort_values(
        ["corridor_id", "corridor_segment_id"]
    )
    corridor_df = corridor_df[
        [
            "corridor_id",
            "corridor_segment_id",
            "daily_riders",
            "agencies_json",
            "routes_json",
            "geom_wkb",
            "time_basis",
            "modes_json",
            "source_breakdown_json",
        ]
    ]

    corridor_flows_path = runtime_config.paths.interim_dir / "corridor_flows.parquet"
    corridor_df.to_parquet(corridor_flows_path, index=False)

    assignment_debug_path = runtime_config.paths.debug_dir / "corridor_assignments.csv"
    assignment_df = pd.DataFrame(assignment_rows)
    if assignment_df.empty:
        assignment_df = pd.DataFrame(
            columns=[
                "segment_id",
                "corridor_id",
                "corridor_segment_id",
                "distance_m",
                "bearing_delta_deg",
            ]
        )
    assignment_df = assignment_df.sort_values(["corridor_id", "corridor_segment_id", "segment_id"])
    assignment_df.to_csv(assignment_debug_path, index=False)

    logger.info("Corridor flow rows written: %s", len(corridor_df))

    return CorridorArtifacts(
        corridor_flows_path=corridor_flows_path,
        assignment_debug_path=assignment_debug_path,
        rows_written=int(len(corridor_df)),
    )
