"""Physical-to-corridor segment assignment and aggregation."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import SupportsFloat, cast

import pandas as pd

from transit_flow_maps.corridors.corridors import CorridorPlan, CorridorSegment, build_corridor_plans
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
    """Resolved corridor assignment for one physical segment id."""

    corridor_id: str
    corridor_segment_id: str
    corridor_index: int


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


def _undirected_cell_pair(cell_a: str, cell_b: str) -> tuple[str, str]:
    if cell_a <= cell_b:
        return cell_a, cell_b
    return cell_b, cell_a


def _load_segment_pair_lookup(segment_keys_path: Path) -> dict[str, tuple[str, str]]:
    columns = ["segment_id", "cell_lo", "cell_hi", "agency", "route_id", "shape_id"]
    segment_keys = pd.read_parquet(segment_keys_path, columns=columns)
    if segment_keys.empty:
        return {}

    ordered = segment_keys.sort_values(
        ["segment_id", "cell_lo", "cell_hi", "agency", "route_id", "shape_id"]
    )
    grouped = (
        ordered.groupby("segment_id", as_index=False)
        .agg(cell_lo=("cell_lo", "first"), cell_hi=("cell_hi", "first"))
        .sort_values("segment_id")
    )
    return {
        str(row["segment_id"]): _undirected_cell_pair(str(row["cell_lo"]), str(row["cell_hi"]))
        for _, row in grouped.iterrows()
    }


def _build_corridor_pair_index(
    plans: dict[str, CorridorPlan],
) -> dict[tuple[str, str], list[CorridorSegment]]:
    pair_index: dict[tuple[str, str], list[CorridorSegment]] = {}
    for corridor_id in sorted(plans):
        plan = plans[corridor_id]
        for corridor_segment in plan.segments:
            pair = _undirected_cell_pair(corridor_segment.cell_lo, corridor_segment.cell_hi)
            pair_index.setdefault(pair, []).append(corridor_segment)

    for pair in pair_index:
        pair_index[pair] = sorted(
            pair_index[pair],
            key=lambda segment: (segment.corridor_id, segment.corridor_index),
        )
    return pair_index


def _assign_segment_ids_by_membership(
    segment_ids: list[str],
    segment_pair_lookup: dict[str, tuple[str, str]],
    corridor_pair_index: dict[tuple[str, str], list[CorridorSegment]],
) -> tuple[dict[str, SegmentAssignment], list[dict[str, object]]]:
    assignments: dict[str, SegmentAssignment] = {}
    assignment_rows: list[dict[str, object]] = []

    for segment_id in segment_ids:
        pair = segment_pair_lookup.get(segment_id)
        if pair is None:
            assignment_rows.append(
                {
                    "segment_id": segment_id,
                    "cell_lo": "",
                    "cell_hi": "",
                    "corridor_id": "",
                    "corridor_segment_id": "",
                    "reason": "missing_segment_pair",
                    "candidate_count": 0,
                }
            )
            continue

        candidates = corridor_pair_index.get(pair, [])
        if not candidates:
            assignment_rows.append(
                {
                    "segment_id": segment_id,
                    "cell_lo": pair[0],
                    "cell_hi": pair[1],
                    "corridor_id": "",
                    "corridor_segment_id": "",
                    "reason": "unmatched_pair",
                    "candidate_count": 0,
                }
            )
            continue

        best = candidates[0]
        assignments[segment_id] = SegmentAssignment(
            corridor_id=best.corridor_id,
            corridor_segment_id=best.corridor_segment_id,
            corridor_index=best.corridor_index,
        )
        assignment_rows.append(
            {
                "segment_id": segment_id,
                "cell_lo": pair[0],
                "cell_hi": pair[1],
                "corridor_id": best.corridor_id,
                "corridor_segment_id": best.corridor_segment_id,
                "reason": "exact_pair",
                "candidate_count": len(candidates),
            }
        )

    return assignments, assignment_rows


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

    segment_pair_lookup = _load_segment_pair_lookup(segment_keys_path)
    if not segment_pair_lookup:
        raise RuntimeError("No segment cell-pair lookup could be loaded from segment_keys.parquet")

    corridor_pair_index = _build_corridor_pair_index(plans)
    if not corridor_pair_index:
        raise RuntimeError("No corridor cell-pair index available")

    unique_segment_ids = sorted(set(flows["segment_id"].astype(str)))
    assignments, assignment_rows = _assign_segment_ids_by_membership(
        unique_segment_ids,
        segment_pair_lookup,
        corridor_pair_index,
    )

    if not assignments:
        raise RuntimeError("No physical segments were assigned to any corridor segment")

    exact_count = sum(1 for row in assignment_rows if row["reason"] == "exact_pair")
    unmatched_count = len(assignment_rows) - exact_count
    logger.info(
        "Corridor pair assignment exact=%s unmatched=%s total=%s",
        exact_count,
        unmatched_count,
        len(assignment_rows),
    )

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
                "cell_lo",
                "cell_hi",
                "corridor_id",
                "corridor_segment_id",
                "reason",
                "candidate_count",
            ]
        )
    assignment_df = assignment_df.sort_values(
        ["reason", "corridor_id", "corridor_segment_id", "segment_id"]
    )
    assignment_df.to_csv(assignment_debug_path, index=False)

    logger.info("Corridor flow rows written: %s", len(corridor_df))

    return CorridorArtifacts(
        corridor_flows_path=corridor_flows_path,
        assignment_debug_path=assignment_debug_path,
        rows_written=int(len(corridor_df)),
    )
