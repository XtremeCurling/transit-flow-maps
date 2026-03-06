from __future__ import annotations

from types import SimpleNamespace
from typing import cast

from shapely.geometry import LineString

from transit_flow_maps.corridors.assign_to_corridors import (
    _assign_segment_ids_by_membership,
    _build_corridor_pair_index,
)
from transit_flow_maps.corridors.corridors import CorridorPlan, CorridorSegment


def _corridor_segment(
    corridor_id: str,
    corridor_segment_id: str,
    corridor_index: int,
    cell_lo: str,
    cell_hi: str,
) -> CorridorSegment:
    metric_line = LineString([(0.0, 0.0), (1.0, 1.0)])
    return CorridorSegment(
        corridor_id=corridor_id,
        corridor_segment_id=corridor_segment_id,
        corridor_index=corridor_index,
        h3_segment_id=f"{corridor_id}:{corridor_segment_id}",
        cell_lo=cell_lo,
        cell_hi=cell_hi,
        geom_wkb=bytes(metric_line.wkb),
        bearing_undirected_deg=45.0,
        metric_line=metric_line,
    )


def test_membership_assignment_matches_exact_pair() -> None:
    segment = _corridor_segment("market", "market:0000", 0, "a", "b")
    corridor_pair_index = {("a", "b"): [segment]}

    assignments, rows = _assign_segment_ids_by_membership(
        ["seg_1"],
        {"seg_1": ("a", "b")},
        corridor_pair_index,
    )

    assert assignments["seg_1"].corridor_segment_id == "market:0000"
    assert rows[0]["reason"] == "exact_pair"


def test_membership_assignment_unmatched_pair() -> None:
    assignments, rows = _assign_segment_ids_by_membership(
        ["seg_2"],
        {"seg_2": ("x", "y")},
        {},
    )

    assert assignments == {}
    assert rows[0]["segment_id"] == "seg_2"
    assert rows[0]["reason"] == "unmatched_pair"


def test_membership_assignment_tiebreak_is_deterministic() -> None:
    shared_pair = ("c1", "c2")
    seg_alpha = _corridor_segment("alpha", "alpha:0003", 3, *shared_pair)
    seg_market = _corridor_segment("market", "market:0001", 1, *shared_pair)

    plans = cast(
        dict[str, CorridorPlan],
        {
            "market": SimpleNamespace(segments=[seg_market]),
            "alpha": SimpleNamespace(segments=[seg_alpha]),
        },
    )
    corridor_pair_index = _build_corridor_pair_index(plans)

    assignments, rows = _assign_segment_ids_by_membership(
        ["seg_3"],
        {"seg_3": shared_pair},
        corridor_pair_index,
    )

    assert assignments["seg_3"].corridor_id == "alpha"
    assert rows[0]["candidate_count"] == 2
