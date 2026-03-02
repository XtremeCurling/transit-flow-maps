from __future__ import annotations

from transit_flow_maps.conflation.simplify import clean_cell_sequence


def test_clean_cell_sequence_collapses_adjacent_duplicates() -> None:
    assert clean_cell_sequence(["a", "a", "b", "b", "c"]) == ["a", "b", "c"]


def test_clean_cell_sequence_collapses_aba() -> None:
    assert clean_cell_sequence(["a", "b", "a", "c"]) == ["a", "c"]


def test_clean_cell_sequence_collapses_abcb() -> None:
    assert clean_cell_sequence(["a", "b", "c", "b", "d"]) == ["a", "b", "d"]


def test_clean_cell_sequence_stabilizes_after_multiple_passes() -> None:
    # First pass: a,b,a -> a ; resulting a,c,b,c -> collapses to a,c
    assert clean_cell_sequence(["a", "b", "a", "c", "b", "c"]) == ["a", "c"]
