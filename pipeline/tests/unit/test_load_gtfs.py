from __future__ import annotations

import pytest

from transit_flow_maps.gtfs.load_gtfs import _resolve_zip_member_name


def test_resolve_zip_member_name_exact_match() -> None:
    names = ["shapes.txt", "trips.txt"]
    assert _resolve_zip_member_name(names, "shapes.txt") == "shapes.txt"


def test_resolve_zip_member_name_nested_gtfs_folder() -> None:
    names = [
        "sfmta/",
        "sfmta/shapes.txt",
        "sfmta/trips.txt",
        "__MACOSX/sfmta/._shapes.txt",
    ]
    assert _resolve_zip_member_name(names, "shapes.txt") == "sfmta/shapes.txt"


def test_resolve_zip_member_name_prefers_shallowest_path() -> None:
    names = [
        "nested/gtfs/shapes.txt",
        "gtfs/shapes.txt",
    ]
    assert _resolve_zip_member_name(names, "shapes.txt") == "gtfs/shapes.txt"


def test_resolve_zip_member_name_raises_when_missing() -> None:
    with pytest.raises(KeyError):
        _resolve_zip_member_name(["stops.txt"], "shapes.txt")
