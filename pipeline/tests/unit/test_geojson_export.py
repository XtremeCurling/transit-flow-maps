from transit_flow_maps.export.geojson import _determine_time_basis


def test_determine_time_basis_single_value() -> None:
    assert _determine_time_basis({"weekday_average"}) == "weekday_average"


def test_determine_time_basis_mixed_value() -> None:
    assert _determine_time_basis({"weekday_average", "saturday_average"}) == "mixed"


def test_determine_time_basis_empty_defaults_weekday_average() -> None:
    assert _determine_time_basis(set()) == "weekday_average"
