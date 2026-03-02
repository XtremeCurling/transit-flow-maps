from __future__ import annotations

from transit_flow_maps.flows.muni_stop_loads import (
    _normalize_direction_label,
    _normalize_route_alpha,
)


def test_normalize_route_alpha_strips_leading_zeroes() -> None:
    assert _normalize_route_alpha("001") == "1"
    assert _normalize_route_alpha("014R") == "14R"
    assert _normalize_route_alpha("001X") == "1X"


def test_normalize_direction_label_handles_truncated_tokens() -> None:
    assert _normalize_direction_label("INBOUND") == "inbound"
    assert _normalize_direction_label("OUTBOUND") == "outbound"
    assert _normalize_direction_label("NORTHBOU") == "north"
    assert _normalize_direction_label("SOUTHBOU") == "south"
