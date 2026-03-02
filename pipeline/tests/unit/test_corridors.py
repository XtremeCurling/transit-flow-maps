from pathlib import Path

from transit_flow_maps.corridors.corridors import build_corridor_plans
from transit_flow_maps.util.config import load_runtime_config


def test_build_corridor_plans_contains_market_segments() -> None:
    config_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "transit_flow_maps"
        / "config"
        / "default.yaml"
    )
    runtime_config = load_runtime_config(config_path)

    plans = build_corridor_plans(runtime_config)

    assert "market" in plans
    market = plans["market"]
    assert market.corridor_id == "market"
    assert len(market.segments) > 0
    assert market.segments[0].corridor_segment_id == "market:0000"
    assert all(seg.corridor_id == "market" for seg in market.segments)


def test_corridor_segment_ids_are_sequential() -> None:
    config_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "transit_flow_maps"
        / "config"
        / "default.yaml"
    )
    runtime_config = load_runtime_config(config_path)
    market = build_corridor_plans(runtime_config)["market"]

    ids = [segment.corridor_segment_id for segment in market.segments]
    expected = [f"market:{idx:04d}" for idx in range(len(market.segments))]
    assert ids == expected
