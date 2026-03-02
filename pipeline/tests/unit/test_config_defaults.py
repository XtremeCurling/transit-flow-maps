from __future__ import annotations

from pathlib import Path

import yaml


def test_default_config_contains_locked_values() -> None:
    config_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "transit_flow_maps"
        / "config"
        / "default.yaml"
    )

    with config_path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    assert cfg["h3_resolution"] == 11
    assert cfg["edge_pos_bins"] == 6
    assert cfg["bearing_bucket_count"] == 12
    assert cfg["densify_spacing_m"] == 20
    assert cfg["snap_hard_cap_m"] == 75
    assert cfg["crs_metric_default"] == "EPSG:26910"
