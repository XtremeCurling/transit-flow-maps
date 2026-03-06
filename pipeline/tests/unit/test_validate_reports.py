from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from transit_flow_maps.util.config import ResolvedPaths, RuntimeConfig, Settings
from transit_flow_maps.validate.reports import run_validation


def _runtime_config(tmp_path: Path, *, max_unmatched_flow_pct: float = 5.0) -> RuntimeConfig:
    paths = ResolvedPaths(
        data_root=tmp_path,
        interim_dir=tmp_path / "interim",
        debug_dir=tmp_path / "debug",
        web_dir=tmp_path / "web",
        sfmta_gtfs_zip=tmp_path / "sfmta.zip",
        bart_gtfs_zip=tmp_path / "bart.zip",
    )
    paths.interim_dir.mkdir(parents=True, exist_ok=True)
    paths.debug_dir.mkdir(parents=True, exist_ok=True)
    paths.web_dir.mkdir(parents=True, exist_ok=True)
    settings = Settings(validate_max_unmatched_flow_pct=max_unmatched_flow_pct)
    return RuntimeConfig(settings=settings, paths=paths)


def test_run_validation_writes_reports_and_schema(tmp_path: Path) -> None:
    runtime_config = _runtime_config(tmp_path, max_unmatched_flow_pct=5.0)

    pd.DataFrame(
        [
            {"segment_id": "seg_1"},
            {"segment_id": "seg_2"},
        ]
    ).to_parquet(runtime_config.paths.interim_dir / "segment_keys.parquet", index=False)

    pd.DataFrame(
        [
            {
                "segment_id": "seg_1",
                "daily_riders": 100.0,
                "agency": "SFMTA",
                "routes_json": json.dumps(["5", "5R"]),
            },
            {
                "segment_id": "seg_2",
                "daily_riders": 50.0,
                "agency": "BART",
                "routes_json": json.dumps(["YELLOW"]),
            },
        ]
    ).to_parquet(runtime_config.paths.interim_dir / "segment_flows.parquet", index=False)

    pd.DataFrame(
        [
            {"route_id": "5", "direction_label": "inbound", "direction_id": "1", "reason": "x"},
            {"route_id": "N", "direction_label": "outbound", "direction_id": "0", "reason": "x"},
            {"route_id": "J", "direction_label": "inbound", "direction_id": "1", "reason": "y"},
        ]
    ).to_csv(runtime_config.paths.debug_dir / "excluded_route_directions.csv", index=False)

    pd.DataFrame(
        [
            {"route_id": "5", "reason": "no_gtfs_match"},
            {"route_id": "N", "reason": "too_far"},
            {"route_id": "N", "reason": "too_far"},
        ]
    ).to_csv(runtime_config.paths.debug_dir / "unsnapped_or_far_snaps.csv", index=False)

    artifacts = run_validation(runtime_config)

    assert artifacts.passed is True
    assert artifacts.flow_only_pct == 0.0

    summary = pd.read_csv(artifacts.summary_path)
    top_segments = pd.read_csv(artifacts.top_segments_path)
    route_throughput = pd.read_csv(artifacts.route_throughput_path)

    assert {"metric", "value"}.issubset(summary.columns)
    assert {"agency", "rank", "segment_id", "daily_riders"}.issubset(top_segments.columns)
    assert {"agency", "route_id", "approx_daily_riders", "segment_rows"}.issubset(
        route_throughput.columns
    )


def test_run_validation_fails_gate_when_flow_only_pct_exceeds_threshold(tmp_path: Path) -> None:
    runtime_config = _runtime_config(tmp_path, max_unmatched_flow_pct=20.0)

    pd.DataFrame([{"segment_id": "seg_1"}]).to_parquet(
        runtime_config.paths.interim_dir / "segment_keys.parquet",
        index=False,
    )
    pd.DataFrame(
        [
            {
                "segment_id": "seg_1",
                "daily_riders": 100.0,
                "agency": "SFMTA",
                "routes_json": json.dumps(["5"]),
            },
            {
                "segment_id": "seg_2",
                "daily_riders": 100.0,
                "agency": "SFMTA",
                "routes_json": json.dumps(["5"]),
            },
        ]
    ).to_parquet(runtime_config.paths.interim_dir / "segment_flows.parquet", index=False)

    artifacts = run_validation(runtime_config)

    assert artifacts.passed is False
    assert artifacts.flow_only_pct > artifacts.max_unmatched_flow_pct
