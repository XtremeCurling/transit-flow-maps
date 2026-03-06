from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from shapely.geometry import LineString

from transit_flow_maps.export.geojson import _determine_time_basis, export_geojson
from transit_flow_maps.util.config import ResolvedPaths, RuntimeConfig, Settings


def _runtime_config(tmp_path: Path) -> RuntimeConfig:
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
    return RuntimeConfig(settings=Settings(), paths=paths)


def _read_geojson(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_determine_time_basis_single_value() -> None:
    assert _determine_time_basis({"weekday_average"}) == "weekday_average"


def test_determine_time_basis_mixed_value() -> None:
    assert _determine_time_basis({"weekday_average", "saturday_average"}) == "mixed"


def test_determine_time_basis_empty_defaults_weekday_average() -> None:
    assert _determine_time_basis(set()) == "weekday_average"


def test_export_physical_outputs_all_and_flows_with_join_coverage(tmp_path: Path) -> None:
    runtime_config = _runtime_config(tmp_path)

    geom_a = bytes(LineString([(-122.40, 37.78), (-122.41, 37.79)]).wkb)
    geom_b = bytes(LineString([(-122.42, 37.77), (-122.43, 37.76)]).wkb)

    segment_keys = pd.DataFrame(
        [
            {
                "segment_id": "seg_a",
                "cell_lo": "c1",
                "cell_hi": "c2",
                "edge_pos_bin": 0,
                "bearing_bucket": 0,
                "agency": "SFMTA",
                "mode": "bus",
                "route_id": "5",
                "shape_id": "shape_a",
                "geom_wkb": geom_a,
                "is_repaired": False,
            },
            {
                "segment_id": "seg_b",
                "cell_lo": "c3",
                "cell_hi": "c4",
                "edge_pos_bin": 0,
                "bearing_bucket": 0,
                "agency": "SFMTA",
                "mode": "tram",
                "route_id": "N",
                "shape_id": "shape_b",
                "geom_wkb": geom_b,
                "is_repaired": False,
            },
        ]
    )
    segment_keys.to_parquet(runtime_config.paths.interim_dir / "segment_keys.parquet", index=False)

    segment_flows = pd.DataFrame(
        [
            {
                "segment_id": "seg_a",
                "daily_riders": 1200.0,
                "agency": "SFMTA",
                "mode": "bus",
                "routes_json": json.dumps(["5"]),
                "time_basis": "weekday_average",
            },
            {
                "segment_id": "seg_c",
                "daily_riders": 800.0,
                "agency": "BART",
                "mode": "subway",
                "routes_json": json.dumps(["YELLOW"]),
                "time_basis": "weekday_average",
            },
        ]
    )
    segment_flows.to_parquet(runtime_config.paths.interim_dir / "segment_flows.parquet", index=False)

    artifacts = export_geojson(runtime_config, view="physical")

    assert artifacts.output_path.name == "physical.geojson"
    assert artifacts.join_coverage_path == runtime_config.paths.debug_dir / "join_coverage.csv"
    assert artifacts.join_coverage_stats is not None
    assert artifacts.join_coverage_stats.intersection_count == 1

    physical_all = _read_geojson(runtime_config.paths.web_dir / "physical_all.geojson")
    physical_flows = _read_geojson(runtime_config.paths.web_dir / "physical_flows.geojson")
    physical_legacy = _read_geojson(runtime_config.paths.web_dir / "physical.geojson")

    all_ids = {
        str(feature["properties"]["segment_id"])
        for feature in physical_all["features"]
        if isinstance(feature, dict)
    }
    flow_ids = {
        str(feature["properties"]["segment_id"])
        for feature in physical_flows["features"]
        if isinstance(feature, dict)
    }

    assert all_ids == {"seg_a", "seg_b"}
    assert flow_ids == {"seg_a"}
    assert physical_legacy["features"] == physical_flows["features"]

    coverage = pd.read_csv(runtime_config.paths.debug_dir / "join_coverage.csv")
    status_by_id = {
        str(row["segment_id"]): str(row["status"])
        for _, row in coverage.iterrows()
    }
    assert status_by_id == {
        "seg_a": "matched",
        "seg_b": "key_only",
        "seg_c": "flow_only",
    }


def test_export_corridor_outputs_all_and_flows(tmp_path: Path) -> None:
    runtime_config = _runtime_config(tmp_path)

    corridor_df = pd.DataFrame(
        [
            {
                "corridor_id": "market",
                "corridor_segment_id": "market:0000",
                "daily_riders": 2500.0,
                "agencies_json": json.dumps(["SFMTA"]),
                "routes_json": json.dumps(["F", "KT"]),
                "geom_wkb": bytes(LineString([(-122.4, 37.79), (-122.41, 37.78)]).wkb),
                "time_basis": "weekday_average",
                "modes_json": json.dumps(["tram"]),
                "source_breakdown_json": json.dumps([]),
            },
            {
                "corridor_id": "market",
                "corridor_segment_id": "market:0001",
                "daily_riders": 0.0,
                "agencies_json": json.dumps([]),
                "routes_json": json.dumps([]),
                "geom_wkb": bytes(LineString([(-122.41, 37.78), (-122.42, 37.77)]).wkb),
                "time_basis": "weekday_average",
                "modes_json": json.dumps([]),
                "source_breakdown_json": json.dumps([]),
            },
        ]
    )
    corridor_df.to_parquet(runtime_config.paths.interim_dir / "corridor_flows.parquet", index=False)

    artifacts = export_geojson(runtime_config, view="corridor")

    assert artifacts.output_path.name == "corridor.geojson"

    corridor_all = _read_geojson(runtime_config.paths.web_dir / "corridor_all.geojson")
    corridor_flows = _read_geojson(runtime_config.paths.web_dir / "corridor_flows.geojson")
    corridor_legacy = _read_geojson(runtime_config.paths.web_dir / "corridor.geojson")

    assert len(corridor_all["features"]) == 2
    assert len(corridor_flows["features"]) == 1
    assert corridor_legacy["features"] == corridor_all["features"]
