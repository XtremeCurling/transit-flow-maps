"""GTFS shapes processing and segment-key build orchestration."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import SupportsBytes, SupportsInt, cast

import pandas as pd
from shapely import wkb

from transit_flow_maps.conflation.segment_keys import (
    SegmentKeyConfig,
    build_segment_keys_for_shape,
)
from transit_flow_maps.gtfs.load_gtfs import load_gtfs_bundle
from transit_flow_maps.gtfs.routes import route_mode_lookup, shape_to_routes
from transit_flow_maps.util.config import RuntimeConfig, ensure_output_directories
from transit_flow_maps.util.logging import get_logger


@dataclass(frozen=True)
class BuildSegmentsArtifacts:
    """Output paths produced by build-segments."""

    segment_keys_path: Path
    debug_geojson_path: Path
    repairs_csv_path: Path
    summary_csv_path: Path
    rows_written: int


def _coerce_to_int(value: object) -> int:
    return int(cast(SupportsInt | str | bytes | bytearray, value))


def _coerce_to_bytes(value: object) -> bytes:
    return bytes(cast(SupportsBytes | bytes | bytearray, value))


def _segment_key_config(runtime_config: RuntimeConfig) -> SegmentKeyConfig:
    settings = runtime_config.settings
    return SegmentKeyConfig(
        h3_resolution=settings.h3_resolution,
        edge_pos_bins=settings.edge_pos_bins,
        bearing_bucket_count=settings.bearing_bucket_count,
        densify_spacing_m=settings.densify_spacing_m,
        non_neighbor_max_path_cells=settings.non_neighbor_max_path_cells,
        non_neighbor_max_recursion_depth=settings.non_neighbor_max_recursion_depth,
        crs_metric_default=settings.crs_metric_default,
    )


def _agency_zip_pairs(runtime_config: RuntimeConfig) -> list[tuple[str, Path]]:
    return [
        ("SFMTA", runtime_config.paths.sfmta_gtfs_zip),
        ("BART", runtime_config.paths.bart_gtfs_zip),
    ]


def _shape_points(group: pd.DataFrame) -> list[tuple[float, float]]:
    sorted_group = group.sort_values("shape_pt_sequence")
    points = [
        (float(row["shape_pt_lon"]), float(row["shape_pt_lat"]))
        for _, row in sorted_group.iterrows()
    ]

    deduped: list[tuple[float, float]] = []
    for point in points:
        if not deduped or deduped[-1] != point:
            deduped.append(point)
    return deduped


def _build_debug_geojson(df: pd.DataFrame, output_path: Path) -> None:
    summary = (
        df.groupby("segment_id", as_index=False)
        .agg(
            daily_rows=("segment_id", "size"),
            agencies=("agency", lambda s: sorted(set(str(v) for v in s))),
            routes=("route_id", lambda s: sorted(set(str(v) for v in s))),
            modes=("mode", lambda s: sorted(set(str(v) for v in s))),
            is_repaired=("is_repaired", "max"),
            edge_pos_bin=("edge_pos_bin", "first"),
            bearing_bucket=("bearing_bucket", "first"),
            geom_wkb=("geom_wkb", "first"),
        )
        .sort_values("segment_id")
    )

    features: list[dict[str, object]] = []
    for _, row in summary.iterrows():
        geom = wkb.loads(bytes(row["geom_wkb"]))
        coordinates = [[float(x), float(y)] for x, y in geom.coords]
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coordinates},
                "properties": {
                    "segment_id": str(row["segment_id"]),
                    "contributors": int(row["daily_rows"]),
                    "agencies": list(row["agencies"]),
                    "routes": list(row["routes"]),
                    "modes": list(row["modes"]),
                    "is_repaired": bool(row["is_repaired"]),
                    "edge_pos_bin": int(row["edge_pos_bin"]),
                    "bearing_bucket": int(row["bearing_bucket"]),
                },
            }
        )

    payload = {"type": "FeatureCollection", "features": features}
    output_path.write_text(json.dumps(payload), encoding="utf-8")


def build_segments(runtime_config: RuntimeConfig) -> BuildSegmentsArtifacts:
    """Build segment_keys.parquet and debug artifacts from configured GTFS sources."""
    logger = get_logger(__name__)
    ensure_output_directories(runtime_config.paths)

    segment_config = _segment_key_config(runtime_config)

    all_records: list[dict[str, object]] = []
    all_repair_logs: list[dict[str, object]] = []

    for agency, zip_path in _agency_zip_pairs(runtime_config):
        if not zip_path.exists():
            logger.warning("Skipping agency %s because GTFS zip is missing: %s", agency, zip_path)
            continue

        logger.info("Loading GTFS for agency=%s from %s", agency, zip_path)
        bundle = load_gtfs_bundle(agency=agency, zip_path=zip_path)
        modes = route_mode_lookup(bundle.routes)
        routes_by_shape = shape_to_routes(bundle.trips)

        for shape_id, shape_rows in bundle.shapes.groupby("shape_id", sort=True):
            points = _shape_points(shape_rows)
            if len(points) < 2:
                continue

            result = build_segment_keys_for_shape(
                shape_id=str(shape_id),
                shape_points_lonlat=points,
                config=segment_config,
            )

            route_ids = routes_by_shape.get(str(shape_id), ["unknown_route"])
            for route_id in route_ids:
                mode = modes.get(route_id, "unknown")
                for row in result.records:
                    record = {
                        "segment_id": str(row["segment_id"]),
                        "cell_lo": str(row["cell_lo"]),
                        "cell_hi": str(row["cell_hi"]),
                        "edge_pos_bin": _coerce_to_int(row["edge_pos_bin"]),
                        "bearing_bucket": _coerce_to_int(row["bearing_bucket"]),
                        "agency": agency,
                        "mode": mode,
                        "route_id": route_id,
                        "shape_id": str(shape_id),
                        "geom_wkb": _coerce_to_bytes(row["geom_wkb"]),
                        "is_repaired": bool(row["is_repaired"]),
                    }
                    all_records.append(record)

            for log in result.repair_logs:
                row = dict(log)
                row["agency"] = agency
                all_repair_logs.append(row)

    if not all_records:
        raise RuntimeError(
            "No segment records were produced. Verify GTFS zip paths and shapes.txt availability."
        )

    df = pd.DataFrame(all_records)
    df = df.sort_values(["agency", "route_id", "shape_id", "segment_id"])  # deterministic ordering

    # Keep one row per contributor-segment identity while preserving deterministic metadata.
    grouped = (
        df.groupby(["segment_id", "agency", "mode", "route_id", "shape_id"], as_index=False)
        .agg(
            cell_lo=("cell_lo", "first"),
            cell_hi=("cell_hi", "first"),
            edge_pos_bin=("edge_pos_bin", "first"),
            bearing_bucket=("bearing_bucket", "first"),
            geom_wkb=("geom_wkb", "first"),
            is_repaired=("is_repaired", "max"),
        )
        .sort_values(["agency", "route_id", "shape_id", "segment_id"])
    )

    segment_keys_path = runtime_config.paths.interim_dir / "segment_keys.parquet"
    grouped = grouped[
        [
            "segment_id",
            "cell_lo",
            "cell_hi",
            "edge_pos_bin",
            "bearing_bucket",
            "agency",
            "mode",
            "route_id",
            "shape_id",
            "geom_wkb",
            "is_repaired",
        ]
    ]
    grouped.to_parquet(segment_keys_path, index=False)

    repairs_csv_path = runtime_config.paths.debug_dir / "non_neighbor_repairs.csv"
    repairs_df = pd.DataFrame(all_repair_logs)
    if repairs_df.empty:
        repairs_df = pd.DataFrame(
            columns=[
                "shape_id",
                "from_cell",
                "to_cell",
                "status",
                "method",
                "path_len",
                "path",
                "agency",
            ]
        )
    repairs_df = repairs_df[
        ["agency", "shape_id", "from_cell", "to_cell", "status", "method", "path_len", "path"]
    ].sort_values(["agency", "shape_id", "from_cell", "to_cell"])
    repairs_df.to_csv(repairs_csv_path, index=False)

    debug_geojson_path = runtime_config.paths.debug_dir / "segment_keys_debug.geojson"
    _build_debug_geojson(grouped, debug_geojson_path)

    summary_csv_path = runtime_config.paths.debug_dir / "segment_build_summary.csv"
    summary_rows = [
        {
            "metric": "segment_key_rows",
            "value": int(len(grouped)),
        },
        {
            "metric": "unique_segment_ids",
            "value": int(grouped["segment_id"].nunique()),
        },
        {
            "metric": "repair_rows",
            "value": int(len(repairs_df)),
        },
        {
            "metric": "repair_dropped_rows",
            "value": int((repairs_df["status"] == "dropped").sum()) if not repairs_df.empty else 0,
        },
    ]
    pd.DataFrame(summary_rows).to_csv(summary_csv_path, index=False)

    return BuildSegmentsArtifacts(
        segment_keys_path=segment_keys_path,
        debug_geojson_path=debug_geojson_path,
        repairs_csv_path=repairs_csv_path,
        summary_csv_path=summary_csv_path,
        rows_written=int(len(grouped)),
    )
