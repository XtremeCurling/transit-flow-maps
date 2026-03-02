"""GeoJSON export helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import SupportsFloat, cast

import pandas as pd
from shapely import wkb
from shapely.geometry import LineString

from transit_flow_maps.util.config import RuntimeConfig, ensure_output_directories
from transit_flow_maps.util.logging import get_logger


@dataclass(frozen=True)
class ExportGeoJSONArtifacts:
    """Output path metadata for export-geojson."""

    output_path: Path
    rows_written: int


@dataclass
class PhysicalAggregateState:
    """Aggregation state for physical segment export."""

    daily_riders: float = 0.0
    agencies: set[str] = field(default_factory=set)
    modes: set[str] = field(default_factory=set)
    routes: set[str] = field(default_factory=set)
    time_bases: set[str] = field(default_factory=set)
    source_breakdown: dict[tuple[str, str], dict[str, object]] = field(default_factory=dict)


def _parse_json_list(value: object) -> list[str]:
    if value is None:
        return []
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return []
    try:
        decoded = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(decoded, list):
        return []
    return [str(item) for item in decoded]


def _safe_float(value: object) -> float:
    if value is None:
        return 0.0
    try:
        return float(cast(SupportsFloat | str | bytes | bytearray, value))
    except (TypeError, ValueError):
        return 0.0


def _line_coordinates_from_wkb(geom_wkb: bytes) -> list[list[float]]:
    geom = wkb.loads(geom_wkb)
    if not isinstance(geom, LineString):
        return []
    return [[float(x), float(y)] for x, y in geom.coords]


def _segment_geom_lookup(segment_keys_path: Path) -> dict[str, bytes]:
    columns = ["segment_id", "geom_wkb", "agency", "route_id", "shape_id"]
    segment_keys = pd.read_parquet(segment_keys_path, columns=columns)
    if segment_keys.empty:
        return {}

    ordered = segment_keys.sort_values(["segment_id", "agency", "route_id", "shape_id"])
    grouped = ordered.groupby("segment_id", as_index=False).agg(geom_wkb=("geom_wkb", "first"))
    return {
        str(row["segment_id"]): bytes(row["geom_wkb"])
        for _, row in grouped.iterrows()
    }


def _determine_time_basis(values: set[str]) -> str:
    if len(values) == 1:
        return sorted(values)[0]
    if len(values) > 1:
        return "mixed"
    return "weekday_average"


def _write_geojson(features: list[dict[str, object]], output_path: Path) -> None:
    payload = {"type": "FeatureCollection", "features": features}
    output_path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")


def _export_physical(runtime_config: RuntimeConfig) -> ExportGeoJSONArtifacts:
    logger = get_logger(__name__)
    segment_flows_path = runtime_config.paths.interim_dir / "segment_flows.parquet"
    segment_keys_path = runtime_config.paths.interim_dir / "segment_keys.parquet"
    if not segment_flows_path.exists():
        raise FileNotFoundError(f"Missing segment flows parquet: {segment_flows_path}")
    if not segment_keys_path.exists():
        raise FileNotFoundError(f"Missing segment keys parquet: {segment_keys_path}")

    flows = pd.read_parquet(segment_flows_path)
    required = {"segment_id", "daily_riders", "agency", "mode", "routes_json", "time_basis"}
    missing = sorted(required - set(flows.columns))
    if missing:
        raise ValueError(f"segment_flows.parquet missing required columns: {missing}")

    geom_lookup = _segment_geom_lookup(segment_keys_path)
    if not geom_lookup:
        raise RuntimeError("No segment geometry available for physical export")

    states: dict[str, PhysicalAggregateState] = {}
    for _, row in flows.iterrows():
        segment_id = str(row["segment_id"])
        if segment_id not in geom_lookup:
            continue

        state = states.setdefault(segment_id, PhysicalAggregateState())
        agency = str(row["agency"])
        mode = str(row["mode"])
        routes = _parse_json_list(row["routes_json"])
        riders = _safe_float(row["daily_riders"])
        time_basis = str(row["time_basis"])

        state.daily_riders += riders
        state.agencies.add(agency)
        state.modes.add(mode)
        state.routes.update(routes)
        state.time_bases.add(time_basis)

        breakdown_key = (agency, mode)
        breakdown = state.source_breakdown.setdefault(
            breakdown_key,
            {"daily_riders": 0.0, "routes": set(), "time_basis": set()},
        )
        breakdown["daily_riders"] = _safe_float(breakdown.get("daily_riders")) + riders
        breakdown_routes = breakdown.get("routes")
        if isinstance(breakdown_routes, set):
            breakdown_routes.update(routes)
        breakdown_time_bases = breakdown.get("time_basis")
        if isinstance(breakdown_time_bases, set):
            breakdown_time_bases.add(time_basis)

    features: list[dict[str, object]] = []
    for segment_id in sorted(states):
        state = states[segment_id]
        geom_wkb = geom_lookup.get(segment_id)
        if geom_wkb is None:
            continue

        coordinates = _line_coordinates_from_wkb(geom_wkb)
        if len(coordinates) < 2:
            continue

        breakdown_rows: list[dict[str, object]] = []
        for agency, mode in sorted(state.source_breakdown):
            breakdown = state.source_breakdown[(agency, mode)]
            breakdown_routes = breakdown.get("routes")
            routes_list = sorted(breakdown_routes) if isinstance(breakdown_routes, set) else []
            breakdown_time_basis = breakdown.get("time_basis")
            time_basis_values = (
                breakdown_time_basis if isinstance(breakdown_time_basis, set) else set()
            )
            breakdown_rows.append(
                {
                    "agency": agency,
                    "mode": mode,
                    "daily_riders": round(_safe_float(breakdown.get("daily_riders")), 6),
                    "routes": routes_list,
                    "time_basis": _determine_time_basis(time_basis_values),
                }
            )

        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coordinates},
                "properties": {
                    "segment_id": segment_id,
                    "daily_riders": round(float(state.daily_riders), 6),
                    "time_basis": _determine_time_basis(state.time_bases),
                    "routes": sorted(state.routes),
                    "agencies": sorted(state.agencies),
                    "modes": sorted(state.modes),
                    "source_breakdown": breakdown_rows,
                },
            }
        )

    output_path = runtime_config.paths.web_dir / "physical.geojson"
    _write_geojson(features, output_path)
    logger.info("Exported physical GeoJSON features: %s", len(features))
    return ExportGeoJSONArtifacts(output_path=output_path, rows_written=len(features))


def _export_corridor(runtime_config: RuntimeConfig) -> ExportGeoJSONArtifacts:
    logger = get_logger(__name__)
    corridor_path = runtime_config.paths.interim_dir / "corridor_flows.parquet"
    if not corridor_path.exists():
        raise FileNotFoundError(f"Missing corridor flows parquet: {corridor_path}")

    corridor_df = pd.read_parquet(corridor_path)
    required = {
        "corridor_id",
        "corridor_segment_id",
        "daily_riders",
        "agencies_json",
        "routes_json",
        "geom_wkb",
        "time_basis",
    }
    missing = sorted(required - set(corridor_df.columns))
    if missing:
        raise ValueError(f"corridor_flows.parquet missing required columns: {missing}")

    corridor_df = corridor_df.sort_values(["corridor_id", "corridor_segment_id"])
    features: list[dict[str, object]] = []
    for _, row in corridor_df.iterrows():
        coordinates = _line_coordinates_from_wkb(bytes(row["geom_wkb"]))
        if len(coordinates) < 2:
            continue

        agencies = _parse_json_list(row["agencies_json"])
        routes = _parse_json_list(row["routes_json"])
        modes = _parse_json_list(row["modes_json"]) if "modes_json" in corridor_df.columns else []
        source_breakdown: object
        if "source_breakdown_json" in corridor_df.columns:
            try:
                source_breakdown = json.loads(str(row["source_breakdown_json"]))
            except json.JSONDecodeError:
                source_breakdown = []
        else:
            source_breakdown = []

        corridor_segment_id = str(row["corridor_segment_id"])
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "LineString", "coordinates": coordinates},
                "properties": {
                    "corridor_id": str(row["corridor_id"]),
                    "corridor_segment_id": corridor_segment_id,
                    "segment_id": corridor_segment_id,
                    "daily_riders": round(_safe_float(row["daily_riders"]), 6),
                    "time_basis": str(row["time_basis"]),
                    "routes": routes,
                    "agencies": agencies,
                    "modes": modes,
                    "source_breakdown": source_breakdown,
                },
            }
        )

    output_path = runtime_config.paths.web_dir / "corridor.geojson"
    _write_geojson(features, output_path)
    logger.info("Exported corridor GeoJSON features: %s", len(features))
    return ExportGeoJSONArtifacts(output_path=output_path, rows_written=len(features))


def export_geojson(runtime_config: RuntimeConfig, *, view: str) -> ExportGeoJSONArtifacts:
    """Export web GeoJSON for the requested view."""
    ensure_output_directories(runtime_config.paths)
    normalized = view.lower()
    if normalized == "physical":
        return _export_physical(runtime_config)
    if normalized == "corridor":
        return _export_corridor(runtime_config)
    raise ValueError(f"Unsupported view: {view}")
