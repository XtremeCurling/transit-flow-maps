"""Corridor definitions and deterministic segmentation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import SupportsBytes, SupportsFloat, cast

from shapely import wkb
from shapely.geometry import LineString

from transit_flow_maps.conflation.densify import (
    bearing_degrees_xy,
    densify_linestring_lonlat,
    fold_undirected_bearing,
)
from transit_flow_maps.conflation.segment_keys import (
    SegmentKeyConfig,
    build_segment_keys_for_shape,
)
from transit_flow_maps.util.config import RuntimeConfig
from transit_flow_maps.util.crs import CRSContext, build_crs_context, choose_metric_crs


@dataclass(frozen=True)
class CorridorSegment:
    """One ordered corridor segment."""

    corridor_id: str
    corridor_segment_id: str
    corridor_index: int
    h3_segment_id: str
    geom_wkb: bytes
    bearing_undirected_deg: float
    metric_line: LineString


@dataclass(frozen=True)
class CorridorPlan:
    """Precomputed geometry and segments for a corridor."""

    corridor_id: str
    centerline_lonlat: list[tuple[float, float]]
    centerline_metric: LineString
    crs_context: CRSContext
    segments: list[CorridorSegment]


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


def _coerce_float(value: object) -> float:
    return float(cast(SupportsFloat | str | bytes | bytearray, value))


def _coerce_bytes(value: object) -> bytes:
    return bytes(cast(SupportsBytes | bytes | bytearray, value))


def _market_centerline_lonlat() -> list[tuple[float, float]]:
    # Approximate Market Street centerline from Ferry Building to Castro.
    return [
        (-122.39357, 37.79545),
        (-122.39753, 37.79296),
        (-122.40227, 37.78994),
        (-122.40727, 37.78685),
        (-122.41221, 37.78381),
        (-122.41723, 37.78071),
        (-122.42213, 37.77695),
        (-122.42679, 37.77342),
        (-122.43170, 37.76966),
    ]


def corridor_centerlines() -> dict[str, list[tuple[float, float]]]:
    """Return v1 corridor centerline polylines."""
    return {"market": _market_centerline_lonlat()}


def _to_metric_line(
    points_lonlat: list[tuple[float, float]],
    crs_context: CRSContext,
) -> LineString:
    metric_coords = [
        tuple(float(v) for v in crs_context.to_metric.transform(lon, lat))
        for lon, lat in points_lonlat
    ]
    return LineString(metric_coords)


def _undirected_bearing_for_metric_line(line: LineString) -> float:
    coords = list(line.coords)
    if len(coords) < 2:
        return 0.0
    start = (float(coords[0][0]), float(coords[0][1]))
    end = (float(coords[-1][0]), float(coords[-1][1]))
    bearing = bearing_degrees_xy(start, end)
    if bearing is None:
        return 0.0
    return float(fold_undirected_bearing(bearing))


def build_corridor_plans(runtime_config: RuntimeConfig) -> dict[str, CorridorPlan]:
    """Build deterministic corridor segment plans."""
    plans: dict[str, CorridorPlan] = {}
    segment_config = _segment_key_config(runtime_config)

    for corridor_id, centerline in sorted(corridor_centerlines().items()):
        metric_crs = choose_metric_crs(centerline, runtime_config.settings.crs_metric_default)
        crs_context = build_crs_context(metric_crs)
        sampled = densify_linestring_lonlat(
            centerline,
            spacing_m=runtime_config.settings.corridor_sample_spacing_m,
            to_metric=crs_context.to_metric,
            to_wgs84=crs_context.to_wgs84,
        )
        centerline_metric = _to_metric_line(sampled, crs_context)

        result = build_segment_keys_for_shape(
            shape_id=f"corridor:{corridor_id}",
            shape_points_lonlat=sampled,
            config=segment_config,
            pre_densified=True,
        )
        record_count = min(len(result.transition_records), len(result.records))
        if record_count == 0:
            raise RuntimeError(f"No corridor segments produced for corridor={corridor_id}")

        indexed_records: list[tuple[float, int, dict[str, object]]] = []
        for idx in range(record_count):
            transition = result.transition_records[idx]
            record = result.records[idx]
            midpoint_measure = _coerce_float(transition.get("midpoint_measure_m", 0.0))
            indexed_records.append((midpoint_measure, idx, record))
        indexed_records.sort(
            key=lambda item: (item[0], str(item[2].get("segment_id", "")), item[1])
        )

        segments: list[CorridorSegment] = []
        for seg_idx, (_, _, record) in enumerate(indexed_records):
            geom_wkb = _coerce_bytes(record["geom_wkb"])
            line_lonlat = wkb.loads(geom_wkb)
            if not isinstance(line_lonlat, LineString):
                continue

            metric_line = _to_metric_line(
                [(float(x), float(y)) for x, y in line_lonlat.coords],
                crs_context,
            )
            corridor_segment_id = f"{corridor_id}:{seg_idx:04d}"
            segments.append(
                CorridorSegment(
                    corridor_id=corridor_id,
                    corridor_segment_id=corridor_segment_id,
                    corridor_index=seg_idx,
                    h3_segment_id=str(record["segment_id"]),
                    geom_wkb=geom_wkb,
                    bearing_undirected_deg=_undirected_bearing_for_metric_line(metric_line),
                    metric_line=metric_line,
                )
            )

        if not segments:
            raise RuntimeError(f"No valid corridor LineStrings for corridor={corridor_id}")

        plans[corridor_id] = CorridorPlan(
            corridor_id=corridor_id,
            centerline_lonlat=centerline,
            centerline_metric=centerline_metric,
            crs_context=crs_context,
            segments=segments,
        )

    return plans
