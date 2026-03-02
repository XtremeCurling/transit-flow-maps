"""Geometry densification logic."""

from __future__ import annotations

import math

from pyproj import Transformer
from shapely.geometry import LineString


def _project_points(
    points_lonlat: list[tuple[float, float]],
    transformer: Transformer,
) -> list[tuple[float, float]]:
    return [
        (float(x), float(y))
        for x, y in (transformer.transform(lon, lat) for lon, lat in points_lonlat)
    ]


def _unproject_points(
    points_xy: list[tuple[float, float]],
    transformer: Transformer,
) -> list[tuple[float, float]]:
    return [
        (float(lon), float(lat))
        for lon, lat in (transformer.transform(x, y) for x, y in points_xy)
    ]


def densify_linestring_lonlat(
    points_lonlat: list[tuple[float, float]],
    *,
    spacing_m: float,
    to_metric: Transformer,
    to_wgs84: Transformer,
) -> list[tuple[float, float]]:
    """Densify a polyline in metric space and return lon/lat points."""
    if len(points_lonlat) < 2:
        return points_lonlat

    metric_points = _project_points(points_lonlat, to_metric)
    line = LineString(metric_points)

    if line.length == 0:
        return [points_lonlat[0], points_lonlat[-1]]

    if spacing_m <= 0:
        raise ValueError("spacing_m must be > 0")

    # Ensure segment spacing is <= spacing_m while preserving endpoints.
    interval_count = max(1, int(math.ceil(line.length / spacing_m)))
    step = line.length / interval_count

    samples_xy: list[tuple[float, float]] = []
    for i in range(interval_count + 1):
        distance = min(line.length, i * step)
        point = line.interpolate(distance)
        samples_xy.append((float(point.x), float(point.y)))

    densified_lonlat = _unproject_points(samples_xy, to_wgs84)
    densified_lonlat[0] = points_lonlat[0]
    densified_lonlat[-1] = points_lonlat[-1]
    return densified_lonlat


def bearing_degrees_xy(start_xy: tuple[float, float], end_xy: tuple[float, float]) -> float | None:
    """Return clockwise bearing in degrees where 0 is north."""
    dx = end_xy[0] - start_xy[0]
    dy = end_xy[1] - start_xy[1]
    if dx == 0 and dy == 0:
        return None

    angle_rad = math.atan2(dx, dy)
    return (math.degrees(angle_rad) + 360.0) % 360.0


def fold_undirected_bearing(bearing: float) -> float:
    """Fold directional bearing into [0, 180)."""
    folded = bearing % 180.0
    if folded < 0:
        folded += 180.0
    return folded
