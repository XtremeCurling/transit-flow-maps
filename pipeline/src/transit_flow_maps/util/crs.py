"""CRS helper utilities."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from pyproj import CRS, Transformer

WGS84 = CRS.from_epsg(4326)


@dataclass(frozen=True)
class CRSContext:
    """Coordinate transform context for metric calculations."""

    metric_crs: CRS
    to_metric: Transformer
    to_wgs84: Transformer


def _lon_to_utm_zone(longitude: float) -> int:
    zone = int((longitude + 180.0) // 6.0) + 1
    return min(60, max(1, zone))


def _utm_epsg_for_point(longitude: float, latitude: float) -> int:
    zone = _lon_to_utm_zone(longitude)
    if latitude >= 0:
        return 32600 + zone
    return 32700 + zone


def _default_epsg_zone(default_crs: CRS) -> int | None:
    epsg = default_crs.to_epsg()
    if epsg is None:
        return None
    if 32601 <= epsg <= 32660:
        return epsg - 32600
    if 32701 <= epsg <= 32760:
        return epsg - 32700
    if 26901 <= epsg <= 26923:
        return epsg - 26900
    return None


def _bbox_center(points_lonlat: Iterable[tuple[float, float]]) -> tuple[float, float]:
    lons = [p[0] for p in points_lonlat]
    lats = [p[1] for p in points_lonlat]
    if not lons or not lats:
        raise ValueError("Cannot infer CRS from empty coordinate collection")
    return ((min(lons) + max(lons)) / 2.0, (min(lats) + max(lats)) / 2.0)


def choose_metric_crs(
    points_lonlat: Iterable[tuple[float, float]],
    default_crs_name: str = "EPSG:26910",
) -> CRS:
    """Choose metric CRS, using default in-zone and auto-UTM fallback otherwise."""
    center_lon, center_lat = _bbox_center(points_lonlat)
    default_crs = CRS.from_user_input(default_crs_name)
    default_zone = _default_epsg_zone(default_crs)
    inferred_zone = _lon_to_utm_zone(center_lon)

    if default_zone is not None and default_zone == inferred_zone:
        return default_crs

    return CRS.from_epsg(_utm_epsg_for_point(center_lon, center_lat))


def build_crs_context(metric_crs: CRS) -> CRSContext:
    """Build transformers between WGS84 and selected metric CRS."""
    to_metric = Transformer.from_crs(WGS84, metric_crs, always_xy=True)
    to_wgs84 = Transformer.from_crs(metric_crs, WGS84, always_xy=True)
    return CRSContext(metric_crs=metric_crs, to_metric=to_metric, to_wgs84=to_wgs84)
