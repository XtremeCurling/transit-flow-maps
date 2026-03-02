from __future__ import annotations

from pyproj import CRS

from transit_flow_maps.conflation.densify import densify_linestring_lonlat
from transit_flow_maps.util.crs import build_crs_context


def test_densify_preserves_endpoints_and_spacing() -> None:
    crs_ctx = build_crs_context(CRS.from_epsg(26910))
    points = [(-122.431, 37.774), (-122.421, 37.774)]

    densified = densify_linestring_lonlat(
        points,
        spacing_m=100.0,
        to_metric=crs_ctx.to_metric,
        to_wgs84=crs_ctx.to_wgs84,
    )

    assert densified[0] == points[0]
    assert densified[-1] == points[-1]
    assert len(densified) > 2

    # Ensure each produced step is <= spacing target in metric CRS.
    metric_points = [crs_ctx.to_metric.transform(lon, lat) for lon, lat in densified]
    for i in range(len(metric_points) - 1):
        dx = metric_points[i + 1][0] - metric_points[i][0]
        dy = metric_points[i + 1][1] - metric_points[i][1]
        assert (dx * dx + dy * dy) ** 0.5 <= 100.0 + 1e-6
