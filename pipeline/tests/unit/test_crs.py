from __future__ import annotations

from transit_flow_maps.util.crs import choose_metric_crs


def test_choose_metric_crs_keeps_default_when_in_zone() -> None:
    points = [(-122.45, 37.77), (-122.40, 37.80)]
    crs = choose_metric_crs(points, "EPSG:26910")
    assert crs.to_epsg() == 26910


def test_choose_metric_crs_falls_back_to_utm_when_outside_default_zone() -> None:
    points = [(-73.99, 40.73), (-73.95, 40.76)]
    crs = choose_metric_crs(points, "EPSG:26910")
    assert crs.to_epsg() == 32618
