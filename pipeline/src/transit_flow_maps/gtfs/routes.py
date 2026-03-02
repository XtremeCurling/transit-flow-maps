"""GTFS route utilities."""

from collections import defaultdict

import pandas as pd

_ROUTE_TYPE_TO_MODE = {
    "0": "tram",
    "1": "subway",
    "2": "rail",
    "3": "bus",
    "4": "ferry",
    "5": "cable_tram",
    "6": "aerial_lift",
    "7": "funicular",
    "11": "trolleybus",
    "12": "monorail",
    "900": "tram",
    "901": "subway",
    "902": "rail",
    "903": "bus",
}


def route_mode_lookup(routes: pd.DataFrame) -> dict[str, str]:
    """Map route_id to normalized mode strings."""
    mode_by_route: dict[str, str] = {}
    for _, row in routes.iterrows():
        route_id = str(row["route_id"])
        route_type = str(row.get("route_type", ""))
        mode_by_route[route_id] = _ROUTE_TYPE_TO_MODE.get(route_type, "unknown")
    return mode_by_route


def shape_to_routes(trips: pd.DataFrame) -> dict[str, list[str]]:
    """Map shape_id to sorted route_ids that reference the shape."""
    values: dict[str, set[str]] = defaultdict(set)
    for _, row in trips.iterrows():
        shape_id = str(row["shape_id"])
        route_id = str(row["route_id"])
        values[shape_id].add(route_id)

    return {shape_id: sorted(route_ids) for shape_id, route_ids in sorted(values.items())}
