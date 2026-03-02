"""H3 indexing helpers with compatibility wrappers."""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

import h3


def latlng_to_cell(lat: float, lng: float, resolution: int) -> str:
    """Convert lat/lng to H3 cell index."""
    if hasattr(h3, "latlng_to_cell"):
        return str(h3.latlng_to_cell(lat, lng, resolution))
    return str(h3.geo_to_h3(lat, lng, resolution))


def are_neighbor_cells(a: str, b: str) -> bool:
    """Return whether two H3 cells are neighbors."""
    if hasattr(h3, "are_neighbor_cells"):
        return bool(h3.are_neighbor_cells(a, b))
    return bool(h3.h3_indexes_are_neighbors(a, b))


def grid_path_cells(start: str, end: str) -> list[str]:
    """Return deterministic grid path from start to end inclusive."""
    if hasattr(h3, "grid_path_cells"):
        return [str(cell) for cell in h3.grid_path_cells(start, end)]
    return [str(cell) for cell in h3.h3_line(start, end)]


def cell_to_latlng(cell: str) -> tuple[float, float]:
    """Return cell center lat/lng."""
    if hasattr(h3, "cell_to_latlng"):
        lat, lng = h3.cell_to_latlng(cell)
        return float(lat), float(lng)
    lat, lng = h3.h3_to_geo(cell)
    return float(lat), float(lng)


def _cells_to_directed_edge(origin: str, destination: str) -> str:
    if hasattr(h3, "cells_to_directed_edge"):
        return str(h3.cells_to_directed_edge(origin, destination))
    return str(h3.get_h3_unidirectional_edge(origin, destination))


def _directed_edge_to_boundary(edge: str) -> list[tuple[float, float]]:
    if hasattr(h3, "directed_edge_to_boundary"):
        raw_boundary = h3.directed_edge_to_boundary(edge)
    else:
        raw_boundary = h3.get_h3_unidirectional_edge_boundary(edge)

    coords = cast(Sequence[Sequence[float]], raw_boundary)
    return [(float(coord[0]), float(coord[1])) for coord in coords]


def directed_edge_boundary(origin: str, destination: str) -> list[tuple[float, float]]:
    """Return boundary vertices (lat, lng) for directed edge origin->destination."""
    edge = _cells_to_directed_edge(origin, destination)
    coords = _directed_edge_to_boundary(edge)
    return [(float(lat), float(lng)) for lat, lng in coords]
