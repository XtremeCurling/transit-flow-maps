"""GTFS loading utilities."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from zipfile import ZipFile

import pandas as pd


@dataclass(frozen=True)
class GTFSBundle:
    """Loaded GTFS tables for an agency."""

    agency: str
    zip_path: Path
    shapes: pd.DataFrame
    trips: pd.DataFrame
    routes: pd.DataFrame


def _resolve_zip_member_name(archive_names: list[str], expected_name: str) -> str:
    expected_lower = expected_name.lower()

    if expected_name in archive_names:
        return expected_name

    candidates: list[str] = []
    for member_name in archive_names:
        if member_name.endswith("/"):
            continue
        if member_name.startswith("__MACOSX/"):
            continue
        if PurePosixPath(member_name).name.lower() == expected_lower:
            candidates.append(member_name)

    if not candidates:
        sample = ", ".join(sorted(archive_names)[:10])
        raise KeyError(
            f"Missing GTFS file {expected_name!r} in archive. "
            f"First members: [{sample}]"
        )

    # Deterministic selection when duplicates exist.
    candidates.sort(key=lambda name: (name.count("/"), name.lower(), name))
    return candidates[0]


def read_gtfs_table(zip_path: Path, name: str) -> pd.DataFrame:
    """Read a GTFS table from a zip, resolving nested folder packaging."""
    with ZipFile(zip_path) as zf:
        member_name = _resolve_zip_member_name(zf.namelist(), name)
        with zf.open(member_name) as fh:
            return pd.read_csv(fh, dtype=str)


def read_optional_gtfs_table(zip_path: Path, name: str) -> pd.DataFrame | None:
    """Read an optional GTFS table if present; return None when absent."""
    with ZipFile(zip_path) as zf:
        try:
            member_name = _resolve_zip_member_name(zf.namelist(), name)
        except KeyError:
            return None
        with zf.open(member_name) as fh:
            return pd.read_csv(fh, dtype=str)


def _normalize_shapes(shapes: pd.DataFrame) -> pd.DataFrame:
    required = {"shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"}
    missing = sorted(required - set(shapes.columns))
    if missing:
        raise ValueError(f"Missing required shapes columns: {missing}")

    normalized = shapes.copy()
    normalized["shape_pt_lat"] = pd.to_numeric(normalized["shape_pt_lat"], errors="coerce")
    normalized["shape_pt_lon"] = pd.to_numeric(normalized["shape_pt_lon"], errors="coerce")
    normalized["shape_pt_sequence"] = pd.to_numeric(
        normalized["shape_pt_sequence"],
        errors="coerce",
        downcast="integer",
    )
    normalized = normalized.dropna(
        subset=["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"]
    )
    normalized["shape_pt_sequence"] = normalized["shape_pt_sequence"].astype(int)
    normalized = normalized.sort_values(
        ["shape_id", "shape_pt_sequence", "shape_pt_lat", "shape_pt_lon"]
    )
    return normalized


def _normalize_trips(trips: pd.DataFrame) -> pd.DataFrame:
    required = {"route_id", "shape_id"}
    missing = sorted(required - set(trips.columns))
    if missing:
        raise ValueError(f"Missing required trips columns: {missing}")

    normalized = trips.copy()
    normalized = normalized.dropna(subset=["route_id", "shape_id"])
    return normalized


def _normalize_routes(routes: pd.DataFrame) -> pd.DataFrame:
    required = {"route_id"}
    missing = sorted(required - set(routes.columns))
    if missing:
        raise ValueError(f"Missing required routes columns: {missing}")

    normalized = routes.copy()
    normalized = normalized.dropna(subset=["route_id"])
    return normalized


def load_gtfs_bundle(agency: str, zip_path: Path) -> GTFSBundle:
    """Load core GTFS tables needed for deterministic segment construction."""
    if not zip_path.exists():
        raise FileNotFoundError(f"GTFS zip not found for agency={agency}: {zip_path}")

    shapes = _normalize_shapes(read_gtfs_table(zip_path, "shapes.txt"))
    trips = _normalize_trips(read_gtfs_table(zip_path, "trips.txt"))
    routes = _normalize_routes(read_gtfs_table(zip_path, "routes.txt"))

    return GTFSBundle(agency=agency, zip_path=zip_path, shapes=shapes, trips=trips, routes=routes)
