"""Configuration loading for transit-flow-maps."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field


def project_root() -> Path:
    """Return repository root based on package file location."""
    return Path(__file__).resolve().parents[4]


class PathsConfig(BaseModel):
    """Filesystem path settings."""

    model_config = ConfigDict(extra="ignore")

    data_root: str = "data"
    interim_dir: str = "data/processed/interim"
    debug_dir: str = "data/processed/debug"
    web_dir: str = "data/processed/web"
    sfmta_gtfs_zip: str = "data/raw/gtfs/sfmta.zip"
    bart_gtfs_zip: str = "data/raw/gtfs/bart.zip"


class Settings(BaseModel):
    """Runtime settings from YAML."""

    model_config = ConfigDict(extra="ignore")

    h3_resolution: int = 11
    edge_pos_bins: int = 6
    bearing_bucket_count: int = 12

    densify_spacing_m: float = 20

    snap_target_m: float = 35
    snap_warn_m: float = 40
    snap_hard_cap_m: float = 75

    canonical_stop_dedupe_m: float = 10
    degenerate_span_epsilon_m: float = 3

    non_neighbor_max_path_cells: int = 20
    non_neighbor_max_recursion_depth: int = 4

    corridor_buffer_m: float = 50
    corridor_assignment_max_distance_m: float = 60
    corridor_sample_spacing_m: float = 20

    muni_scale_by_trip_count: bool = False
    validate_max_unmatched_flow_pct: float = 5.0

    crs_metric_default: str = "EPSG:26910"

    paths: PathsConfig = Field(default_factory=PathsConfig)


@dataclass(frozen=True)
class ResolvedPaths:
    """Resolved absolute paths used by pipeline commands."""

    data_root: Path
    interim_dir: Path
    debug_dir: Path
    web_dir: Path
    sfmta_gtfs_zip: Path
    bart_gtfs_zip: Path


@dataclass(frozen=True)
class RuntimeConfig:
    """Fully-loaded settings with resolved absolute paths."""

    settings: Settings
    paths: ResolvedPaths


def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config at {path} must decode to a mapping")
    return data


def _resolve_path(value: str, root: Path) -> Path:
    p = Path(value)
    if p.is_absolute():
        return p
    return (root / p).resolve()


def _resolve_paths(settings: Settings, root: Path) -> ResolvedPaths:
    data_root = _resolve_path(settings.paths.data_root, root)
    interim_dir = _resolve_path(settings.paths.interim_dir, root)
    debug_dir = _resolve_path(settings.paths.debug_dir, root)
    web_dir = _resolve_path(settings.paths.web_dir, root)
    sfmta_gtfs_zip = _resolve_path(settings.paths.sfmta_gtfs_zip, root)
    bart_gtfs_zip = _resolve_path(settings.paths.bart_gtfs_zip, root)
    return ResolvedPaths(
        data_root=data_root,
        interim_dir=interim_dir,
        debug_dir=debug_dir,
        web_dir=web_dir,
        sfmta_gtfs_zip=sfmta_gtfs_zip,
        bart_gtfs_zip=bart_gtfs_zip,
    )


def load_runtime_config(config_path: Path) -> RuntimeConfig:
    """Load YAML settings and resolve all configured paths."""
    absolute_config = config_path.resolve()
    data = _read_yaml(absolute_config)
    settings = Settings.model_validate(data)
    root = project_root()
    resolved = _resolve_paths(settings, root)
    return RuntimeConfig(settings=settings, paths=resolved)


def ensure_output_directories(paths: ResolvedPaths) -> None:
    """Create output directories if needed."""
    paths.interim_dir.mkdir(parents=True, exist_ok=True)
    paths.debug_dir.mkdir(parents=True, exist_ok=True)
    paths.web_dir.mkdir(parents=True, exist_ok=True)
