"""Validation report generation and quality gates."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import SupportsFloat, cast

import pandas as pd

from transit_flow_maps.util.config import RuntimeConfig, ensure_output_directories
from transit_flow_maps.util.logging import get_logger


@dataclass(frozen=True)
class ValidationArtifacts:
    """Output paths and gate result produced by validate."""

    summary_path: Path
    top_segments_path: Path
    route_throughput_path: Path
    flow_only_pct: float
    max_unmatched_flow_pct: float
    passed: bool


def _safe_float(value: object) -> float:
    if value is None:
        return 0.0
    try:
        return float(cast(SupportsFloat | str | bytes | bytearray, value))
    except (TypeError, ValueError):
        return 0.0


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


def _read_optional_csv(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)
    df = pd.read_csv(path)
    for column in columns:
        if column not in df.columns:
            df[column] = ""
    return df


def run_validation(runtime_config: RuntimeConfig) -> ValidationArtifacts:
    """Build validation reports and evaluate quality gates."""
    logger = get_logger(__name__)
    ensure_output_directories(runtime_config.paths)

    segment_keys_path = runtime_config.paths.interim_dir / "segment_keys.parquet"
    segment_flows_path = runtime_config.paths.interim_dir / "segment_flows.parquet"
    if not segment_keys_path.exists():
        raise FileNotFoundError(f"Missing segment keys parquet: {segment_keys_path}")
    if not segment_flows_path.exists():
        raise FileNotFoundError(f"Missing segment flows parquet: {segment_flows_path}")

    segment_keys = pd.read_parquet(segment_keys_path, columns=["segment_id"])
    segment_flows = pd.read_parquet(
        segment_flows_path,
        columns=["segment_id", "daily_riders", "agency", "routes_json"],
    )

    segment_keys["segment_id"] = segment_keys["segment_id"].astype(str)
    segment_flows["segment_id"] = segment_flows["segment_id"].astype(str)
    segment_flows["agency"] = segment_flows["agency"].astype(str)

    key_ids = set(segment_keys["segment_id"].tolist())
    flow_ids = set(segment_flows["segment_id"].tolist())
    matched_ids = key_ids & flow_ids
    key_only_ids = key_ids - flow_ids
    flow_only_ids = flow_ids - key_ids

    key_count = len(key_ids)
    flow_count = len(flow_ids)
    matched_count = len(matched_ids)
    key_only_count = len(key_only_ids)
    flow_only_count = len(flow_only_ids)

    key_match_rate_pct = (100.0 * matched_count / key_count) if key_count > 0 else 100.0
    flow_match_rate_pct = (100.0 * matched_count / flow_count) if flow_count > 0 else 100.0
    flow_only_pct = (100.0 * flow_only_count / flow_count) if flow_count > 0 else 0.0

    max_unmatched_flow_pct = float(runtime_config.settings.validate_max_unmatched_flow_pct)
    passed = flow_only_pct <= max_unmatched_flow_pct

    excluded_path = runtime_config.paths.debug_dir / "excluded_route_directions.csv"
    unsnapped_path = runtime_config.paths.debug_dir / "unsnapped_or_far_snaps.csv"
    excluded_df = _read_optional_csv(
        excluded_path,
        ["route_id", "direction_label", "direction_id", "reason"],
    )
    unsnapped_df = _read_optional_csv(
        unsnapped_path,
        ["route_id", "direction", "source_stop_id", "source_stop_name", "reason", "distance_m"],
    )

    excluded_counts = (
        excluded_df["reason"].astype(str).value_counts().sort_index()
        if not excluded_df.empty
        else pd.Series(dtype="int64")
    )
    unsnapped_counts = (
        unsnapped_df["reason"].astype(str).value_counts().sort_index()
        if not unsnapped_df.empty
        else pd.Series(dtype="int64")
    )

    top_segments_path = runtime_config.paths.debug_dir / "validation_top_segments.csv"
    top_segments = (
        segment_flows.groupby(["agency", "segment_id"], as_index=False)
        .agg(daily_riders=("daily_riders", "sum"))
        .sort_values(["agency", "daily_riders", "segment_id"], ascending=[True, False, True])
    )
    if top_segments.empty:
        top_segments = pd.DataFrame(columns=["agency", "rank", "segment_id", "daily_riders"])
    else:
        top_segments["rank"] = top_segments.groupby("agency").cumcount() + 1
        top_segments = top_segments[top_segments["rank"] <= 20]
        top_segments = top_segments[["agency", "rank", "segment_id", "daily_riders"]]
    top_segments.to_csv(top_segments_path, index=False)

    route_throughput_path = runtime_config.paths.debug_dir / "validation_route_throughput.csv"
    route_df = segment_flows.copy()
    route_df["routes"] = route_df["routes_json"].map(_parse_json_list)
    route_df["routes"] = route_df["routes"].map(
        lambda routes: routes if len(routes) > 0 else ["unknown_route"]
    )
    route_df["route_count"] = route_df["routes"].map(len)
    route_df = route_df.explode("routes")
    route_df["route_id"] = route_df["routes"].astype(str)
    route_df["route_share"] = route_df["daily_riders"].map(_safe_float) / route_df[
        "route_count"
    ].clip(lower=1)

    route_summary = (
        route_df.groupby(["agency", "route_id"], as_index=False)
        .agg(approx_daily_riders=("route_share", "sum"), segment_rows=("route_share", "size"))
        .sort_values(["agency", "approx_daily_riders", "route_id"], ascending=[True, False, True])
    )
    route_summary.to_csv(route_throughput_path, index=False)

    summary_rows: list[dict[str, object]] = [
        {"metric": "segment_key_count", "value": key_count},
        {"metric": "segment_flow_count", "value": flow_count},
        {"metric": "segment_match_count", "value": matched_count},
        {"metric": "segment_key_only_count", "value": key_only_count},
        {"metric": "segment_flow_only_count", "value": flow_only_count},
        {"metric": "key_match_rate_pct", "value": round(key_match_rate_pct, 6)},
        {"metric": "flow_match_rate_pct", "value": round(flow_match_rate_pct, 6)},
        {"metric": "flow_only_pct", "value": round(flow_only_pct, 6)},
        {
            "metric": "validate_max_unmatched_flow_pct",
            "value": round(max_unmatched_flow_pct, 6),
        },
        {"metric": "gate_passed", "value": int(passed)},
    ]

    for reason, count in excluded_counts.items():
        summary_rows.append(
            {
                "metric": f"muni_excluded_route_direction_reason_{reason}",
                "value": int(count),
            }
        )

    for reason, count in unsnapped_counts.items():
        summary_rows.append(
            {
                "metric": f"muni_snap_drop_reason_{reason}",
                "value": int(count),
            }
        )

    summary_path = runtime_config.paths.debug_dir / "validation_summary.csv"
    pd.DataFrame(summary_rows).to_csv(summary_path, index=False)

    logger.info(
        "Validation gate %s flow_only_pct=%.4f threshold=%.4f",
        "passed" if passed else "failed",
        flow_only_pct,
        max_unmatched_flow_pct,
    )

    return ValidationArtifacts(
        summary_path=summary_path,
        top_segments_path=top_segments_path,
        route_throughput_path=route_throughput_path,
        flow_only_pct=flow_only_pct,
        max_unmatched_flow_pct=max_unmatched_flow_pct,
        passed=passed,
    )
