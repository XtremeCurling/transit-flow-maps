"""BART OD matrix to segment throughput conversion."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile

import networkx as nx
import pandas as pd

from transit_flow_maps.conflation.segment_keys import (
    SegmentKeyConfig,
    build_segment_keys_for_shape,
)
from transit_flow_maps.gtfs.load_gtfs import read_gtfs_table
from transit_flow_maps.util.config import RuntimeConfig, ensure_output_directories
from transit_flow_maps.util.logging import get_logger

_SHEET_NAMES = {
    "weekday_average": "Avg Weekday OD",
    "saturday_average": "Avg Saturday OD",
    "sunday_average": "Avg Sunday OD",
    "total_trips": "Total Trips OD",
}

# Station code mapping from BART OD workbook abbreviations to GTFS parent station IDs.
_BART_CODE_TO_PARENT: dict[str, str] = {
    "RM": "RICH",
    "EN": "DELN",
    "EP": "PLZA",
    "NB": "NBRK",
    "BK": "DBRK",
    "AS": "ASHB",
    "MA": "MCAR",
    "19": "19TH",
    "12": "12TH",
    "LM": "LAKE",
    "FV": "FTVL",
    "CL": "COLS",
    "SL": "SANL",
    "BF": "BAYF",
    "HY": "HAYW",
    "SH": "SHAY",
    "UC": "UCTY",
    "FM": "FRMT",
    "CN": "CONC",
    "PH": "PHIL",
    "WC": "WCRK",
    "LF": "LAFY",
    "OR": "ORIN",
    "RR": "ROCK",
    "OW": "WOAK",
    "EM": "EMBR",
    "MT": "MONT",
    "PL": "POWL",
    "CC": "CIVC",
    "16": "16TH",
    "24": "24TH",
    "GP": "GLEN",
    "BP": "BALB",
    "DC": "DALY",
    "CM": "COLM",
    "CV": "CAST",
    "ED": "DUBL",
    "NC": "NCON",
    "WP": "PITT",
    "SS": "SSAN",
    "SB": "SBRN",
    "SO": "SFIA",
    "MB": "MLBR",
    "WD": "WDUB",
    "OA": "OAKL",
    "WS": "WARM",
    "ML": "MLPT",
    "BE": "BERY",
    "PC": "PCTR",
    "AN": "ANTC",
}


@dataclass(frozen=True)
class BartFlowArtifacts:
    """Output paths produced by build-flows-bart."""

    segment_flows_path: Path
    conservation_path: Path
    dropped_edges_path: Path
    dropped_od_path: Path
    rows_written: int


@dataclass(frozen=True)
class ODEntity:
    """One OD matrix cell value."""

    origin_code: str
    destination_code: str
    demand: float


@dataclass(frozen=True)
class StationGraphContext:
    """GTFS-derived BART station graph and metadata."""

    graph: nx.Graph
    parent_station_ids: set[str]
    parent_coords: dict[str, tuple[float, float]]
    edge_routes: dict[tuple[str, str], set[str]]
    edge_shapes: dict[tuple[str, str], list[tuple[float, float]]]


def _undirected_station_pair(a: str, b: str) -> tuple[str, str]:
    if a <= b:
        return a, b
    return b, a


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


def _cell_ref_parts(reference: str) -> tuple[str, int]:
    match = re.match(r"^([A-Z]+)([0-9]+)$", reference)
    if match is None:
        raise ValueError(f"Invalid XLSX cell reference: {reference}")
    return match.group(1), int(match.group(2))


def _col_to_index(col: str) -> int:
    value = 0
    for ch in col:
        value = value * 26 + (ord(ch) - ord("A") + 1)
    return value


def _safe_float(value: str) -> float:
    text = value.strip()
    if text == "":
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _shared_strings(zip_file: ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in zip_file.namelist():
        return []

    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    root = ET.fromstring(zip_file.read("xl/sharedStrings.xml"))
    out: list[str] = []
    for si in root.findall("x:si", ns):
        value = "".join((node.text or "") for node in si.findall(".//x:t", ns))
        out.append(value)
    return out


def _sheet_path_by_name(zip_file: ZipFile, sheet_name: str) -> str:
    ns_main = {
        "x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
        "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    }
    workbook = ET.fromstring(zip_file.read("xl/workbook.xml"))
    sheets = workbook.find("x:sheets", ns_main)
    if sheets is None:
        raise ValueError("XLSX workbook missing sheets metadata")

    rel_id: str | None = None
    for sheet in sheets.findall("x:sheet", ns_main):
        if sheet.attrib.get("name") == sheet_name:
            rel_id = sheet.attrib.get(
                "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
            )
            break

    if rel_id is None:
        raise ValueError(f"Sheet not found in XLSX: {sheet_name}")

    rels = ET.fromstring(zip_file.read("xl/_rels/workbook.xml.rels"))
    target: str | None = None
    for rel in rels:
        if rel.attrib.get("Id") == rel_id:
            target = rel.attrib.get("Target")
            break

    if target is None:
        raise ValueError(f"Workbook relationship not found for sheet: {sheet_name}")

    return f"xl/{target}" if not target.startswith("xl/") else target


def _sheet_cells_by_row(zip_file: ZipFile, sheet_path: str) -> dict[int, dict[str, str]]:
    ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    sheet_root = ET.fromstring(zip_file.read(sheet_path))
    strings = _shared_strings(zip_file)

    rows: dict[int, dict[str, str]] = {}
    for cell in sheet_root.findall(".//x:sheetData/x:row/x:c", ns):
        ref = cell.attrib.get("r", "")
        if ref == "":
            continue

        col, row_idx = _cell_ref_parts(ref)
        cell_type = cell.attrib.get("t", "")

        value = ""
        if cell_type == "s":
            v = cell.find("x:v", ns)
            if v is not None and v.text is not None:
                idx = int(v.text)
                value = strings[idx] if 0 <= idx < len(strings) else v.text
        elif cell_type == "inlineStr":
            text_nodes = cell.findall("x:is/x:t", ns)
            value = "".join((node.text or "") for node in text_nodes)
        else:
            v = cell.find("x:v", ns)
            if v is not None and v.text is not None:
                value = v.text

        rows.setdefault(row_idx, {})[col] = value

    return rows


def _extract_od_entries_from_cells(cells_by_row: dict[int, dict[str, str]]) -> list[ODEntity]:
    header_row = cells_by_row.get(2, {})
    if not header_row:
        raise ValueError("OD matrix sheet missing header row 2")

    ordered_cols = sorted(header_row.keys(), key=_col_to_index)
    destination_cols: list[tuple[str, str]] = []
    for col in ordered_cols:
        if col == "A":
            continue
        code = header_row[col].strip()
        if code == "" or code.lower() == "exits":
            break
        destination_cols.append((col, code))

    if not destination_cols:
        raise ValueError("OD matrix sheet has no destination station columns")

    od_rows: list[ODEntity] = []
    for row_idx in sorted(idx for idx in cells_by_row if idx >= 3):
        row_cells = cells_by_row[row_idx]
        origin_code = row_cells.get("A", "").strip()
        if origin_code == "":
            continue
        if origin_code.lower() == "entries":
            break

        for col, destination_code in destination_cols:
            demand = _safe_float(row_cells.get(col, ""))
            if demand <= 0.0:
                continue
            od_rows.append(
                ODEntity(
                    origin_code=origin_code,
                    destination_code=destination_code,
                    demand=demand,
                )
            )

    return od_rows


def _parse_od_xlsx(path: Path, *, time_basis: str) -> list[ODEntity]:
    sheet_name = _SHEET_NAMES.get(time_basis)
    if sheet_name is None:
        raise ValueError(f"Unsupported time basis: {time_basis}")

    with ZipFile(path) as zip_file:
        sheet_path = _sheet_path_by_name(zip_file, sheet_name)
        cells_by_row = _sheet_cells_by_row(zip_file, sheet_path)

    return _extract_od_entries_from_cells(cells_by_row)


def _load_shapes_by_id(shapes: pd.DataFrame) -> dict[str, list[tuple[float, float]]]:
    required = {"shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"}
    missing = required - set(shapes.columns)
    if missing:
        return {}

    normalized = shapes.copy()
    normalized["shape_pt_lat"] = pd.to_numeric(normalized["shape_pt_lat"], errors="coerce")
    normalized["shape_pt_lon"] = pd.to_numeric(normalized["shape_pt_lon"], errors="coerce")
    normalized["shape_pt_sequence"] = pd.to_numeric(
        normalized["shape_pt_sequence"],
        errors="coerce",
    )
    normalized = normalized.dropna(
        subset=["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"]
    )
    normalized = normalized.sort_values(
        ["shape_id", "shape_pt_sequence", "shape_pt_lat", "shape_pt_lon"]
    )

    shapes_by_id: dict[str, list[tuple[float, float]]] = {}
    for shape_id, group in normalized.groupby("shape_id", sort=True):
        points = [
            (float(row["shape_pt_lon"]), float(row["shape_pt_lat"]))
            for _, row in group.iterrows()
        ]
        if len(points) >= 2:
            shapes_by_id[str(shape_id)] = points

    return shapes_by_id


def _nearest_shape_index(
    points_lonlat: list[tuple[float, float]],
    target_lonlat: tuple[float, float],
) -> int:
    best_idx = 0
    best_dist = float("inf")
    target_lon, target_lat = target_lonlat
    for idx, (lon, lat) in enumerate(points_lonlat):
        d_lon = lon - target_lon
        d_lat = lat - target_lat
        sq_dist = d_lon * d_lon + d_lat * d_lat
        if sq_dist < best_dist:
            best_dist = sq_dist
            best_idx = idx
    return best_idx


def _shape_subpath(
    points_lonlat: list[tuple[float, float]],
    start_idx: int,
    end_idx: int,
) -> list[tuple[float, float]]:
    if start_idx <= end_idx:
        subpath = points_lonlat[start_idx : end_idx + 1]
    else:
        subpath = list(reversed(points_lonlat[end_idx : start_idx + 1]))
    return subpath


def _build_station_graph(
    stops: pd.DataFrame,
    trips: pd.DataFrame,
    stop_times: pd.DataFrame,
    routes: pd.DataFrame,
    shapes: pd.DataFrame,
) -> StationGraphContext:
    rail_routes = set(
        routes[routes["route_type"].astype(str) == "1"]["route_id"].astype(str)
    )
    trips_rail = trips[trips["route_id"].astype(str).isin(rail_routes)].copy()
    trip_ids_rail = set(trips_rail["trip_id"].astype(str))
    stop_times_rail = stop_times[
        stop_times["trip_id"].astype(str).isin(trip_ids_rail)
    ]

    parent_rows = stops[stops["location_type"].fillna("").astype(str) == "1"].copy()
    parent_station_ids = set(parent_rows["stop_id"].astype(str))

    parent_coords = {
        str(row["stop_id"]): (float(row["stop_lon"]), float(row["stop_lat"]))
        for _, row in parent_rows.iterrows()
    }

    child_to_parent: dict[str, str] = {}
    for _, row in stops.iterrows():
        stop_id = str(row["stop_id"])
        parent = str(row.get("parent_station", "")).strip()
        if parent:
            child_to_parent[stop_id] = parent
        elif stop_id in parent_station_ids:
            child_to_parent[stop_id] = stop_id

    route_short_lookup = {
        str(row["route_id"]): str(row.get("route_short_name", row["route_id"]))
        for _, row in routes.iterrows()
    }
    trip_route_lookup = {
        str(row["trip_id"]): str(row["route_id"])
        for _, row in trips_rail.iterrows()
    }
    trip_shape_lookup = {
        str(row["trip_id"]): str(row["shape_id"])
        for _, row in trips_rail.iterrows()
        if str(row.get("shape_id", "")).strip() != ""
    }
    shapes_by_id = _load_shapes_by_id(shapes)

    graph = nx.Graph()
    edge_routes: dict[tuple[str, str], set[str]] = defaultdict(set)
    edge_shapes: dict[tuple[str, str], list[tuple[float, float]]] = {}

    stop_times_work = stop_times_rail.copy()
    stop_times_work["stop_sequence"] = pd.to_numeric(
        stop_times_work["stop_sequence"],
        errors="coerce",
    )
    stop_times_work = stop_times_work.dropna(subset=["stop_sequence", "stop_id", "trip_id"])

    for trip_id, trip_stops in stop_times_work.groupby("trip_id", sort=True):
        route_id = trip_route_lookup.get(str(trip_id))
        if route_id is None:
            continue
        route_short = route_short_lookup.get(route_id, route_id)
        shape_points = shapes_by_id.get(trip_shape_lookup.get(str(trip_id), ""))
        if shape_points is not None and len(shape_points) < 2:
            shape_points = None

        ordered = trip_stops.sort_values("stop_sequence")
        station_seq: list[str] = []
        station_shape_idx: list[int | None] = []
        for _, row in ordered.iterrows():
            stop_id = str(row["stop_id"])
            station_id = child_to_parent.get(stop_id)
            if station_id is None or station_id not in parent_coords:
                continue
            if not station_seq or station_seq[-1] != station_id:
                station_seq.append(station_id)
                if shape_points is not None:
                    nearest_idx = _nearest_shape_index(shape_points, parent_coords[station_id])
                    station_shape_idx.append(nearest_idx)
                else:
                    station_shape_idx.append(None)

        for u, v in pairwise(station_seq):
            if u == v:
                continue
            graph.add_edge(u, v)
            edge_key = _undirected_station_pair(u, v)
            edge_routes[edge_key].add(route_short)

        if shape_points is None:
            continue

        for idx in range(len(station_seq) - 1):
            u = station_seq[idx]
            v = station_seq[idx + 1]
            start_idx = station_shape_idx[idx]
            end_idx = station_shape_idx[idx + 1]
            if start_idx is None or end_idx is None:
                continue

            candidate = _shape_subpath(shape_points, start_idx, end_idx)
            if len(candidate) < 2:
                continue

            edge_key = _undirected_station_pair(u, v)
            current = edge_shapes.get(edge_key)
            if current is None:
                edge_shapes[edge_key] = candidate
                continue

            if len(candidate) > len(current):
                edge_shapes[edge_key] = candidate
                continue

            if len(candidate) == len(current) and tuple(candidate) < tuple(current):
                edge_shapes[edge_key] = candidate

    return StationGraphContext(
        graph=graph,
        parent_station_ids=parent_station_ids,
        parent_coords=parent_coords,
        edge_routes=dict(edge_routes),
        edge_shapes=edge_shapes,
    )


def _map_station_code(code: str) -> str | None:
    normalized = code.strip().upper()
    if normalized == "":
        return None
    if normalized in _BART_CODE_TO_PARENT:
        return _BART_CODE_TO_PARENT[normalized]
    return normalized


def _lexicographic_shortest_path(
    graph: nx.Graph,
    source: str,
    target: str,
) -> list[str] | None:
    if source == target:
        return [source]

    best: tuple[str, ...] | None = None
    try:
        all_paths = nx.all_shortest_paths(graph, source=source, target=target)
        for path in all_paths:
            as_tuple = tuple(str(node) for node in path)
            if best is None or as_tuple < best:
                best = as_tuple
    except nx.NetworkXNoPath:
        return None
    except nx.NodeNotFound:
        return None

    if best is None:
        return None
    return list(best)


def build_bart_flows(runtime_config: RuntimeConfig, input_file: Path) -> BartFlowArtifacts:
    """Build BART segment throughput from OD matrix input."""
    logger = get_logger(__name__)
    ensure_output_directories(runtime_config.paths)

    if not input_file.exists():
        raise FileNotFoundError(f"BART OD file missing: {input_file}")

    bart_zip = runtime_config.paths.bart_gtfs_zip
    if not bart_zip.exists():
        raise FileNotFoundError(f"BART GTFS zip missing: {bart_zip}")

    logger.info("Loading BART GTFS tables")
    stops = read_gtfs_table(bart_zip, "stops.txt")
    trips = read_gtfs_table(bart_zip, "trips.txt")
    stop_times = read_gtfs_table(bart_zip, "stop_times.txt")
    routes = read_gtfs_table(bart_zip, "routes.txt")
    shapes = read_gtfs_table(bart_zip, "shapes.txt")

    logger.info("Parsing BART OD workbook: %s", input_file)
    od_rows = _parse_od_xlsx(input_file, time_basis="weekday_average")
    if not od_rows:
        raise RuntimeError("No OD rows parsed from BART workbook")

    graph_ctx = _build_station_graph(stops, trips, stop_times, routes, shapes)
    segment_config = _segment_key_config(runtime_config)

    dropped_od_rows: list[dict[str, object]] = []
    dropped_edges_by_key: dict[tuple[str, str], str] = {}

    segment_contrib: dict[str, float] = defaultdict(float)
    segment_routes: dict[str, set[str]] = defaultdict(set)

    edge_segments_cache: dict[tuple[str, str], tuple[list[str], list[dict[str, object]]]] = {}

    total_od_demand = 0.0
    mapped_od_demand = 0.0
    mapped_pair_count = 0

    def record_dropped_edge(edge_key: tuple[str, str], reason: str) -> None:
        dropped_edges_by_key.setdefault(edge_key, reason)

    for od in od_rows:
        total_od_demand += od.demand

        origin_station = _map_station_code(od.origin_code)
        destination_station = _map_station_code(od.destination_code)

        if origin_station is None or destination_station is None:
            dropped_od_rows.append(
                {
                    "origin_code": od.origin_code,
                    "destination_code": od.destination_code,
                    "demand": od.demand,
                    "reason": "unmapped_station_code",
                }
            )
            continue

        path = _lexicographic_shortest_path(
            graph_ctx.graph,
            source=origin_station,
            target=destination_station,
        )
        if path is None:
            dropped_od_rows.append(
                {
                    "origin_code": od.origin_code,
                    "destination_code": od.destination_code,
                    "demand": od.demand,
                    "reason": "no_path",
                }
            )
            continue

        mapped_od_demand += od.demand
        mapped_pair_count += 1

        for u, v in pairwise(path):
            edge_key = _undirected_station_pair(u, v)
            if edge_key not in edge_segments_cache:
                coords_u = graph_ctx.parent_coords.get(u)
                coords_v = graph_ctx.parent_coords.get(v)
                if coords_u is None or coords_v is None:
                    record_dropped_edge(edge_key, "missing_station_geometry")
                    edge_segments_cache[edge_key] = ([], [])
                else:
                    edge_shape = graph_ctx.edge_shapes.get(edge_key)
                    if edge_shape is not None and len(edge_shape) >= 2:
                        shape_points = edge_shape
                    else:
                        shape_points = [coords_u, coords_v]
                    result = build_segment_keys_for_shape(
                        shape_id=f"bart_edge:{u}:{v}",
                        shape_points_lonlat=shape_points,
                        config=segment_config,
                    )
                    segment_ids = [str(row["segment_id"]) for row in result.records]
                    dropped_logs = [
                        row for row in result.repair_logs if str(row.get("status", "")) == "dropped"
                    ]
                    edge_segments_cache[edge_key] = (segment_ids, dropped_logs)

            segment_ids, dropped_logs = edge_segments_cache[edge_key]
            if dropped_logs:
                record_dropped_edge(edge_key, "segment_repair_dropped")
                continue

            if not segment_ids:
                record_dropped_edge(edge_key, "empty_segment_key_result")
                continue

            routes_for_edge = sorted(graph_ctx.edge_routes.get(edge_key, {"BART"}))
            for segment_id in segment_ids:
                segment_contrib[segment_id] += od.demand
                segment_routes[segment_id].update(routes_for_edge)

    segment_rows: list[dict[str, object]] = []
    for segment_id in sorted(segment_contrib):
        routes_json = json.dumps(sorted(segment_routes[segment_id]))
        segment_rows.append(
            {
                "segment_id": segment_id,
                "daily_riders": float(segment_contrib[segment_id]),
                "agency": "BART",
                "mode": "subway",
                "routes_json": routes_json,
                "time_basis": "weekday_average",
            }
        )

    if not segment_rows:
        raise RuntimeError("No BART segment flow rows were generated")

    bart_segment_df = pd.DataFrame(segment_rows)

    segment_flows_path = runtime_config.paths.interim_dir / "segment_flows.parquet"
    if segment_flows_path.exists():
        existing = pd.read_parquet(segment_flows_path)
        if "agency" in existing.columns:
            existing = existing[existing["agency"].astype(str) != "BART"]
        merged = pd.concat([existing, bart_segment_df], ignore_index=True)
    else:
        merged = bart_segment_df

    merged = merged[
        ["segment_id", "daily_riders", "agency", "mode", "routes_json", "time_basis"]
    ].sort_values(["agency", "segment_id"])
    merged.to_parquet(segment_flows_path, index=False)

    dropped_edge_rows = [
        {"from_station": u, "to_station": v, "reason": dropped_edges_by_key[(u, v)]}
        for u, v in sorted(dropped_edges_by_key)
    ]
    dropped_edges_path = runtime_config.paths.debug_dir / "bart_dropped_edges.csv"
    dropped_edges_df = pd.DataFrame(dropped_edge_rows)
    if dropped_edges_df.empty:
        dropped_edges_df = pd.DataFrame(columns=["from_station", "to_station", "reason"])
    dropped_edges_df.to_csv(dropped_edges_path, index=False)

    dropped_od_path = runtime_config.paths.debug_dir / "bart_dropped_od.csv"
    dropped_od_df = pd.DataFrame(dropped_od_rows)
    if dropped_od_df.empty:
        dropped_od_df = pd.DataFrame(
            columns=["origin_code", "destination_code", "demand", "reason"]
        )
    dropped_od_df.to_csv(dropped_od_path, index=False)

    conservation_path = runtime_config.paths.debug_dir / "bart_conservation.csv"
    conservation_rows = [
        {"metric": "od_pairs_input", "value": len(od_rows)},
        {"metric": "od_pairs_mapped", "value": mapped_pair_count},
        {"metric": "od_pairs_dropped", "value": len(dropped_od_df)},
        {"metric": "od_demand_input", "value": total_od_demand},
        {"metric": "od_demand_mapped", "value": mapped_od_demand},
        {
            "metric": "od_demand_dropped",
            "value": max(0.0, total_od_demand - mapped_od_demand),
        },
        {"metric": "segment_rows_bart", "value": len(bart_segment_df)},
        {"metric": "dropped_edges", "value": len(dropped_edges_df)},
    ]
    pd.DataFrame(conservation_rows).to_csv(conservation_path, index=False)

    if len(dropped_edges_df) > 0:
        raise RuntimeError(
            "BART stage failed: dropped edges detected. See bart_dropped_edges.csv"
        )

    logger.info("BART segment flow rows written: %s", len(bart_segment_df))

    return BartFlowArtifacts(
        segment_flows_path=segment_flows_path,
        conservation_path=conservation_path,
        dropped_edges_path=dropped_edges_path,
        dropped_od_path=dropped_od_path,
        rows_written=int(len(bart_segment_df)),
    )
