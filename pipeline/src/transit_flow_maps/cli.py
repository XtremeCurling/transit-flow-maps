"""Typer CLI entrypoint for transit-flow-maps."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer

from transit_flow_maps.corridors.assign_to_corridors import build_corridors as build_corridors_job
from transit_flow_maps.export.geojson import export_geojson as export_geojson_job
from transit_flow_maps.flows.bart_od import build_bart_flows
from transit_flow_maps.flows.muni_stop_loads import build_muni_flows
from transit_flow_maps.gtfs.shapes import build_segments as build_segments_job
from transit_flow_maps.util.config import load_runtime_config
from transit_flow_maps.util.logging import configure_logging, get_logger

app = typer.Typer(help="Transit Flow Maps CLI")

DEFAULT_CONFIG_PATH = Path(__file__).parent / "config" / "default.yaml"

VerboseOption = Annotated[
    bool,
    typer.Option("--verbose", help="Enable debug logging."),
]
ConfigOption = Annotated[
    Path,
    typer.Option(
        "--config",
        exists=True,
        readable=True,
        dir_okay=False,
        file_okay=True,
        help="Config YAML.",
    ),
]
MuniInputOption = Annotated[
    Path,
    typer.Option(
        "--input",
        exists=True,
        readable=True,
        dir_okay=False,
        file_okay=True,
        help="Muni CSV input.",
    ),
]
BartInputOption = Annotated[
    Path,
    typer.Option(
        "--input",
        exists=True,
        readable=True,
        dir_okay=False,
        file_okay=True,
        help="BART XLSX input.",
    ),
]
IncludeBartOption = Annotated[
    bool,
    typer.Option(
        "--include-bart",
        help="Include BART in corridor aggregation.",
    ),
]
ViewOption = Annotated[
    str,
    typer.Option(
        "--view",
        case_sensitive=False,
        help="corridor or physical",
    ),
]


@app.callback()
def main(verbose: VerboseOption = False) -> None:
    """CLI callback to initialize logging."""
    configure_logging(debug=verbose)


@app.command("build-segments")
def build_segments_cmd(
    config: ConfigOption = DEFAULT_CONFIG_PATH,
) -> None:
    """Build deterministic segment keys from GTFS shapes."""
    logger = get_logger(__name__)
    runtime_config = load_runtime_config(config)
    logger.info("Running build-segments with config=%s", config.resolve())
    artifacts = build_segments_job(runtime_config)
    typer.echo(f"segment_keys.parquet: {artifacts.segment_keys_path}")
    typer.echo(f"debug overlay: {artifacts.debug_geojson_path}")
    typer.echo(f"repairs log: {artifacts.repairs_csv_path}")
    typer.echo(f"summary: {artifacts.summary_csv_path}")
    typer.echo(f"rows written: {artifacts.rows_written}")


@app.command("build-flows-muni")
def build_flows_muni(
    input_file: MuniInputOption,
    config: ConfigOption = DEFAULT_CONFIG_PATH,
) -> None:
    """Build Muni segment throughput from stop load data."""
    logger = get_logger(__name__)
    runtime_config = load_runtime_config(config)
    logger.info(
        "Running build-flows-muni with input=%s config=%s",
        input_file.resolve(),
        config.resolve(),
    )
    artifacts = build_muni_flows(runtime_config, input_file=input_file)
    typer.echo(f"segment_flows.parquet: {artifacts.segment_flows_path}")
    typer.echo(f"shape cache: {artifacts.shape_cache_path}")
    typer.echo(f"unsnapped/far snaps: {artifacts.unsnapped_path}")
    typer.echo(f"degenerate spans: {artifacts.degenerate_spans_path}")
    typer.echo(f"excluded route-directions: {artifacts.excluded_route_directions_path}")
    typer.echo(f"sanity totals: {artifacts.sanity_totals_path}")
    typer.echo(f"rows written: {artifacts.rows_written}")


@app.command("build-flows-bart")
def build_flows_bart(
    input_file: BartInputOption,
    config: ConfigOption = DEFAULT_CONFIG_PATH,
) -> None:
    """Build BART segment throughput from OD matrix."""
    logger = get_logger(__name__)
    runtime_config = load_runtime_config(config)
    logger.info(
        "Running build-flows-bart with input=%s config=%s",
        input_file.resolve(),
        config.resolve(),
    )
    artifacts = build_bart_flows(runtime_config, input_file=input_file)
    typer.echo(f"segment_flows.parquet: {artifacts.segment_flows_path}")
    typer.echo(f"conservation: {artifacts.conservation_path}")
    typer.echo(f"dropped edges: {artifacts.dropped_edges_path}")
    typer.echo(f"dropped OD rows: {artifacts.dropped_od_path}")
    typer.echo(f"rows written: {artifacts.rows_written}")


@app.command("build-corridors")
def build_corridors(
    include_bart: IncludeBartOption = False,
    config: ConfigOption = DEFAULT_CONFIG_PATH,
) -> None:
    """Build corridor-level aggregates from physical segment flows."""
    logger = get_logger(__name__)
    runtime_config = load_runtime_config(config)
    logger.info(
        "Running build-corridors with include_bart=%s config=%s",
        include_bart,
        config.resolve(),
    )
    artifacts = build_corridors_job(runtime_config, include_bart=include_bart)
    typer.echo(f"corridor_flows.parquet: {artifacts.corridor_flows_path}")
    typer.echo(f"corridor assignments: {artifacts.assignment_debug_path}")
    typer.echo(f"rows written: {artifacts.rows_written}")


@app.command("export-geojson")
def export_geojson(
    view: ViewOption,
    config: ConfigOption = DEFAULT_CONFIG_PATH,
) -> None:
    """Export web GeoJSON for the requested view."""
    normalized_view = view.lower()
    if normalized_view not in {"corridor", "physical"}:
        raise typer.BadParameter("--view must be one of: corridor, physical")

    logger = get_logger(__name__)
    runtime_config = load_runtime_config(config)
    logger.info(
        "Running export-geojson with view=%s config=%s",
        normalized_view,
        config.resolve(),
    )
    artifacts = export_geojson_job(runtime_config, view=normalized_view)
    typer.echo(f"geojson: {artifacts.output_path}")
    typer.echo(f"rows written: {artifacts.rows_written}")


@app.command("validate")
def validate(
    config: ConfigOption = DEFAULT_CONFIG_PATH,
) -> None:
    """Run validation reports and quality gates."""
    logger = get_logger(__name__)
    logger.info("validate scaffold invoked with config=%s", config)
    typer.echo("validate scaffold is ready; implementation follows in Milestone 7.")


if __name__ == "__main__":
    app()
