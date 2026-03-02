from typer.testing import CliRunner

from transit_flow_maps.cli import app

runner = CliRunner()


def test_cli_help_lists_commands() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "build-segments" in result.stdout
    assert "build-flows-muni" in result.stdout
    assert "build-flows-bart" in result.stdout
    assert "build-corridors" in result.stdout
    assert "export-geojson" in result.stdout
    assert "validate" in result.stdout
