"""Test CLI commands."""
from click.testing import CliRunner

from harvest.cli import main


def test_list_command() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["list"])
    assert result.exit_code == 0
    assert "Available harvesters" in result.output


def test_run_unknown_harvester() -> None:
    runner = CliRunner()
    result = runner.invoke(main, ["run", "nonexistent"])
    assert result.exit_code != 0
    assert "no harvester found with name 'nonexistent'" in result.output.lower()
