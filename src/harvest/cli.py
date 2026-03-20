"""CLI entry point for indigenous-harvesters."""
from __future__ import annotations

import logging
import sys

import click

from harvest.core.nc_client import NCClient
from harvest.core.nc_publisher import NCPublisher
from harvest.core.registry import HarvesterRegistry
from harvest.core.runner import Runner

# Global registry — harvesters register themselves on import
registry = HarvesterRegistry()

# Import harvesters to trigger registration
# (added as harvesters are implemented in M1.2+)


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
def main(verbose: bool) -> None:
    """Indigenous content harvesting tools."""
    _setup_logging(verbose)


@main.command()
def list() -> None:
    """List available harvesters."""
    names = registry.list()
    if not names:
        click.echo("Available harvesters: (none registered — add harvesters in M1.2+)")
        return
    click.echo("Available harvesters:")
    for name in names:
        click.echo(f"  - {name}")


@main.command()
@click.argument("name")
@click.option("--dry-run", is_flag=True, help="Preview without delivering to NC")
@click.option("--nc-url", envvar="NC_URL", default="http://localhost:8050", help="NC base URL")
@click.option("--nc-token", envvar="NC_JWT_TOKEN", default="", help="NC JWT token")
def run(name: str, dry_run: bool, nc_url: str, nc_token: str) -> None:
    """Run a harvester by name."""
    try:
        harvester = registry.get(name)
    except KeyError:
        click.echo(f"Error: No harvester found with name '{name}'", err=True)
        sys.exit(1)

    with NCClient(base_url=nc_url, jwt_token=nc_token) as nc_client, \
         NCPublisher(base_url=nc_url, jwt_token=nc_token) as nc_publisher:
        runner = Runner(nc_client=nc_client, nc_publisher=nc_publisher, dry_run=dry_run)
        result = runner.run(harvester)
        click.echo(result.summary())
        if result.errors:
            for error in result.errors:
                click.echo(f"  ERROR: {error}", err=True)
            sys.exit(1)
