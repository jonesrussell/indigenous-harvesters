"""Test runner pipeline."""
from __future__ import annotations

from unittest.mock import MagicMock

from conftest import StubHarvester

from harvest.core.runner import Runner, RunResult


def test_dry_run_does_not_deliver() -> None:
    nc_client = MagicMock()
    nc_publisher = MagicMock()
    runner = Runner(nc_client=nc_client, nc_publisher=nc_publisher, dry_run=True)
    result = runner.run(StubHarvester())

    nc_publisher.deliver.assert_not_called()
    assert result.fetched == 1
    assert result.envelopes_built == 1
    assert result.delivered == 0


def test_run_delivers_envelopes() -> None:
    nc_client = MagicMock()
    nc_client.register_source.return_value = {"id": "uuid-123"}
    nc_publisher = MagicMock()
    nc_publisher.deliver.return_value = {"accepted": 1, "rejected": 0, "results": []}

    runner = Runner(nc_client=nc_client, nc_publisher=nc_publisher, dry_run=False)
    result = runner.run(StubHarvester())

    nc_client.register_source.assert_called_once()
    nc_publisher.deliver.assert_called_once()
    assert result.fetched == 1
    assert result.delivered == 1
    assert result.accepted == 1


def test_run_with_multiple_records() -> None:
    nc_client = MagicMock()
    nc_client.register_source.return_value = {"id": "uuid-123"}
    nc_publisher = MagicMock()
    nc_publisher.deliver.return_value = {"accepted": 3, "rejected": 0, "results": []}

    harvester = StubHarvester(records=[
        {"word": "makwa"},
        {"word": "jiimaan"},
        {"word": "miigwech"},
    ])
    runner = Runner(nc_client=nc_client, nc_publisher=nc_publisher, dry_run=False)
    result = runner.run(harvester)

    assert result.fetched == 3
    assert result.envelopes_built == 3


def test_run_result_summary() -> None:
    result = RunResult(fetched=10, envelopes_built=10, delivered=10, accepted=8, rejected=2, errors=[])
    summary = result.summary()
    assert "10 fetched" in summary
    assert "8 accepted" in summary
    assert "2 rejected" in summary
