"""Shared test fixtures."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator


class StubHarvester:
    """Minimal harvester for testing the runner pipeline."""

    name = "stub"
    source_type = "structured"

    def __init__(self, records: list[dict] | None = None) -> None:
        self._records = records or [{"word": "makwa", "definition": "bear"}]

    def source_registration(self) -> dict:
        return {
            "name": "Stub Source",
            "url": "https://example.com",
            "type": "structured",
            "identity_key": "stub",
            "license_type": "open",
            "attribution_text": "Test attribution",
        }

    def fetch(self) -> Iterator[dict]:
        yield from self._records

    def transform(self, raw: dict) -> list[dict]:
        return [{
            "_entity_type": "dictionary_entry",
            "_source_url": "https://example.com/entry",
            **raw,
        }]


@pytest.fixture
def stub_harvester() -> StubHarvester:
    return StubHarvester()
