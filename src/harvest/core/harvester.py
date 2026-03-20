"""Harvester protocol — the contract every harvester implements."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Iterator


@runtime_checkable
class Harvester(Protocol):
    """Protocol for content harvesters.

    Each harvester knows how to:
    - Register itself as a source in NC
    - Fetch raw records from its data source
    - Transform raw records into Minoo envelope payloads
    """

    name: str
    source_type: str  # "structured" or "api"

    def source_registration(self) -> dict[str, Any]:
        """Return fields for NC source-manager registration."""
        ...

    def fetch(self) -> Iterator[dict[str, Any]]:
        """Yield raw records from the data source."""
        ...

    def transform(self, raw: dict[str, Any]) -> list[dict[str, Any]]:
        """Transform a raw record into envelope payload dicts.

        Returns a list because one raw record may produce multiple
        entities (e.g., a dictionary entry + example sentences).
        """
        ...
