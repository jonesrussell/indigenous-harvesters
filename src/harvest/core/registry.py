"""Harvester plugin registry."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from harvest.core.harvester import Harvester


class HarvesterRegistry:
    """Stores and retrieves harvesters by name."""

    def __init__(self) -> None:
        self._harvesters: dict[str, Harvester] = {}

    def register(self, harvester: Harvester) -> None:
        self._harvesters[harvester.name] = harvester

    def get(self, name: str) -> Harvester:
        if name not in self._harvesters:
            raise KeyError(f"No harvester registered with name: {name}")
        return self._harvesters[name]

    def list(self) -> list[str]:
        return sorted(self._harvesters.keys())
