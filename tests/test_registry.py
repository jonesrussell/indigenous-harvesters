"""Test harvester plugin registry."""
import pytest

from harvest.core.registry import HarvesterRegistry


class FakeHarvester:
    name = "fake"
    source_type = "structured"

    def source_registration(self) -> dict:
        return {"name": "Fake Source", "url": "https://example.com"}

    def fetch(self):
        yield {"word": "test"}

    def transform(self, raw: dict) -> list[dict]:
        return [{"word": raw["word"]}]


def test_register_and_get() -> None:
    registry = HarvesterRegistry()
    harvester = FakeHarvester()
    registry.register(harvester)
    assert registry.get("fake") is harvester


def test_get_unknown_raises() -> None:
    registry = HarvesterRegistry()
    with pytest.raises(KeyError, match="nonexistent"):
        registry.get("nonexistent")


def test_list_registered() -> None:
    registry = HarvesterRegistry()
    registry.register(FakeHarvester())
    names = registry.list()
    assert names == ["fake"]
