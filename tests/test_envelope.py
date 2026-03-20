"""Test Minoo envelope builder."""
from harvest.core.envelope import EnvelopeBuilder


def test_build_minimal_envelope() -> None:
    builder = EnvelopeBuilder(source="opd", entity_type="dictionary_entry")
    envelope = builder.build(
        data={"word": "makwa", "definition": "bear"},
        source_url="https://ojibwe.lib.umn.edu/main-entry/makwa-na",
    )

    assert envelope["version"] == "1.0"
    assert envelope["source"] == "opd"
    assert envelope["entity_type"] == "dictionary_entry"
    assert envelope["source_url"] == "https://ojibwe.lib.umn.edu/main-entry/makwa-na"
    assert envelope["data"] == {"word": "makwa", "definition": "bear"}
    assert envelope["snapshot_type"] == "full"
    # payload_id and timestamp are auto-generated
    assert "payload_id" in envelope
    assert "timestamp" in envelope


def test_build_with_metadata() -> None:
    builder = EnvelopeBuilder(
        source="opd",
        entity_type="dictionary_entry",
        metadata={
            "taxonomy_category": "language",
            "dialect_code": "oji-east",
            "license_type": "cc-by",
            "attribution_text": "OPD, University of Minnesota",
        },
    )
    envelope = builder.build(
        data={"word": "makwa"},
        source_url="https://example.com",
    )
    assert envelope["metadata"]["taxonomy_category"] == "language"
    assert envelope["metadata"]["license_type"] == "cc-by"


def test_payload_id_is_deterministic_for_same_input() -> None:
    builder = EnvelopeBuilder(source="opd", entity_type="dictionary_entry")
    env1 = builder.build(data={"word": "makwa"}, source_url="https://example.com", payload_id="opd-makwa")
    env2 = builder.build(data={"word": "makwa"}, source_url="https://example.com", payload_id="opd-makwa")
    assert env1["payload_id"] == env2["payload_id"] == "opd-makwa"


def test_auto_generated_payload_id_contains_source() -> None:
    builder = EnvelopeBuilder(source="opd", entity_type="dictionary_entry")
    envelope = builder.build(data={"word": "test"}, source_url="https://example.com")
    assert envelope["payload_id"].startswith("opd-")


def test_snapshot_type_override() -> None:
    builder = EnvelopeBuilder(source="opd", entity_type="dictionary_entry")
    envelope = builder.build(
        data={"word": "test"},
        source_url="https://example.com",
        snapshot_type="partial",
    )
    assert envelope["snapshot_type"] == "partial"
