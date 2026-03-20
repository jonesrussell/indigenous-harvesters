"""Build envelopes matching Minoo's PayloadValidator contract."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any


class EnvelopeBuilder:
    """Constructs ingestion envelopes for Minoo's pipeline.

    Required envelope fields (validated by Minoo's PayloadValidator):
        payload_id, version, source, snapshot_type, timestamp,
        entity_type, source_url, data

    Optional: metadata (taxonomy tags, license/attribution).
    """

    def __init__(
        self,
        source: str,
        entity_type: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self._source = source
        self._entity_type = entity_type
        self._metadata = metadata or {}

    def build(
        self,
        data: dict[str, Any],
        source_url: str,
        payload_id: str | None = None,
        snapshot_type: str = "full",
        timestamp: str | None = None,
    ) -> dict[str, Any]:
        envelope: dict[str, Any] = {
            "payload_id": payload_id or f"{self._source}-{uuid.uuid4().hex[:12]}",
            "version": "1.0",
            "source": self._source,
            "snapshot_type": snapshot_type,
            "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
            "entity_type": self._entity_type,
            "source_url": source_url,
            "data": data,
        }
        if self._metadata:
            envelope["metadata"] = self._metadata
        return envelope
