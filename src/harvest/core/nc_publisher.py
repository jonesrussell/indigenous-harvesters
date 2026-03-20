"""Delivers ingestion envelopes to North Cloud's ingest endpoint."""
from __future__ import annotations

import logging
from typing import Any, cast

import httpx

logger = logging.getLogger(__name__)


class NCPublisher:
    """Delivers batches of envelopes to NC's POST /api/v1/ingest."""

    def __init__(self, base_url: str, jwt_token: str, timeout: float = 30.0) -> None:
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {jwt_token}"},
            timeout=timeout,
        )

    def deliver(self, envelopes: list[dict[str, Any]]) -> dict[str, Any]:
        """Deliver a batch of envelopes to NC ingest endpoint."""
        if not envelopes:
            return {"accepted": 0, "rejected": 0, "results": []}

        resp = self._client.post("/api/v1/ingest", json={"envelopes": envelopes})
        resp.raise_for_status()
        return cast("dict[str, Any]", resp.json())

    def close(self) -> None:
        self._client.close()
