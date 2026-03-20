"""HTTP client for North Cloud source-manager API."""
from __future__ import annotations

import logging
from typing import Any, cast

import httpx

logger = logging.getLogger(__name__)


class NCClientError(Exception):
    """Raised when NC API returns an unexpected error."""


class NCClient:
    """Client for North Cloud source-manager API.

    Handles source registration (create or retrieve existing).
    """

    def __init__(self, base_url: str, jwt_token: str, timeout: float = 30.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._client = httpx.Client(
            base_url=self._base_url,
            headers={"Authorization": f"Bearer {jwt_token}"},
            timeout=timeout,
        )

    def register_source(self, source_data: dict[str, Any]) -> dict[str, Any]:
        """Register or retrieve a source in NC source-manager."""
        resp = self._client.post("/api/v1/sources", json=source_data)

        if resp.status_code == 201:
            return cast("dict[str, Any]", resp.json())

        if resp.status_code == 409:
            # Already exists — look up by identity_key
            identity_key = source_data.get("identity_key", "")
            if not identity_key:
                raise NCClientError(f"Source conflict with no identity_key for: {source_data.get('name')}")
            lookup = self._client.get(
                "/api/v1/sources/by-identity",
                params={"identity_key": identity_key},
            )
            if lookup.status_code != 200:
                raise NCClientError(f"Source lookup failed ({lookup.status_code}) for identity_key: {identity_key}")
            return cast("dict[str, Any]", lookup.json())

        resp.raise_for_status()
        return cast("dict[str, Any]", resp.json())  # unreachable but satisfies type checker

    def __enter__(self) -> NCClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()
