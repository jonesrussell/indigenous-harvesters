"""Test North Cloud API client."""
import httpx
import respx
from harvest.core.nc_client import NCClient


NC_BASE = "http://localhost:8050"


@respx.mock
def test_register_source_creates_new() -> None:
    respx.post(f"{NC_BASE}/api/v1/sources").mock(
        return_value=httpx.Response(201, json={"id": "uuid-123", "name": "OPD"})
    )
    client = NCClient(base_url=NC_BASE, jwt_token="test-token")
    result = client.register_source({
        "name": "OPD",
        "url": "https://ojibwe.lib.umn.edu",
        "type": "structured",
    })
    assert result["id"] == "uuid-123"


@respx.mock
def test_register_source_conflict_returns_existing() -> None:
    respx.post(f"{NC_BASE}/api/v1/sources").mock(
        return_value=httpx.Response(409, json={"error": "Source name 'OPD' already exists"})
    )
    respx.get(f"{NC_BASE}/api/v1/sources/by-identity", params={"identity_key": "opd"}).mock(
        return_value=httpx.Response(200, json={"id": "existing-uuid", "name": "OPD"})
    )
    client = NCClient(base_url=NC_BASE, jwt_token="test-token")
    result = client.register_source({
        "name": "OPD",
        "url": "https://ojibwe.lib.umn.edu",
        "type": "structured",
        "identity_key": "opd",
    })
    assert result["id"] == "existing-uuid"
