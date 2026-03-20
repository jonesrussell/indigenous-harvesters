"""Test NC envelope delivery."""
import httpx
import respx
from harvest.core.nc_publisher import NCPublisher

NC_BASE = "http://localhost:8050"


@respx.mock
def test_deliver_envelopes() -> None:
    respx.post(f"{NC_BASE}/api/v1/ingest").mock(
        return_value=httpx.Response(200, json={"accepted": 2, "rejected": 0, "results": []})
    )
    publisher = NCPublisher(base_url=NC_BASE, jwt_token="test-token")
    result = publisher.deliver([
        {"payload_id": "test-1", "version": "1.0", "source": "opd",
         "snapshot_type": "full", "timestamp": "2026-03-20T00:00:00Z",
         "entity_type": "dictionary_entry", "source_url": "https://example.com",
         "data": {"word": "makwa"}},
        {"payload_id": "test-2", "version": "1.0", "source": "opd",
         "snapshot_type": "full", "timestamp": "2026-03-20T00:00:00Z",
         "entity_type": "dictionary_entry", "source_url": "https://example.com",
         "data": {"word": "jiimaan"}},
    ])
    assert result["accepted"] == 2


@respx.mock
def test_deliver_empty_batch_skips_request() -> None:
    publisher = NCPublisher(base_url=NC_BASE, jwt_token="test-token")
    result = publisher.deliver([])
    assert result["accepted"] == 0
    assert result["rejected"] == 0
