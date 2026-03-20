"""Orchestration pipeline: register → fetch → transform → build → deliver."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from harvest.core.envelope import EnvelopeBuilder
from harvest.core.license_tracker import LicenseTracker

if TYPE_CHECKING:
    from harvest.core.harvester import Harvester
    from harvest.core.nc_client import NCClient
    from harvest.core.nc_publisher import NCPublisher

logger = logging.getLogger(__name__)


@dataclass
class RunResult:
    """Summary of a harvester run."""

    fetched: int = 0
    envelopes_built: int = 0
    delivered: int = 0
    accepted: int = 0
    rejected: int = 0
    errors: list[str] = field(default_factory=list)

    def summary(self) -> str:
        return (
            f"{self.fetched} fetched, {self.envelopes_built} envelopes built, "
            f"{self.delivered} delivered, {self.accepted} accepted, "
            f"{self.rejected} rejected"
        )


class Runner:
    """Runs a harvester through the full pipeline."""

    def __init__(self, nc_client: NCClient, nc_publisher: NCPublisher, dry_run: bool = False) -> None:
        self._nc = nc_client
        self._publisher = nc_publisher
        self._dry_run = dry_run
        self._license_tracker = LicenseTracker()

    def run(self, harvester: Harvester) -> RunResult:
        result = RunResult()

        # 1. Validate license
        reg = harvester.source_registration()
        license_errors = self._license_tracker.validate_source(reg)
        if license_errors:
            result.errors.extend(license_errors)
            logger.error("License validation failed for %s: %s", harvester.name, license_errors)
            return result

        # 2. Register source in NC
        if not self._dry_run:
            self._nc.register_source(reg)

        # 3. Build envelope builder with source metadata
        metadata: dict[str, Any] = {}
        if "license_type" in reg:
            metadata["license_type"] = reg["license_type"]
        if "attribution_text" in reg:
            metadata["attribution_text"] = reg["attribution_text"]

        # 4. Fetch and transform
        envelopes: list[dict[str, Any]] = []
        for raw in harvester.fetch():
            result.fetched += 1
            try:
                payloads = harvester.transform(raw)
            except Exception as exc:
                result.errors.append(f"transform error on record {result.fetched}: {exc}")
                logger.warning("Transform failed for record %d: %s", result.fetched, exc)
                continue
            for payload in payloads:
                payload = dict(payload)  # shallow copy — don't mutate harvester data
                entity_type = payload.pop("_entity_type", "unknown")
                source_url = payload.pop("_source_url", reg.get("url", ""))
                builder = EnvelopeBuilder(
                    source=harvester.name,
                    entity_type=entity_type,
                    metadata=metadata,
                )
                envelope = builder.build(data=payload, source_url=source_url)
                envelopes.append(envelope)
                result.envelopes_built += 1

        # 5. Deliver
        if self._dry_run:
            logger.info("[DRY RUN] Would deliver %d envelopes", len(envelopes))
            return result

        if envelopes:
            delivery = self._publisher.deliver(envelopes)
            result.delivered = len(envelopes)
            result.accepted = delivery.get("accepted", 0)
            result.rejected = delivery.get("rejected", 0)

        return result
