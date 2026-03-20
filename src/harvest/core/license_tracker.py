"""License validation and tracking for harvested content."""
from __future__ import annotations

from typing import Any

HARVESTABLE_LICENSE_TYPES = frozenset({"open", "cc-by", "cc-by-sa", "restricted"})
RECOGNIZED_LICENSE_TYPES = HARVESTABLE_LICENSE_TYPES | {"unknown"}


class LicenseTracker:
    """Validates license and attribution metadata for sources."""

    def is_recognized(self, license_type: str) -> bool:
        """True if the license type is a known value (including 'unknown')."""
        return license_type in RECOGNIZED_LICENSE_TYPES

    def is_harvestable(self, license_type: str) -> bool:
        """True if the license type allows automated harvesting."""
        return license_type in HARVESTABLE_LICENSE_TYPES

    def validate_source(self, source_config: dict[str, Any]) -> list[str]:
        errors: list[str] = []
        license_type = source_config.get("license_type", "")

        if not self.is_recognized(license_type):
            errors.append(
                f"License type '{license_type}' is not recognized. "
                f"Must be one of: {', '.join(sorted(RECOGNIZED_LICENSE_TYPES))}"
            )
        elif not self.is_harvestable(license_type):
            errors.append(
                f"License type '{license_type}' requires manual review before harvesting"
            )

        if not source_config.get("attribution_text"):
            errors.append("attribution_text is required for all sources")

        return errors
