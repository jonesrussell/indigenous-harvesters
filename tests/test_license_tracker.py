"""Test license validation and tracking."""
from harvest.core.license_tracker import LicenseTracker


def test_harvestable_license_types() -> None:
    tracker = LicenseTracker()
    for lt in ["open", "cc-by", "cc-by-sa", "restricted"]:
        assert tracker.is_harvestable(lt)


def test_unknown_is_recognized_but_not_harvestable() -> None:
    tracker = LicenseTracker()
    assert tracker.is_recognized("unknown")
    assert not tracker.is_harvestable("unknown")


def test_invalid_license_not_recognized() -> None:
    tracker = LicenseTracker()
    assert not tracker.is_recognized("proprietary")


def test_validate_source_config_passes() -> None:
    tracker = LicenseTracker()
    errors = tracker.validate_source({
        "license_type": "cc-by",
        "attribution_text": "OPD, University of Minnesota",
    })
    assert errors == []


def test_validate_source_config_missing_attribution() -> None:
    tracker = LicenseTracker()
    errors = tracker.validate_source({
        "license_type": "cc-by",
    })
    assert len(errors) == 1
    assert "attribution_text" in errors[0]


def test_validate_source_config_unknown_license_requires_review() -> None:
    tracker = LicenseTracker()
    errors = tracker.validate_source({
        "license_type": "unknown",
        "attribution_text": "Some source",
    })
    assert len(errors) == 1
    assert "manual review" in errors[0]


def test_validate_source_config_invalid_license() -> None:
    tracker = LicenseTracker()
    errors = tracker.validate_source({
        "license_type": "proprietary",
        "attribution_text": "Some source",
    })
    assert len(errors) == 1
    assert "not recognized" in errors[0]
