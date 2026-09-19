from datetime import datetime, timedelta, timezone

from health_monitor import build_health, component_health


def test_fresh_when_recently_updated():
    now = datetime.now(timezone.utc)
    c = component_health(name="a", status="LIVE", updated_at=now.isoformat(), expected_refresh_seconds=2.0, now=now)
    assert c["status"] == "FRESH"
    assert c["age_seconds"] == 0.0


def test_stale_when_far_older_than_expected_refresh():
    now = datetime.now(timezone.utc)
    old = (now - timedelta(seconds=30)).isoformat()
    c = component_health(name="a", status="LIVE", updated_at=old, expected_refresh_seconds=2.0, now=now)
    assert c["status"] == "STALE"


def test_warning_between_two_and_five_times_expected_refresh():
    now = datetime.now(timezone.utc)
    old = (now - timedelta(seconds=45)).isoformat()
    c = component_health(name="a", status="LIVE", updated_at=old, expected_refresh_seconds=10.0, now=now)
    assert c["status"] == "WARNING"


def test_error_status_is_never_disguised_as_fresh():
    now = datetime.now(timezone.utc)
    c = component_health(name="a", status="ERROR", updated_at=now.isoformat(), expected_refresh_seconds=2.0, now=now, last_error="boom")
    assert c["status"] == "ERROR"
    assert c["last_error"] == "boom"


def test_missing_manager_reports_error_not_silence():
    now = datetime.now(timezone.utc)
    c = component_health(name="missing", status=None, updated_at=None, expected_refresh_seconds=2.0, now=now)
    assert c["status"] == "ERROR"


def test_healthy_status_without_timestamp_trusts_status():
    now = datetime.now(timezone.utc)
    c = component_health(name="a", status="LIVE", updated_at=None, expected_refresh_seconds=2.0, now=now)
    assert c["status"] == "FRESH"


def test_record_count_mismatch_is_surfaced():
    now = datetime.now(timezone.utc)
    c = component_health(name="a", status="LIVE", updated_at=now.isoformat(), expected_refresh_seconds=2.0, now=now, record_count=985, expected_record_count=990)
    assert c["record_count_match"] is False


def test_build_health_overall_status_down_when_all_errored():
    now = datetime.now(timezone.utc)
    components = [component_health(name="a", status=None, updated_at=None, expected_refresh_seconds=2.0, now=now)]
    result = build_health(components, "CLOSED")
    assert result["overall_status"] == "DOWN"
    assert result["error_count"] == 1


def test_build_health_overall_status_healthy_when_all_fresh():
    now = datetime.now(timezone.utc)
    components = [component_health(name="a", status="LIVE", updated_at=now.isoformat(), expected_refresh_seconds=2.0, now=now)]
    result = build_health(components, "OPEN")
    assert result["overall_status"] == "HEALTHY"
    assert result["components"]["a"]["status"] == "FRESH"


def test_build_health_degraded_when_mixed():
    now = datetime.now(timezone.utc)
    fresh = component_health(name="a", status="LIVE", updated_at=now.isoformat(), expected_refresh_seconds=2.0, now=now)
    errored = component_health(name="b", status=None, updated_at=None, expected_refresh_seconds=2.0, now=now)
    result = build_health([fresh, errored], "OPEN")
    assert result["overall_status"] == "DEGRADED"
