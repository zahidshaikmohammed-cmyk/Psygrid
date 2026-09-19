from __future__ import annotations

"""Aggregates freshness/status across every Psygrid live feed into one
lightweight endpoint. Never recomputes or refetches upstream data itself —
it only reads what each manager/state object already knows about itself
(a status string, a last-update timestamp, an error string, a record
count), so it stays cheap regardless of how many feeds exist.
"""

from datetime import datetime, timezone
from typing import Optional


def _parse_updated_at(value) -> Optional[datetime]:
    if value in (None, ""):
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), timezone.utc)
        text = str(value).strip()
        if text.endswith(" IST"):
            # Best-effort: treat naive IST-labelled strings as needing no
            # further timezone math here; freshness only needs the delta,
            # and callers passing this format also pass a tz-aware "now".
            text = text[:-4]
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except (TypeError, ValueError):
        return None


def component_health(
    *,
    name: str,
    status: Optional[str],
    updated_at,
    expected_refresh_seconds: float,
    now: datetime,
    last_error: str = "",
    record_count: Optional[int] = None,
    expected_record_count: Optional[int] = None,
    extra: Optional[dict] = None,
) -> dict:
    parsed = _parse_updated_at(updated_at)
    age_seconds = None
    if parsed is not None:
        try:
            reference = now.astimezone(parsed.tzinfo) if parsed.tzinfo else now
            age_seconds = max(0.0, (reference - parsed).total_seconds())
        except (OverflowError, ValueError):
            age_seconds = None

    if status in (None, "ERROR", "CONFIG_ERROR", "AUTH_ERROR"):
        freshness_status = "ERROR"
    elif status in ("STARTING", "AUTHENTICATING", "AUTH_WAITING"):
        freshness_status = "WARNING"
    elif age_seconds is None:
        # A healthy status without a numeric timestamp (some in-process
        # runtimes don't track one) is trusted rather than assumed stale.
        freshness_status = "FRESH" if status in ("LIVE", "OK", "CONNECTED") else "WARNING"
    elif age_seconds > expected_refresh_seconds * 5:
        freshness_status = "STALE"
    elif age_seconds > expected_refresh_seconds * 2:
        freshness_status = "WARNING"
    else:
        freshness_status = "FRESH"

    result: dict = {
        "name": name,
        "status": freshness_status,
        "source_status": status,
        "age_seconds": round(age_seconds, 2) if age_seconds is not None else None,
        "last_update": updated_at,
        "expected_refresh_seconds": expected_refresh_seconds,
    }
    if last_error:
        result["last_error"] = last_error
    if record_count is not None:
        result["record_count"] = record_count
    if expected_record_count is not None:
        result["expected_record_count"] = expected_record_count
        result["record_count_match"] = record_count == expected_record_count if record_count is not None else None
    if extra:
        result.update(extra)
    return result


def build_health(components: list[dict], market_status: str) -> dict:
    healthy = sum(1 for c in components if c["status"] == "FRESH")
    warning = sum(1 for c in components if c["status"] == "WARNING")
    stale = sum(1 for c in components if c["status"] == "STALE")
    error = sum(1 for c in components if c["status"] == "ERROR")
    if error > 0:
        overall = "DEGRADED" if healthy > 0 else "DOWN"
    elif stale > 0:
        overall = "DEGRADED"
    elif warning > 0:
        overall = "WARNING"
    else:
        overall = "HEALTHY"

    return {
        "service": "PSYGRID",
        "market_status": market_status,
        "overall_status": overall,
        "component_count": len(components),
        "healthy_count": healthy,
        "warning_count": warning,
        "stale_count": stale,
        "error_count": error,
        "components": {c["name"]: c for c in components},
    }
