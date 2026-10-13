"""Node fetcher safety (offline): request plan, market-hours refusal, live-service health guard."""

import importlib.util
from datetime import datetime, timedelta
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "behaviour_fetch", Path(__file__).resolve().parents[1] / "deploy/live-core/behaviour_fetch.py"
)
bf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bf)
IST = bf.IST


def test_request_plan_tiles_the_span_with_a_lead_in():
    start = datetime(2021, 10, 1, 9, 15, tzinfo=IST)
    end = datetime(2026, 10, 12, 15, 30, tzinfo=IST)
    w = bf.windows(start, end)
    assert len(w) == 22
    assert w[0][0] == start.date() and w[-1][1] == end.date()
    for (cur, stop, frm, to), nxt in zip(w, [*w[1:], None], strict=True):
        assert frm.date() == cur - timedelta(days=bf.OVERLAP_DAYS) and frm.strftime("%H:%M") == "09:15"
        assert to.date() == stop and to.strftime("%H:%M") == "15:30"
        assert (to.date() - frm.date()).days <= bf.SPAN_DAYS
        if nxt:
            assert nxt[0] == stop + timedelta(days=1)  # no gap, no overlap in the kept days
    assert all(frm.date() == cur for cur, _, frm, _ in bf.windows(start, end, 5))  # no lead-in at 5-minute


def test_market_hours_refusal():
    assert bf.market_busy(datetime(2026, 10, 13, 8, 45, tzinfo=IST))
    assert bf.market_busy(datetime(2026, 10, 13, 15, 44, tzinfo=IST))
    assert not bf.market_busy(datetime(2026, 10, 13, 15, 45, tzinfo=IST))
    assert not bf.market_busy(datetime(2026, 10, 13, 8, 44, tzinfo=IST))
    assert not bf.market_busy(datetime(2026, 10, 17, 11, 0, tzinfo=IST))  # Saturday


class _Resp:
    def __init__(self, code=200, status="OK", resets=0):
        self.status_code, self._j = code, {"status": status, "supervisor": {"state": "CLOSED", "hard_resets": resets}}

    def json(self):
        return self._j


def test_health_guard_stops_on_two_bad_checks_in_a_row(monkeypatch):
    seq = iter([_Resp(), _Resp(503), _Resp(), _Resp(503), _Resp(503)])
    monkeypatch.setattr(bf.requests, "get", lambda *a, **k: next(seq))
    g = bf.HealthGuard(print)
    assert g.check().startswith("OK/CLOSED")
    assert "WARN HTTP 503" in g.check()  # one bad check is tolerated
    g.check()
    g.check()
    with pytest.raises(bf.Stop, match="HTTP 503"):
        g.check()


def test_health_guard_stops_on_a_hard_reset_or_unreachable_service(monkeypatch):
    seq = iter([_Resp(resets=2), _Resp(resets=3), _Resp(resets=3)])
    monkeypatch.setattr(bf.requests, "get", lambda *a, **k: next(seq))
    g = bf.HealthGuard(print)
    g.check()
    g.check()
    with pytest.raises(bf.Stop, match="hard reset"):
        g.check()

    def down(*a, **k):
        raise bf.requests.ConnectionError("refused")

    monkeypatch.setattr(bf.requests, "get", down)
    g2 = bf.HealthGuard(print)
    g2.check()
    with pytest.raises(bf.Stop, match="ConnectionError"):
        g2.check()
