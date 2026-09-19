import threading
from types import SimpleNamespace

from market_breadth import build_market_breadth, build_sector_breadth
from sector_taxonomy import sector_for_symbol


def _fake_state():
    state = SimpleNamespace()
    state.lock = threading.RLock()
    state.settings = SimpleNamespace(timezone="Asia/Kolkata")
    state.session_status = "LIVE"
    state.instruments = {
        "1": {"symbol": "TCS", "security_id": "1"},
        "2": {"symbol": "INFY", "security_id": "2"},
        "3": {"symbol": "HDFCBANK", "security_id": "3"},
        "4": {"symbol": "UNKNOWNCO", "security_id": "4"},
    }
    state.last_ltp_by_security = {"1": 3900.0, "2": 1500.0, "3": 1700.0}  # "4" missing -> unknown
    state.market_reference = {
        "1": {"previous_close": 3800.0},  # up
        "2": {"previous_close": 1550.0},  # down
        "3": {"previous_close": 1700.0},  # unchanged
    }
    state.live_candles = {
        "1": [{"high": 3950.0, "low": 3850.0}],
        "2": [{"high": 1560.0, "low": 1490.0}],
        "3": [{"high": 1710.0, "low": 1690.0}],
    }
    state.current_1m = {}
    return state


def test_advance_decline_counts():
    state = _fake_state()
    payload = build_market_breadth(state)
    assert payload["advancing"] == 1
    assert payload["declining"] == 1
    assert payload["unchanged"] == 1
    assert payload["unknown"] == 1
    assert payload["universe_size"] == 4
    assert payload["coverage_count"] == 3


def test_no_bullish_bearish_labels_anywhere():
    state = _fake_state()
    payload = build_market_breadth(state)
    import json
    text = json.dumps(payload).upper()
    for forbidden in ("BULLISH", "BEARISH", "CONFIRMED", "SIGNAL", "BUY", "SELL"):
        assert forbidden not in text


def test_new_session_high_flagged_when_ltp_at_day_high():
    state = _fake_state()
    state.last_ltp_by_security["1"] = 3950.0  # equals day high
    payload = build_market_breadth(state)
    tcs = next(r for r in payload["constituents"] if r["symbol"] == "TCS")
    assert tcs["is_new_session_high"] is True


def test_sector_breadth_groups_by_sector_and_stays_raw():
    state = _fake_state()
    payload = build_sector_breadth(state)
    banking = next(s for s in payload["sectors"] if s["sector"] == "BANKING")
    assert banking["constituent_count"] == 1
    it_sector = next(s for s in payload["sectors"] if s["sector"] == "INFORMATION_TECHNOLOGY")
    assert it_sector["constituent_count"] == 2
    import json
    assert "BULLISH" not in json.dumps(payload).upper()


def test_unclassified_symbol_falls_back_to_other():
    assert sector_for_symbol("UNKNOWNCO") == "OTHER"
