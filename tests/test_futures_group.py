"""FuturesGroup: one instrument-master download and ONE batched quote request
per cycle for NIFTY/BANKNIFTY (NSE) and SENSEX (BSE)."""

import csv
import datetime as dt
import io
from types import SimpleNamespace
from unittest.mock import patch

from futures_layer import FuturesGroup, FuturesManager

SETTINGS = SimpleNamespace(timezone="Asia/Kolkata")


def _master_csv() -> str:
    exp = (dt.date.today() + dt.timedelta(days=7)).isoformat() + " 14:30:00"
    rows = [
        ("NSE", "NIFTY-Oct2026-FUT", "101"),
        ("NSE", "BANKNIFTY-Oct2026-FUT", "102"),
        ("BSE", "SENSEX-Oct2026-FUT", "103"),
    ]
    buf = io.StringIO()
    fields = [
        "SEM_EXM_EXCH_ID",
        "SEM_INSTRUMENT_NAME",
        "SEM_TRADING_SYMBOL",
        "SEM_SMST_SECURITY_ID",
        "SEM_EXPIRY_DATE",
        "SEM_LOT_UNITS",
        "SEM_TICK_SIZE",
    ]
    w = csv.DictWriter(buf, fieldnames=fields)
    w.writeheader()
    for exch, sym, sid in rows:
        w.writerow(dict(zip(fields, (exch, "FUTIDX", sym, sid, exp, "30", "0.05"), strict=True)))
    return buf.getvalue()


class RecordingAPI:
    def __init__(self, quotes, fail=False):
        self.quotes, self.fail, self.calls = quotes, fail, []

    def quote_snapshot(self, instruments):
        self.calls.append([(i.exchange_segment, i.security_id) for i in instruments])
        if self.fail:
            raise RuntimeError("Dhan API HTTP 429 rate limit")
        return {k: v for k, v in self.quotes.items() if k in {i.security_id for i in instruments}}


def _group(api):
    managers = [
        FuturesManager("NIFTY", SETTINGS, api),
        FuturesManager("BANKNIFTY", SETTINGS, api),
        FuturesManager("SENSEX", SETTINGS, api, exchange="BSE"),
    ]
    return managers, FuturesGroup(managers, api)


def test_one_download_resolves_nse_and_bse_contracts():
    api = RecordingAPI({})
    managers, group = _group(api)
    with patch("futures_layer.download_instrument_master", return_value=_master_csv()) as dl:
        group.resolve_contracts()
    assert dl.call_count == 1
    assert [m.state.contract.security_id for m in managers] == ["101", "102", "103"]
    assert managers[2].state.contract.exchange_segment == "BSE_FNO"


def test_one_batched_quote_request_updates_all_three():
    api = RecordingAPI({"101": {"last_price": 1.0}, "102": {"last_price": 2.0}, "103": {"last_price": 3.0}})
    managers, group = _group(api)
    with patch("futures_layer.download_instrument_master", return_value=_master_csv()):
        group.resolve_contracts()
    group.poll_quotes()
    assert len(api.calls) == 1
    assert sorted(api.calls[0]) == [("BSE_FNO", "103"), ("NSE_FNO", "101"), ("NSE_FNO", "102")]
    snaps = [m.state.snapshot() for m in managers]
    assert [s["status"] for s in snaps] == ["LIVE"] * 3
    assert [s["last_price"] for s in snaps] == [1.0, 2.0, 3.0]
    assert all(s["updated_at"] for s in snaps)


def test_missing_row_and_request_failure_are_reported_not_fabricated():
    api = RecordingAPI({"101": {"last_price": 1.0}})
    managers, group = _group(api)
    with patch("futures_layer.download_instrument_master", return_value=_master_csv()):
        group.resolve_contracts()
    group.poll_quotes()
    assert managers[0].state.status == "LIVE"
    assert "QUOTE_UNAVAILABLE" in managers[1].state.last_error
    api.fail = True
    group.poll_quotes()
    assert all(m.state.status == "ERROR" and "429" in m.state.last_error for m in managers)


def test_unresolved_symbol_gets_not_resolved_error():
    api = RecordingAPI({})
    m = FuturesManager("MIDCPNIFTY", SETTINGS, api)
    group = FuturesGroup([m], api)
    with patch("futures_layer.download_instrument_master", return_value=_master_csv()):
        group.resolve_contracts()
    assert m.state.contract is None and "NOT_RESOLVED" in m.state.last_error
    group.poll_quotes()
    assert api.calls == []


def test_manager_start_stop_delegate_to_group():
    api = RecordingAPI({})
    managers, group = _group(api)
    with patch("futures_layer.download_instrument_master", return_value=_master_csv()):
        for m in managers:
            m.start()
        assert group.quote_thread is not None and group.quote_thread.is_alive()
        for m in managers:
            m.stop()
    assert group.quote_thread is None
