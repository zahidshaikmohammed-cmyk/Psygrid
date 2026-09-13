from types import SimpleNamespace

from banknifty_options import BankNiftyOptionsState, _normalize_chain
from banknifty_depth import _parse_depth_message, _select_contracts
import struct


def settings():
    return SimpleNamespace(timezone="Asia/Kolkata")


def test_banknifty_option_identity_and_ram_state():
    state = BankNiftyOptionsState(settings())
    payload = {"last_price": 55000.0, "oc": {"55000": {"ce": {"security_id": 1001}, "pe": {"security_id": 1002}}}}
    state.set_snapshot(payload, ["2026-09-24"], "2026-09-24")
    snap = state.snapshot()
    assert snap["symbol"] == "BANKNIFTY"
    assert snap["security_id"] == "25"
    assert snap["exchange_segment"] == "IDX_I"
    assert snap["instrument"] == "INDEX"
    assert snap["status"] == "LIVE"
    assert snap["synthetic_data"] is False
    assert snap["storage"] == "RAM_ONLY"
    assert snap["strikes"][0]["ce"]["security_id"] == 1001


def test_banknifty_chain_sorting():
    rows = _normalize_chain({"oc": {"55100": {"ce": {}}, "54900": {"pe": {}}, "55000": {"ce": {}, "pe": {}}}})
    assert [row["strike"] for row in rows] == [54900.0, 55000.0, 55100.0]


def test_banknifty_depth_selects_nearest_25_strikes_and_both_sides():
    strikes = {}
    for strike in range(52000, 54501, 100):
        strikes[str(strike)] = {"ce": {"security_id": str(100000 + strike)}, "pe": {"security_id": str(200000 + strike)}}
    option_state = SimpleNamespace(snapshot=lambda: {"expiry": "2026-09-24", "underlying_ltp": 53200.0, "strikes": [{"strike": float(k), **v} for k, v in strikes.items()]})
    contracts, expiry, ltp = _select_contracts(option_state)
    assert expiry == "2026-09-24"
    assert ltp == 53200.0
    assert len(contracts) == 50
    assert len({c.strike for c in contracts}) == 25
    assert {c.option_type for c in contracts} == {"CE", "PE"}


def test_banknifty_depth_parser():
    header = struct.pack("<HBBiI", 332, 41, 0, 12345, 0)
    levels = b"".join(struct.pack("<dII", 101.0 + i, 10 + i, 1 + i) for i in range(20))
    parsed = _parse_depth_message(header + levels)
    assert len(parsed) == 1
    security_id, side, rows = parsed[0]
    assert security_id == "12345"
    assert side == "bid"
    assert len(rows) == 20
    assert rows[0]["price"] == 101.0
    assert rows[-1]["level"] == 20
