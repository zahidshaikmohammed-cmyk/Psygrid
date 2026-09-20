from types import SimpleNamespace

from sensex_options import SensexOptionsState, _normalize_chain
from sensex_depth import SensexDepthContract, SensexDepthState, _parse_depth_message, _select_contracts
import struct


def settings():
    return SimpleNamespace(timezone="Asia/Kolkata")


def test_sensex_option_identity_and_ram_state():
    state = SensexOptionsState(settings())
    payload = {"last_price": 82000.0, "oc": {"82000": {"ce": {"security_id": 3001}, "pe": {"security_id": 3002}}}}
    state.set_snapshot(payload, ["2026-09-30"], "2026-09-30")
    snap = state.snapshot()
    assert snap["symbol"] == "SENSEX"
    assert snap["security_id"] == "51"
    assert snap["exchange_segment"] == "IDX_I"  # underlying identity, matches sealed sensex.py
    assert snap["instrument"] == "INDEX"
    assert snap["status"] == "LIVE"
    assert snap["synthetic_data"] is False
    assert snap["storage"] == "RAM_ONLY"
    assert snap["strikes"][0]["ce"]["security_id"] == 3001
    assert "analytics" in snap


def test_sensex_chain_sorting():
    rows = _normalize_chain({"oc": {"82100": {"ce": {}}, "81900": {"pe": {}}, "82000": {"ce": {}, "pe": {}}}})
    assert [row["strike"] for row in rows] == [81900.0, 82000.0, 82100.0]


def test_sensex_depth_uses_bse_fno_segment():
    from sensex_depth import SENSEX_DEPTH_EXCHANGE_SEGMENT
    assert SENSEX_DEPTH_EXCHANGE_SEGMENT == "BSE_FNO"


def test_sensex_depth_selects_nearest_25_strikes_and_both_sides():
    strikes = {}
    for strike in range(80000, 84001, 100):
        strikes[str(strike)] = {"ce": {"security_id": str(300000 + strike)}, "pe": {"security_id": str(400000 + strike)}}
    option_state = SimpleNamespace(snapshot=lambda: {"expiry": "2026-09-30", "underlying_ltp": 82000.0, "strikes": [{"strike": float(k), **v} for k, v in strikes.items()]})
    contracts, expiry, ltp = _select_contracts(option_state)
    assert expiry == "2026-09-30"
    assert ltp == 82000.0
    assert len(contracts) == 50
    assert len({c.strike for c in contracts}) == 25
    assert {c.option_type for c in contracts} == {"CE", "PE"}


def test_sensex_depth_parser():
    header = struct.pack("<HBBiI", 332, 41, 0, 55555, 0)
    levels = b"".join(struct.pack("<dII", 200.0 + i, 5 + i, 1) for i in range(20))
    parsed = _parse_depth_message(header + levels)
    assert len(parsed) == 1
    security_id, side, rows = parsed[0]
    assert security_id == "55555"
    assert side == "bid"
    assert len(rows) == 20


def test_sensex_depth_crossed_book_detection():
    state = SensexDepthState(settings())
    state.set_contracts([SensexDepthContract("1", 82000.0, "CE", "2026-09-30")], "2026-09-30")
    state.update_depth("1", "bid", [{"level": 1, "price": 105.0, "quantity": 10, "orders": 1}])
    state.update_depth("1", "ask", [{"level": 1, "price": 100.0, "quantity": 10, "orders": 1}])
    assert state.snapshot()["contracts"][0]["crossed_book"] is True
