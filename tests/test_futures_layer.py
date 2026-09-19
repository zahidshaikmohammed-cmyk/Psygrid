from types import SimpleNamespace

from derivatives_instruments import FuturesContract
from futures_layer import FuturesState


def _settings():
    return SimpleNamespace(timezone="Asia/Kolkata")


def test_state_is_ram_only_and_non_synthetic_before_contract_resolved():
    state = FuturesState("NIFTY", _settings())
    snap = state.snapshot()
    assert snap["symbol"] == "NIFTY"
    assert snap["status"] == "STARTING"
    assert snap["synthetic_data"] is False
    assert snap["storage"] == "RAM_ONLY"
    assert snap["security_id"] is None


def test_contract_identity_and_quote_fields():
    state = FuturesState("NIFTY", _settings())
    contract = FuturesContract(
        symbol="NIFTY", security_id="49081", exchange_segment="NSE_FNO", instrument="FUTIDX",
        trading_symbol="NIFTY-Oct2026-FUT", expiry_date="2026-10-30", lot_size=75, tick_size=0.05,
    )
    state.set_contract(contract)
    state.set_quote({"last_price": 25100.5, "volume": 123456, "oi": 5000000, "ohlc": {"open": 25000.0, "high": 25200.0, "low": 24950.0, "close": 25050.0}})
    snap = state.snapshot()
    assert snap["status"] == "LIVE"
    assert snap["trading_symbol"] == "NIFTY-Oct2026-FUT"
    assert snap["lot_size"] == 75
    assert snap["tick_size"] == 0.05
    assert snap["last_price"] == 25100.5
    assert snap["oi"] == 5000000
    assert snap["ohlc"]["high"] == 25200.0


def test_oi_change_tracked_across_quote_updates():
    state = FuturesState("NIFTY", _settings())
    contract = FuturesContract(symbol="NIFTY", security_id="1", exchange_segment="NSE_FNO", instrument="FUTIDX", trading_symbol="X", expiry_date="2026-10-30", lot_size=75, tick_size=0.05)
    state.set_contract(contract)
    state.set_quote({"oi": 1000000, "last_price": 100.0})
    assert state.snapshot()["oi_change"] is None  # first observation
    state.set_quote({"oi": 1050000, "last_price": 101.0})
    assert state.snapshot()["oi_change"] == 50000


def test_oi_change_resets_when_contract_rolls_to_new_expiry():
    state = FuturesState("NIFTY", _settings())
    c1 = FuturesContract(symbol="NIFTY", security_id="1", exchange_segment="NSE_FNO", instrument="FUTIDX", trading_symbol="X", expiry_date="2026-09-25", lot_size=75, tick_size=0.05)
    state.set_contract(c1)
    state.set_quote({"oi": 1000000})
    c2 = FuturesContract(symbol="NIFTY", security_id="2", exchange_segment="NSE_FNO", instrument="FUTIDX", trading_symbol="Y", expiry_date="2026-10-30", lot_size=75, tick_size=0.05)
    state.set_contract(c2)
    state.set_quote({"oi": 500000})
    assert state.snapshot()["oi_change"] is None  # new contract, no prior OI to diff against
