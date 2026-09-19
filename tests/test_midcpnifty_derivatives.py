from midcpnifty_options import (
    MIDCPNIFTY_OPTIONS_EXCHANGE_SEGMENT,
    MIDCPNIFTY_OPTIONS_INSTRUMENT,
    MIDCPNIFTY_OPTIONS_SECURITY_ID,
    MIDCPNIFTY_OPTIONS_SYMBOL,
    MidcapNiftyOptionsState,
    _normalize_chain,
)
from midcpnifty_depth import _parse_depth_message, _select_contracts


def test_midcpnifty_options_identity():
    assert MIDCPNIFTY_OPTIONS_SYMBOL == "MIDCPNIFTY"
    assert MIDCPNIFTY_OPTIONS_SECURITY_ID == "442"
    assert MIDCPNIFTY_OPTIONS_EXCHANGE_SEGMENT == "IDX_I"
    assert MIDCPNIFTY_OPTIONS_INSTRUMENT == "INDEX"


def test_midcpnifty_chain_sorting():
    raw = {"oc": {"100.0": {"ce": {"security_id": 2}}, "75.0": {"pe": {"security_id": 1}}}}
    rows = _normalize_chain(raw)
    assert [row["strike"] for row in rows] == [75.0, 100.0]


def test_midcpnifty_state_is_ram_only_and_non_synthetic():
    class Settings:
        timezone = "Asia/Kolkata"

    state = MidcapNiftyOptionsState(Settings())
    snapshot = state.snapshot()
    assert snapshot["storage"] == "RAM_ONLY"
    assert snapshot["synthetic_data"] is False
    assert snapshot["status"] == "STARTING"


def test_midcpnifty_depth_selects_nearest_25_strikes_and_ce_pe():
    class OptionState:
        def snapshot(self):
            return {
                "expiry": "2026-09-29",
                "underlying_ltp": 1000.0,
                "strikes": [
                    {"strike": float(900 + i * 10), "ce": {"security_id": str(1000 + i)}, "pe": {"security_id": str(2000 + i)} }
                    for i in range(31)
                ],
            }

    contracts, expiry, ltp = _select_contracts(OptionState())
    assert expiry == "2026-09-29"
    assert ltp == 1000.0
    assert len(contracts) == 50
    assert {contract.option_type for contract in contracts} == {"CE", "PE"}
    assert len({contract.strike for contract in contracts}) == 25


def test_midcpnifty_depth_parser_reads_20_levels():
    import struct

    payload = bytearray(332)
    struct.pack_into("<H", payload, 0, 332)
    payload[2] = 41
    struct.pack_into("<i", payload, 4, 12345)
    for index in range(20):
        struct.pack_into("<dII", payload, 12 + index * 16, 100.0 + index, 10 + index, 1 + index)
    parsed = _parse_depth_message(bytes(payload))
    assert len(parsed) == 1
    security_id, side, levels = parsed[0]
    assert security_id == "12345"
    assert side == "bid"
    assert len(levels) == 20
    assert levels[0] == {"level": 1, "price": 100.0, "quantity": 10, "orders": 1}
