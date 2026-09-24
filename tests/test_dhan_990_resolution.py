import json
from pathlib import Path

from instrument_master import fetch_nse_equity_security_ids

EXPECTED = 989


def test_all_990_symbols_resolve_against_current_dhan_instrument_master():
    payload = json.loads(Path("stocks.json").read_text(encoding="utf-8"))
    symbols = [str(symbol).strip().upper() for symbol in payload["symbols"]]
    assert len(symbols) == EXPECTED
    assert len(set(symbols)) == EXPECTED

    resolved = fetch_nse_equity_security_ids(symbols, timeout=60)

    assert len(resolved) == EXPECTED
    assert set(resolved) == set(symbols)
    security_ids = list(resolved.values())
    assert all(security_ids)
    assert len(set(security_ids)) == EXPECTED
