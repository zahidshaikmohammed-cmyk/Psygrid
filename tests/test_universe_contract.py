import json
from pathlib import Path
from types import SimpleNamespace

from output import market_live_json
from app import SHARD_RANGES
from state import PsygridState


EXPECTED = 990
EXPECTED_KEYS = {"symbol", "security_id", "previous_close", "today_open", "candles_1m"}
EXPECTED_CANDLE_KEYS = {"timestamp", "open", "high", "low", "close", "volume"}


def test_canonical_stock_universe_is_exactly_450_and_unique():
    payload = json.loads(Path("stocks.json").read_text(encoding="utf-8"))
    symbols = payload["symbols"]
    assert payload["universe"] == "PSYGRID_990"
    assert payload["exchange"] == "NSE"
    assert payload["instrument"] == "EQUITY"
    assert len(symbols) == EXPECTED
    assert len(set(symbols)) == EXPECTED


def test_shards_are_exactly_10_disjoint_blocks_of_45():
    assert tuple(end - start for _, start, end in SHARD_RANGES) == (45,) * 22
    assert SHARD_RANGES[0][1:] == (0, 45)
    assert SHARD_RANGES[-1][1:] == (945, 990)
    for left, right in zip(SHARD_RANGES, SHARD_RANGES[1:]):
        assert left[2] == right[1]
    ranges = [(start, end) for _, start, end in SHARD_RANGES]
    flattened = [index for start, end in ranges for index in range(start, end)]
    assert len(flattened) == EXPECTED
    assert len(set(flattened)) == EXPECTED
    assert flattened == list(range(EXPECTED))


def test_public_output_contract_has_no_duplicate_minutes_or_extra_stock_fields():
    settings = SimpleNamespace(timezone="Asia/Kolkata", max_live_age_seconds=30)
    state = PsygridState(settings)
    instruments = [
        SimpleNamespace(symbol=f"S{index:03d}", security_id=str(index), exchange_segment="NSE_EQ", instrument="EQUITY")
        for index in range(EXPECTED)
    ]
    state.begin("2026-09-16", instruments)
    for index in range(EXPECTED):
        sid = str(index)
        state.set_market_reference(sid, previous_close=100.0, today_open=101.0)
        state.live_candles[sid] = [
            {
                "timestamp": 1000,
                "epoch": 1000,
                "open": 101.0,
                "high": 102.0,
                "low": 100.0,
                "close": 101.5,
                "volume": 10,
                "source": "DHAN_HISTORICAL_API",
                "complete": True,
            },
            {
                "timestamp": 1000,
                "epoch": 1000,
                "open": 101.0,
                "high": 103.0,
                "low": 99.0,
                "close": 102.0,
                "volume": 20,
                "source": "DHAN_WEBSOCKET_QUOTE",
                "complete": True,
            },
        ]

    payload = market_live_json(state)
    assert payload["universe_size"] == EXPECTED
    assert payload["stock_count"] == EXPECTED
    assert payload["data_policy"] == "1M_OHLCV_PLUS_PREVIOUS_CLOSE_AND_TODAY_OPEN"
    assert payload["synthetic_candles"] is False
    assert set(payload["stocks"]) == {f"S{index:03d}" for index in range(EXPECTED)}

    for stock in payload["stocks"].values():
        assert set(stock) == EXPECTED_KEYS
        candles = stock["candles_1m"]
        assert len(candles) == 1
        assert set(candles[0]) == EXPECTED_CANDLE_KEYS
        # Historical data is the canonical source on a same-minute collision.
        assert candles[0]["volume"] == 10
        assert len({row["timestamp"] for row in candles}) == len(candles)
        assert "5m" not in stock
        assert "15m" not in stock
        assert "1h" not in stock
        assert "depth" not in stock
        assert "indicators" not in stock
