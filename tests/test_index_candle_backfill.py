"""1m index candles keep growing when the index websocket sends nothing, and
websocket + backfilled candles never produce a duplicate minute. 5m/15m/1h
history and 1m gaps older than a few minutes are recovered the same way."""

from types import SimpleNamespace

from index_layer import IndexInstrument, IndexLayerManager, IndexState

T0 = 1758700500  # a minute boundary


def _settings():
    return SimpleNamespace(timezone="Asia/Kolkata", market_start="09:15", market_end="15:30")


def _candle(ts, close=25000.0):
    return {"timestamp": ts, "open": close, "high": close + 5, "low": close - 5, "close": close,
            "volume": 0, "source": "DHAN_HISTORICAL_API", "complete": True}


class FakeDhan:
    def __init__(self, rows_1m=None, rows_by_interval=None, fail=False):
        self.rows_1m = rows_1m or {}
        self.rows_by_interval = rows_by_interval or {}
        self.fail, self.calls = fail, []

    def load_today_completed_intraday(self, item, interval):
        self.calls.append((item.security_id, interval))
        if self.fail:
            raise RuntimeError("Dhan API HTTP 429 rate limit")
        if interval == 1:
            return [dict(r) for r in self.rows_1m.get(item.security_id, [])]
        return [dict(r) for r in self.rows_by_interval.get((item.security_id, interval), [])]


def _manager(api, keys=("nifty", "banknifty", "sensex", "niftyit")):
    m = IndexLayerManager.__new__(IndexLayerManager)
    m.settings, m.dhan_api, m.resolution_errors = _settings(), api, {}
    ids = {"nifty": "13", "banknifty": "25", "sensex": "51", "niftyit": "29"}
    m.states = {}
    for k in keys:
        st = IndexState(m.settings, k, k.upper(), IndexInstrument(security_id=ids[k], exchange_segment="IDX_I"))
        st.session_status = "LIVE"
        m.states[k] = st
    return m


def test_backfill_grows_series_without_any_websocket_ticks():
    api = FakeDhan({"13": [_candle(T0), _candle(T0 + 60)], "25": [_candle(T0)], "51": [_candle(T0)]})
    m = _manager(api)
    m.states["nifty"].live_candles = [_candle(T0 - 60)]
    m.backfill_recent_candles()
    assert [c["timestamp"] for c in m.states["nifty"].live_candles] == [T0 - 60, T0, T0 + 60]
    assert len(m.states["sensex"].live_candles) == 1
    # only the derivatives underlyings are backfilled, not every index
    assert sorted(api.calls) == [("13", 1), ("13", 5), ("13", 15), ("13", 60), ("25", 1), ("25", 5), ("25", 15), ("25", 60), ("51", 1), ("51", 5), ("51", 15), ("51", 60)]
    assert m.states["niftyit"].live_candles == []


def test_backfill_uses_the_whole_trading_day_not_just_a_short_lookback():
    # An 11-minute websocket gap is older than any short lookback window -
    # the exact shape of the production incident this guards against.
    api = FakeDhan({"13": [_candle(T0 - 11 * 60 + i * 60) for i in range(12)]})
    m = _manager(api)
    m.states["nifty"].live_candles = [_candle(T0 - 12 * 60)]
    m.backfill_recent_candles()
    assert len(m.states["nifty"].live_candles) == 13
    assert ("13", 1) in api.calls  # whole-day call, no lookback_intervals parameter


def test_backfill_fills_5m_15m_1h_history():
    api = FakeDhan(rows_by_interval={
        ("13", 5): [_candle(T0)],
        ("13", 15): [_candle(T0)],
        ("13", 60): [_candle(T0)],
    })
    m = _manager(api, keys=("nifty",))
    assert m.states["nifty"].historical.get("5m", []) == []
    m.backfill_recent_candles()
    assert len(m.states["nifty"].historical["5m"]) == 1
    assert len(m.states["nifty"].historical["15m"]) == 1
    assert len(m.states["nifty"].historical["1h"]) == 1


def test_backfill_is_idempotent_across_repeated_cycles():
    api = FakeDhan({"13": [_candle(T0)]}, rows_by_interval={("13", 5): [_candle(T0)]})
    m = _manager(api, keys=("nifty",))
    m.backfill_recent_candles()
    m.backfill_recent_candles()
    assert len(m.states["nifty"].live_candles) == 1
    assert len(m.states["nifty"].historical["5m"]) == 1


def test_backfill_failure_keeps_series_and_records_error():
    m = _manager(FakeDhan(fail=True))
    m.states["nifty"].live_candles = [_candle(T0)]
    m.backfill_recent_candles()
    assert [c["timestamp"] for c in m.states["nifty"].live_candles] == [T0]
    assert "429" in m.states["nifty"].last_backfill_error


def test_websocket_rollover_never_duplicates_a_backfilled_minute():
    st = _manager(FakeDhan(), keys=("nifty",)).states["nifty"]
    st.update_quote(25000.0, T0 + 10, 0, 0)          # websocket builds minute T0
    st.merge_today_1m([_candle(T0, close=25001.0)])  # backfill already has T0
    st.update_quote(25010.0, T0 + 65, 0, 0)          # rollover into T0+60
    ts = [c["timestamp"] for c in st.live_candles]
    assert ts == [T0] and st.live_candles[0]["close"] == 25001.0  # official candle kept
    st.finalize_current()
    assert [c["timestamp"] for c in st.live_candles] == [T0, T0 + 60]
