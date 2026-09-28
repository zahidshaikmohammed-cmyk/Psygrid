"""1m index candles keep growing when the index websocket sends nothing, and
websocket + backfilled candles never produce a duplicate minute."""

from types import SimpleNamespace

from index_layer import IndexInstrument, IndexLayerManager, IndexState

T0 = 1758700500  # a minute boundary


def _settings():
    return SimpleNamespace(timezone="Asia/Kolkata", market_start="09:15", market_end="15:30")


def _candle(ts, close=25000.0):
    return {"timestamp": ts, "open": close, "high": close + 5, "low": close - 5, "close": close,
            "volume": 0, "source": "DHAN_HISTORICAL_API", "complete": True}


class FakeDhan:
    def __init__(self, rows=None, fail=False):
        self.rows, self.fail, self.calls = rows or {}, fail, []

    def load_recent_completed_intraday(self, item, interval, lookback_intervals=4):
        self.calls.append((item.security_id, interval, lookback_intervals))
        if self.fail:
            raise RuntimeError("Dhan API HTTP 429 rate limit")
        return [dict(r) for r in self.rows.get(item.security_id, [])]


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
    assert sorted(c[0] for c in api.calls) == ["13", "25", "51"]
    assert m.states["niftyit"].live_candles == []


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
