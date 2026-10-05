"""A symbol with no candles carries ``freshness: None``; the snapshot must count it, not crash (HTTP 500 live)."""

from indicator_runtime import IndicatorRuntime


def test_snapshot_tolerates_no_data_results_with_null_freshness():
    source = {"stocks": {"AAA": {}, "BBB": {}}}
    runtime = IndicatorRuntime(None, lambda _state: source)
    runtime._results = {
        "AAA": {"status": "OK", "freshness": {"status": "FRESH"}},
        "BBB": {"status": "NO_DATA", "as_of": None, "freshness": None, "indicators": {}, "indicator_status": {}},
    }
    snap = runtime.snapshot()
    assert snap["fresh_count"] == 1
    assert snap["stale_count"] == 0
    assert snap["stock_count"] == 2
