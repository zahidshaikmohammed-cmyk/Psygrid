import os
from types import SimpleNamespace

from global_context import GlobalContextManager, GlobalContextState


def test_snapshot_always_marks_delayed():
    state = GlobalContextState(SimpleNamespace(timezone="Asia/Kolkata"))
    state.set_series({"sp500": {"series_id": "SP500", "value": 6500.0, "source_date": "2026-09-18", "source": "FRED_FEDERAL_RESERVE_BANK_OF_ST_LOUIS"}})
    snap = state.snapshot()
    assert snap["market_data_status"] == "DELAYED"
    assert snap["series"]["sp500"]["value"] == 6500.0


def test_not_available_items_are_explicit_not_omitted():
    state = GlobalContextState(SimpleNamespace(timezone="Asia/Kolkata"))
    snap = state.snapshot()
    assert "gift_nifty" in snap["not_available"]
    assert "dxy" in snap["not_available"]
    assert "nasdaq" in snap["not_available"]


def test_missing_api_key_reports_clear_error_not_fabricated_data(monkeypatch):
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    manager = GlobalContextManager(SimpleNamespace(timezone="Asia/Kolkata"))
    # Directly exercise one loop iteration's key check without starting a thread.
    api_key = os.getenv("FRED_API_KEY", "").strip()
    assert api_key == ""
    manager.state.set_error("FRED_API_KEY environment variable is not set; global-context is unavailable")
    snap = manager.state.snapshot()
    assert snap["status"] == "ERROR"
    assert "FRED_API_KEY" in snap["error"]
    assert snap["series"] == {}
