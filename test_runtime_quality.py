from __future__ import annotations

from types import SimpleNamespace

from dhan_api_runtime import DhanAPI
from runtime_output import _stock_runtime_fixup


def test_rest_depth_is_normalized_to_flat_levels():
    row = {
        "depth": {
            "buy": [{"price": 100.0, "quantity": 12, "orders": 2}],
            "sell": [{"price": 100.5, "quantity": 15, "orders": 3}],
        }
    }
    DhanAPI._normalize_depth(row)
    assert isinstance(row["depth"], list)
    assert row["depth"][0]["bid_price"] == 100.0
    assert row["depth"][0]["ask_price"] == 100.5
    assert row["depth"][0]["bid_quantity"] == 12
    assert row["depth"][0]["ask_quantity"] == 15


def test_runtime_output_uses_rest_quote_as_fresh():
    state = SimpleNamespace(
        lock=__import__("threading").RLock(),
        market_context={
            "123": {
                "ltp": 250.25,
                "best_bid": 250.20,
                "best_ask": 250.30,
                "received_epoch": 1000.0,
                "source": "DHAN_REST_QUOTE_RECOVERY",
            }
        },
        last_tick_received_by_security={},
    )
    state.freshness = lambda security_id, now: {
        "status": "LIVE",
        "data_age_seconds": 1.2,
        "live_data_valid": True,
        "source": "DHAN_REST_QUOTE_RECOVERY",
    }
    stock = {"security_id": "123", "current": {}}
    _stock_runtime_fixup(state, stock)
    assert stock["freshness"]["live_data_valid"] is True
    assert stock["ltp_source"] == "DHAN_REST_QUOTE_RECOVERY"
    assert stock["current"]["ltp"] == 250.25
    assert stock["current"]["quote_valid"] is True


def test_runtime_output_uses_websocket_receipt_for_freshness_timestamp():
    state = SimpleNamespace(
        lock=__import__("threading").RLock(),
        market_context={"123": {"ltp": 251.0, "source": "DHAN_WEBSOCKET_FULL"}},
        last_tick_received_by_security={"123": 1000.0},
    )
    state.freshness = lambda security_id, now: {
        "status": "LIVE",
        "data_age_seconds": 2.0,
        "live_data_valid": True,
        "source": "DHAN_WEBSOCKET_FULL",
    }
    stock = {"security_id": "123", "current": {}}
    _stock_runtime_fixup(state, stock)
    assert stock["ltp_source"] == "DHAN_WEBSOCKET_FULL"
    assert stock["current"]["quote_valid"] is True
    assert stock["current"]["ltp"] == 251.0
