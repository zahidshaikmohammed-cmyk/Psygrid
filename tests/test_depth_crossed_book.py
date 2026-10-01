from types import SimpleNamespace

from index_depth import DepthContract, IndexDepthState
from index_options import NIFTY


def test_crossed_book_flagged_when_top_bid_exceeds_top_ask():
    state = IndexDepthState(SimpleNamespace(timezone="Asia/Kolkata"), NIFTY)
    state.set_contracts([DepthContract("1", 25000.0, "CE", "2026-10-30")], "2026-10-30")
    state.update_depth("1", "bid", [{"level": 1, "price": 105.0, "quantity": 10, "orders": 1}])
    state.update_depth("1", "ask", [{"level": 1, "price": 100.0, "quantity": 10, "orders": 1}])
    snap = state.snapshot()
    assert snap["contracts"][0]["crossed_book"] is True


def test_normal_book_is_not_flagged_crossed():
    state = IndexDepthState(SimpleNamespace(timezone="Asia/Kolkata"), NIFTY)
    state.set_contracts([DepthContract("1", 25000.0, "CE", "2026-10-30")], "2026-10-30")
    state.update_depth("1", "bid", [{"level": 1, "price": 99.0, "quantity": 10, "orders": 1}])
    state.update_depth("1", "ask", [{"level": 1, "price": 100.0, "quantity": 10, "orders": 1}])
    snap = state.snapshot()
    assert snap["contracts"][0]["crossed_book"] is False


def test_missing_one_side_is_not_flagged_crossed():
    state = IndexDepthState(SimpleNamespace(timezone="Asia/Kolkata"), NIFTY)
    state.set_contracts([DepthContract("1", 25000.0, "CE", "2026-10-30")], "2026-10-30")
    state.update_depth("1", "bid", [{"level": 1, "price": 99.0, "quantity": 10, "orders": 1}])
    snap = state.snapshot()
    assert snap["contracts"][0]["crossed_book"] is False
