from option_analytics import compute_chain_analytics


def test_missing_security_id_is_counted_not_silently_dropped():
    rows = [{"strike": 100.0, "ce": {"last_price": 1.0, "oi": 10}, "pe": {"security_id": "P1", "last_price": 1.0, "oi": 10}}]
    analytics, _ = compute_chain_analytics(rows, 100.0, {})
    assert analytics["data_quality"]["contracts_total"] == 2
    assert analytics["data_quality"]["contracts_missing_security_id"] == 1


def test_crossed_market_is_detected():
    rows = [{"strike": 100.0, "ce": {"security_id": "C1", "top_bid_price": 55.0, "top_ask_price": 50.0, "last_price": 52.0, "oi": 10}}]
    analytics, _ = compute_chain_analytics(rows, 100.0, {})
    assert analytics["data_quality"]["crossed_markets_detected"] == 1


def test_normal_market_is_not_flagged_crossed():
    rows = [{"strike": 100.0, "ce": {"security_id": "C1", "top_bid_price": 49.0, "top_ask_price": 50.0, "last_price": 49.5, "oi": 10}}]
    analytics, _ = compute_chain_analytics(rows, 100.0, {})
    assert analytics["data_quality"]["crossed_markets_detected"] == 0


def test_duplicate_security_id_is_counted():
    rows = [
        {"strike": 100.0, "ce": {"security_id": "DUP", "last_price": 1.0, "oi": 5}},
        {"strike": 110.0, "pe": {"security_id": "DUP", "last_price": 1.0, "oi": 5}},
    ]
    analytics, _ = compute_chain_analytics(rows, 100.0, {})
    assert analytics["data_quality"]["duplicate_security_ids"] == 1
