from option_analytics import ChainAnalyticsTracker, compute_chain_analytics


def _rows():
    return [
        {"strike": 24900.0, "ce": {"security_id": "C1", "last_price": 150.0, "oi": 1000, "volume": 500, "implied_volatility": 14.2},
         "pe": {"security_id": "P1", "last_price": 40.0, "oi": 4000, "volume": 2000, "implied_volatility": 15.1}},
        {"strike": 25000.0, "ce": {"security_id": "C2", "last_price": 90.0, "oi": 6000, "volume": 3000, "implied_volatility": 13.8},
         "pe": {"security_id": "P2", "last_price": 85.0, "oi": 5500, "volume": 2800, "implied_volatility": 14.5}},
        {"strike": 25100.0, "ce": {"security_id": "C3", "last_price": 45.0, "oi": 7000, "volume": 4000, "implied_volatility": 13.5},
         "pe": {"security_id": "P3", "last_price": 150.0, "oi": 1200, "volume": 600, "implied_volatility": 15.9}},
    ]


def test_pcr_atm_and_max_pain():
    analytics, _ = compute_chain_analytics(_rows(), 25000.0, {})
    assert analytics["pcr_oi"] == (4000 + 5500 + 1200) / (1000 + 6000 + 7000)
    assert analytics["atm_strike"] == 25000.0
    assert analytics["max_pain_strike"] == 25000.0


def test_support_resistance_from_oi_concentration():
    analytics, _ = compute_chain_analytics(_rows(), 25000.0, {})
    assert analytics["resistance_strikes"][0] == 25100.0  # highest call OI
    assert analytics["support_strikes"][0] == 25000.0  # highest put OI


def test_moneyness_tags():
    analytics, _ = compute_chain_analytics(_rows(), 25000.0, {})
    by_id = {c["security_id"]: c for c in analytics["contracts"]}
    assert by_id["C1"]["moneyness"] == "ITM"  # call strike below spot
    assert by_id["C3"]["moneyness"] == "OTM"  # call strike above spot
    assert by_id["P1"]["moneyness"] == "OTM"  # put strike below spot
    assert by_id["P3"]["moneyness"] == "ITM"  # put strike above spot
    assert by_id["C2"]["moneyness"] == "ATM"


def test_first_snapshot_has_no_buildup_classification():
    analytics, _ = compute_chain_analytics(_rows(), 25000.0, {})
    assert all(c["oi_change_classification"] == "INSUFFICIENT_DATA" for c in analytics["contracts"])


def test_tracker_classifies_buildup_across_two_snapshots():
    tracker = ChainAnalyticsTracker()
    tracker.update(_rows(), 25000.0)

    rows2 = _rows()
    rows2[1]["ce"]["last_price"] = 95.0
    rows2[1]["ce"]["oi"] = 6500  # price up, OI up
    rows2[1]["pe"]["last_price"] = 80.0
    rows2[1]["pe"]["oi"] = 5000  # price down, OI down

    analytics = tracker.update(rows2, 25010.0)
    by_id = {c["security_id"]: c for c in analytics["contracts"]}
    assert by_id["C2"]["oi_change_classification"] == "LONG_BUILDUP"
    assert by_id["P2"]["oi_change_classification"] == "LONG_UNWINDING"


def test_empty_chain_yields_none_pcr_and_no_max_pain():
    analytics, _ = compute_chain_analytics([], 25000.0, {})
    assert analytics["pcr_oi"] is None
    assert analytics["pcr_volume"] is None
    assert analytics["max_pain_strike"] is None
    assert analytics["atm_strike"] is None
