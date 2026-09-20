from derivatives_instruments import _parse_expiry, _float_or_none, _int_or_none


def test_parse_expiry_handles_common_formats():
    assert _parse_expiry("2026-10-30").isoformat() == "2026-10-30"
    assert _parse_expiry("30-10-2026").isoformat() == "2026-10-30"
    assert _parse_expiry("30/10/2026").isoformat() == "2026-10-30"
    assert _parse_expiry("").__bool__() is False if _parse_expiry("") else True


def test_parse_expiry_returns_none_for_garbage():
    assert _parse_expiry("not-a-date") is None
    assert _parse_expiry("") is None


def test_int_and_float_coercion_never_raises():
    assert _int_or_none("75") == 75
    assert _int_or_none("75.0") == 75
    assert _int_or_none("garbage") is None
    assert _float_or_none("0.05") == 0.05
    assert _float_or_none("garbage") is None


def test_fetch_front_month_futures_respects_exchange_parameter():
    import csv
    import datetime as dt
    import io
    from unittest.mock import MagicMock, patch
    from derivatives_instruments import fetch_front_month_index_futures

    next_week = (dt.date.today() + dt.timedelta(days=7)).isoformat()
    rows = [
        {"SEM_EXM_EXCH_ID": "NSE", "SEM_INSTRUMENT_NAME": "FUTIDX", "SEM_TRADING_SYMBOL": f"NIFTY-{next_week}-FUT", "SEM_SMST_SECURITY_ID": "1", "SEM_EXPIRY_DATE": next_week, "SEM_LOT_UNITS": "75", "SEM_TICK_SIZE": "0.05"},
        {"SEM_EXM_EXCH_ID": "BSE", "SEM_INSTRUMENT_NAME": "FUTIDX", "SEM_TRADING_SYMBOL": f"SENSEX-{next_week}-FUT", "SEM_SMST_SECURITY_ID": "2", "SEM_EXPIRY_DATE": next_week, "SEM_LOT_UNITS": "10", "SEM_TICK_SIZE": "0.05"},
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    for row in rows:
        writer.writerow(row)

    fake_response = MagicMock()
    fake_response.text = buf.getvalue()
    fake_response.encoding = "utf-8"
    fake_response.raise_for_status = lambda: None

    with patch("derivatives_instruments.requests.get", return_value=fake_response):
        nse_result = fetch_front_month_index_futures(("NIFTY",), exchange="NSE")
        bse_result = fetch_front_month_index_futures(("SENSEX",), exchange="BSE")
        # An NSE-only fetch for SENSEX finds nothing - it never picks up the
        # BSE row by accident, and the BSE row's exchange_segment is correct.
        cross_result = fetch_front_month_index_futures(("SENSEX",), exchange="NSE")

    assert nse_result["NIFTY"].security_id == "1"
    assert nse_result["NIFTY"].exchange_segment == "NSE_FNO"
    assert bse_result["SENSEX"].security_id == "2"
    assert bse_result["SENSEX"].exchange_segment == "BSE_FNO"
    assert bse_result["SENSEX"].lot_size == 10
    assert "SENSEX" not in cross_result
