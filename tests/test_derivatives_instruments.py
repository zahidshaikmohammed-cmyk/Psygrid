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
