from index_layer import INDEX_FALLBACK_IDS, INDEX_SPECS, _resolve_one

def test_index_resolution_has_hard_fallbacks_for_core_indices():
    for key in ("nifty", "banknifty", "sensex", "nifty500", "finnifty", "indiavix", "niftyit"):
        instrument, error = _resolve_one(key)
        assert instrument is not None
        assert error == ""
        assert instrument.security_id == INDEX_FALLBACK_IDS[key]
        assert instrument.exchange_segment == "IDX_I"

def test_index_specs_are_independent():
    assert len(INDEX_SPECS) == 16
    assert set(INDEX_FALLBACK_IDS) < set(INDEX_SPECS)
