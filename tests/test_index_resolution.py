from index_layer import INDEX_FALLBACK_IDS, INDEX_SPECS, _resolve_one

def test_core_index_resolution_does_not_depend_on_csv_availability():
    for key in ("nifty", "banknifty", "sensex", "nifty500", "finnifty", "indiavix", "niftyit"):
        instrument, error = _resolve_one(key)
        assert instrument is not None
        assert error == ""
        assert instrument.security_id == INDEX_FALLBACK_IDS[key]
        assert instrument.exchange_segment == "IDX_I"

def test_all_original_index_specs_remain_present():
    assert len(INDEX_SPECS) == 16
    assert set(INDEX_FALLBACK_IDS) < set(INDEX_SPECS)
