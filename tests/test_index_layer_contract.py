from app import app

EXPECTED_INDEX_ROUTES = [
    "/public/nifty.json",
    "/public/banknifty.json",
    "/public/sensex.json",
    "/public/nifty500.json",
    "/public/niftymidcap100.json",
    "/public/niftysmallcap100.json",
    "/public/finnifty.json",
    "/public/indiavix.json",
    "/public/niftyit.json",
    "/public/niftyauto.json",
    "/public/niftypharma.json",
    "/public/niftymetal.json",
    "/public/niftyfmcg.json",
    "/public/niftyrealty.json",
    "/public/niftyenergy.json",
    "/public/niftyinfra.json",
]

def test_all_original_index_routes_are_registered():
    routes = {route.path for route in app.routes}
    assert set(EXPECTED_INDEX_ROUTES) <= routes

def test_index_layer_is_separate_from_990_routes():
    routes = {route.path for route in app.routes}
    assert "/public/live.json" in routes
    assert "/public/live-a.json" in routes
    assert "/public/live-v.json" in routes
    assert set(EXPECTED_INDEX_ROUTES).isdisjoint({
        "/public/live.json", "/public/live-a.json", "/public/live-v.json"
    })
