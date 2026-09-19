from types import SimpleNamespace

from fastapi.testclient import TestClient
import app as app_module

def test_index_layer_failure_is_explicit_not_silent(monkeypatch):
    # A healthy equity layer with a failed index layer must surface the
    # index failure specifically, not the generic "still starting" gate.
    monkeypatch.setattr(app_module, "config_error", "")
    monkeypatch.setattr(app_module, "state", SimpleNamespace())
    monkeypatch.setattr(app_module, "index_manager", None)
    monkeypatch.setattr(app_module, "index_error", "TEST_INDEX_FAILURE")
    client = TestClient(app_module.app)
    response = client.get("/public/nifty.json")
    assert response.status_code == 503
    assert response.json()["status"] == "INDEX_LAYER_UNAVAILABLE"
    assert response.json()["error"] == "TEST_INDEX_FAILURE"
