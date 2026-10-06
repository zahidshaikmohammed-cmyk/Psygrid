"""The full PSYGRID shares its Dhan token only with the Live Core nodes' private IPs."""

from __future__ import annotations

from types import SimpleNamespace

from token_share import allowed_clients, token_share

TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJleHAiOjQxMDI0NDQ4MDB9.c2lnbmF0dXJlLW5vdC1yZWFs"
SETTINGS = SimpleNamespace(client_id="1100000001", access_token=TOKEN, token_source="AUTO_GENERATED_TOTP")
NODES = allowed_clients("10.0.0.215,10.0.0.165")


def test_only_private_non_loopback_addresses_can_be_allowed():
    assert allowed_clients("10.0.0.215, 10.0.0.165;127.0.0.1,129.225.112.47,0.0.0.0/0,junk") == frozenset(
        {"10.0.0.215", "10.0.0.165"}
    )
    assert allowed_clients("") == frozenset()


def test_disabled_without_an_allowlist():
    assert token_share(SETTINGS, "10.0.0.215", {}, frozenset()) == (404, {"status": "NOT_FOUND"})


def test_an_allowed_node_gets_the_current_token():
    status, payload = token_share(SETTINGS, "10.0.0.165", {}, NODES)
    assert status == 200
    assert payload["access_token"] == TOKEN and payload["client_id"] == "1100000001"


def test_everyone_else_is_refused_without_the_token():
    for host in ("10.0.0.10", "129.225.112.47", "127.0.0.1", None):
        status, payload = token_share(SETTINGS, host, {}, NODES)
        assert status == 403 and TOKEN not in str(payload)


def test_a_request_relayed_by_a_proxy_is_refused_even_from_an_allowed_address():
    for header in ("x-forwarded-for", "x-real-ip", "forwarded"):
        status, payload = token_share(SETTINGS, "10.0.0.215", {header: "10.0.0.215"}, NODES)
        assert status == 403 and TOKEN not in str(payload)


def test_no_token_yet_is_503_not_an_empty_token():
    pending = SimpleNamespace(client_id="1100000001", access_token="", token_source="TOTP_PENDING")
    assert token_share(pending, "10.0.0.215", {}, NODES) == (503, {"status": "TOKEN_NOT_READY"})
    assert token_share(None, "10.0.0.215", {}, NODES) == (503, {"status": "TOKEN_NOT_READY"})
