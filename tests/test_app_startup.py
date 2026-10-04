"""app.startup() wires every manager, and app.shutdown() releases them.

External effects are mocked: Dhan, the instrument master and every manager's
start(). The real index derivatives and archive managers are constructed so
their wiring (specs, hooks) is checked, not just their presence.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import app as app_module
from config import Instrument, Settings
from daily_archive import ArchiveManager
from index_depth import IndexDepthManager
from index_options import INDEX_DERIVATIVES, IndexOptionsManager

REAL = {"IndexOptionsManager", "IndexDepthManager", "ArchiveManager", "DailyArchive", "archive_dir_from_environment"}
EXPECTED_GLOBALS = (
    "state",
    "manager",
    "index_manager",
    "indicator_runtime",
    "archive_manager",
    "midcpnifty_underlying_manager",
    "nifty_underlying_indicators",
    "banknifty_underlying_indicators",
    "midcpnifty_underlying_indicators",
    "sensex_underlying_indicators",
    "nifty_futures_manager",
    "banknifty_futures_manager",
    "sensex_futures_manager",
    "stock_options_manager",
    "stock_depth_manager",
    "global_context_manager",
    "rbi_news_manager",
)


@pytest.fixture
def started(monkeypatch, tmp_path):
    monkeypatch.setenv("PSYGRID_ARCHIVE_DIR", str(tmp_path))
    monkeypatch.setenv("PSYGRID_ARCHIVE", "1")
    # startup() assigns module globals; restore them afterwards so no other test sees this run.
    assigned = set(app_module.startup.__code__.co_names) | {
        f"{s.key}_{kind}_manager" for s in INDEX_DERIVATIVES for kind in ("options", "depth")
    }
    for name in assigned:
        if name in vars(app_module) and not callable(getattr(app_module, name)):
            monkeypatch.setattr(app_module, name, getattr(app_module, name))
    settings = Settings(client_id="x", access_token="x", max_instruments=2)
    instruments = [Instrument(symbol="AAA", security_id="1"), Instrument(symbol="BBB", security_id="2")]
    names = app_module.startup.__code__.co_names
    mocks = {
        name: MagicMock(name=name)
        for name in names
        if name in vars(app_module) and isinstance(getattr(app_module, name), type) and name not in REAL
    }
    mocks.update(
        load_settings=MagicMock(return_value=settings),
        load_instruments=MagicMock(return_value=instruments),
        DhanAPI=MagicMock(return_value=SimpleNamespace(settings=settings)),
    )
    with (
        patch.multiple(app_module, **mocks),
        patch.object(IndexOptionsManager, "start"),
        patch.object(IndexDepthManager, "start"),
        patch.object(ArchiveManager, "start"),
    ):
        app_module.startup()
        yield mocks
        app_module.shutdown()


def test_startup_succeeds_and_sets_every_manager(started):
    assert app_module.config_error == ""
    for name in EXPECTED_GLOBALS:
        assert getattr(app_module, name) is not None, name


def test_index_derivatives_are_wired_per_spec(started):
    for spec in INDEX_DERIVATIVES:
        options = getattr(app_module, f"{spec.key}_options_manager")
        depth = getattr(app_module, f"{spec.key}_depth_manager")
        assert isinstance(options, IndexOptionsManager) and options.spec == spec
        assert isinstance(depth, IndexDepthManager) and depth.spec == spec
        assert depth.option_manager is options


def test_archive_hooks_are_attached(started, tmp_path):
    archive = app_module.archive_manager
    assert archive.archive.root == tmp_path
    assert app_module.manager.on_session_end == archive.archive_equity
    assert app_module.index_manager.on_session_end == archive.archive_indices


def test_startup_reports_a_universe_mismatch_instead_of_raising(started):
    with patch.object(app_module, "load_instruments", return_value=[]):
        app_module.startup()
    assert "Universe integrity failure" in app_module.config_error


def test_shutdown_releases_managers(started):
    app_module.shutdown()
    for name in ("manager", "index_manager", "archive_manager", "nifty_options_manager", "stock_options_manager"):
        assert getattr(app_module, name) is None, name


def test_live_only_by_default_nothing_is_archived(started, monkeypatch, tmp_path):
    monkeypatch.delenv("PSYGRID_ARCHIVE", raising=False)
    monkeypatch.delenv("PSYGRID_MICROSTRUCTURE", raising=False)
    app_module.shutdown()
    app_module.startup()
    assert app_module.config_error == ""
    assert app_module.archive_manager is None
    assert app_module.microstructure_recorder is None
    assert list(tmp_path.iterdir()) == []
