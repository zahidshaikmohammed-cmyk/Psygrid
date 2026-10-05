"""Resource guarantees for a 1 GB node: RAM-only data, bounded memory, minimal imports, isolated deploy."""

import os
import shutil
import subprocess
import sys
import threading
import tracemalloc
from pathlib import Path

import orjson
import pytest
from fastapi.testclient import TestClient
from live_core_helpers import feed_minutes, ist

from live_core.api import create_app
from live_core.render import assemble, local_fragments
from live_core.state import SOURCE_WEBSOCKET, NodeState

ROOT = Path(__file__).resolve().parents[2]
SESSION_MINUTES = 360  # 09:15-15:15


def _code_lines(text: str) -> str:
    """The text without comment lines, so explanatory comments may name what the code must not touch."""
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))


def _manifest() -> list[str]:
    lines = (ROOT / "deploy" / "live-core" / "MANIFEST").read_text(encoding="utf-8").splitlines()
    return [line.strip() for line in lines if line.strip() and not line.strip().startswith("#")]


class _DiskWriteRecorder:
    """sys.addaudithook cannot be removed, so the hook is installed once and switched on per test."""

    active = False
    writes: list = []  # noqa: RUF012
    installed = False

    @classmethod
    def hook(cls, event, args):
        if not cls.active:
            return
        if event == "open":
            path, mode, flags = [*args, None, None, None][:3]
            writing = (isinstance(mode, str) and any(c in mode for c in "wax+")) or (
                isinstance(flags, int) and flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_APPEND)
            )
            if writing and path not in (None, os.devnull) and not isinstance(path, int):
                cls.writes.append((event, path))
        elif event in {"os.rename", "os.replace", "os.mkdir", "os.remove", "shutil.copyfile", "os.truncate"}:
            cls.writes.append((event, args[0] if args else None))


def _record_disk_writes():
    if not _DiskWriteRecorder.installed:
        sys.addaudithook(_DiskWriteRecorder.hook)
        _DiskWriteRecorder.installed = True
    _DiskWriteRecorder.writes = []
    return _DiskWriteRecorder


def test_a_whole_session_writes_no_market_data_to_disk(make_runtime, universe):
    recorder = _record_disk_writes()
    recorder.active = True
    probe = Path(os.environ.get("TMPDIR", "/tmp")) / f"live-core-audit-probe-{os.getpid()}"
    try:
        probe.write_bytes(b"x")  # proves the recorder sees writes before trusting an empty result
        probe.unlink()
        assert any(str(probe) == str(path) for _, path in recorder.writes), recorder.writes
        recorder.writes = []
        runtime = make_runtime(0, node_count=1, when=ist(9, 15))
        runtime.test_api.history = {
            universe.symbols[0]: [
                {"timestamp": int(ist(9, 15).timestamp()), "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1}
            ]
        }
        runtime.tick()
        ids = [series.security_id for series in runtime.state.ordered]
        runtime.test_clock.set(ist(9, 30))
        feed_minutes(runtime.state, ids, int(ist(9, 15).timestamp()), 5)
        runtime.history.enqueue_all()
        runtime.tick()
        client = TestClient(create_app(runtime, start_runtime=False))
        for path in ("/public/live.json", "/public/live-a.json", "/public/stock/TCS.json", "/health", "/health/node"):
            assert client.get(path, headers={"Accept-Encoding": "gzip"}).status_code == 200
        runtime.test_clock.set(ist(15, 15))
        runtime.tick()
    finally:
        recorder.active = False
    assert recorder.writes == []


def test_live_core_never_imports_archive_or_heavy_modules():
    code = (
        "import sys; import live_core.runtime, live_core.api, live_core.feed, live_core.history; "
        "heavy = sorted(m for m in ('pandas', 'numpy', 'app', 'daily_archive', 'microstructure', 'intelligence', "
        "'index_layer', 'stock_options', 'stock_depth', 'futures_layer', 'indicator_runtime', 'session', "
        "'state', 'backfill') if m in sys.modules); print(heavy)"
    )
    result = subprocess.run([sys.executable, "-c", code], cwd=ROOT, capture_output=True, text=True, check=True)
    assert result.stdout.strip() == "[]"


def test_full_session_partition_fits_in_a_few_megabytes():
    instruments = [type("I", (), {"symbol": f"S{i:03d}", "security_id": str(i)})() for i in range(495)]
    tracemalloc.start()
    try:
        state = NodeState(clock=lambda: 0)
        state.begin("2026-10-05", instruments)
        baseline = tracemalloc.get_traced_memory()[0]
        epoch = int(ist(9, 15).timestamp())
        for series in state.ordered:
            for minute in range(SESSION_MINUTES):
                price = 100.0 + minute * 0.05
                series.put_completed(
                    (epoch + minute * 60, price, price + 1, price - 1, price, 1000 + minute), SOURCE_WEBSOCKET
                )
        candles_bytes = tracemalloc.get_traced_memory()[0] - baseline
        fragments = local_fragments(state, 0, 495)
        rendered_bytes = tracemalloc.get_traced_memory()[0] - baseline - candles_bytes
    finally:
        tracemalloc.stop()
    assert state.memory_summary()["completed_candles_in_ram"] == 495 * SESSION_MINUTES
    # 178,200 candles: ~8.5 MB of columns. A dict per candle (the full app's representation) is ~25x that.
    assert candles_bytes < 15 * 1024 * 1024, candles_bytes
    # Cached JSON for the whole partition (what /public/live.json serves) stays bounded too.
    assert rendered_bytes < 80 * 1024 * 1024, rendered_bytes
    assert sum(len(fragment) for _, _, fragment in fragments) < 40 * 1024 * 1024


def test_full_day_aggregate_live_json_is_assembled_without_reparsing():
    instruments = [type("I", (), {"symbol": f"S{i:03d}", "security_id": str(i)})() for i in range(989)]
    state = NodeState(clock=lambda: 0)
    state.begin("2026-10-05", instruments)
    epoch = int(ist(9, 15).timestamp())
    for series in state.ordered:
        for minute in range(SESSION_MINUTES):
            series.put_completed((epoch + minute * 60, 10.0, 11.0, 9.0, 10.5, 7), SOURCE_WEBSOCKET)
    items = local_fragments(state, 0, 989)
    tracemalloc.start()
    try:
        body = assemble(
            status="OK",
            session_status="LIVE",
            session_date="2026-10-05",
            universe_size=989,
            items=items,
            sort_by_symbol=True,
        )
        peak = tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()
    assert peak < 3 * len(body)  # parts list + one joined body; no per-candle Python objects
    payload = orjson.loads(body)
    assert payload["stock_count"] == 989
    assert len(payload["stocks"]["S000"]["candles_1m"]) == SESSION_MINUTES


def test_runtime_thread_count_is_small(make_runtime):
    before = threading.active_count()
    runtime = make_runtime(0, when=ist(10, 0))
    runtime.tick()  # session + history worker (the fake feed has no thread)
    assert threading.active_count() - before <= 2
    runtime.test_clock.set(ist(15, 15))
    runtime.tick()


def test_the_shipped_manifest_runs_on_its_own(tmp_path):
    files = _manifest()
    assert "app.py" not in files
    assert not any(path.startswith(("intelligence/", "tests/")) for path in files)
    for path in files:
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / path, target)
    env = {k: v for k, v in os.environ.items() if not k.startswith(("LIVE_CORE_", "PSYGRID_"))}
    env["PYTHONPATH"] = str(tmp_path)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    for node_id, expected in (("0", "0 495 495"), ("1", "495 989 494")):
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "from live_core.runtime import build_runtime; import live_core.api; r = build_runtime(); "
                "p = r.partition; print(p.start, p.end, p.size)",
            ],
            cwd=tmp_path,
            env={**env, "LIVE_CORE_NODE_ID": node_id, "LIVE_CORE_NODE_COUNT": "2"},
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == expected
    bad = subprocess.run(
        [sys.executable, "-m", "live_core"],
        cwd=tmp_path,
        env={**env, "LIVE_CORE_NODE_ID": "2", "LIVE_CORE_NODE_COUNT": "2"},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert bad.returncode == 2 and "refused to start" in bad.stderr


def test_the_systemd_unit_is_bounded_and_independent_of_the_full_service():
    result = subprocess.run(
        ["bash", str(ROOT / "deploy" / "live-core" / "install.sh"), "--render-only"],
        env={**os.environ, "NODE_ID": "1", "NODE_COUNT": "2", "APP_USER": "ubuntu"},
        capture_output=True,
        text=True,
        check=True,
    )
    unit = result.stdout
    lines = {
        line.split("=", 1)[0]: line.split("=", 1)[1]
        for line in unit.splitlines()
        if "=" in line and not line.startswith("#")
    }
    assert "@" not in unit.replace("@NODE", "")  # every placeholder rendered
    assert lines["ExecStart"] == "/home/ubuntu/psygrid-live-core/venv/bin/python -m live_core"
    assert lines["User"] == "ubuntu"
    assert lines["Restart"] == "on-failure"
    assert lines["RestartPreventExitStatus"] == "2"
    assert int(lines["StartLimitBurst"]) <= 10 and int(lines["RestartSec"]) >= 5
    assert lines["MemoryMax"] == "800M" and lines["LimitNOFILE"] == "8192"
    assert lines["WatchdogSec"] and lines["ProtectSystem"] == "strict"
    assert lines["WantedBy"] == "multi-user.target"
    assert "LIVE_CORE_NODE_ID=1" in unit
    assert "psygrid.service" not in _code_lines(unit) and "app.py" not in unit
    assert "ReadWritePaths" not in unit  # no writable data directory: RAM only


def test_the_live_core_workflow_deploys_only_the_live_core():
    text = (ROOT / ".github" / "workflows" / "deploy-live-core.yml").read_text(encoding="utf-8")
    code = _code_lines(text)
    triggers = code.split("\non:\n", 1)[1].split("\n\n", 1)[0]
    assert "workflow_dispatch:" in triggers
    assert "push:" not in triggers and "pull_request:" not in triggers  # a restart drops the RAM session
    assert "python app.py" not in code and "app:app" not in code
    assert "systemctl restart psygrid" not in code and "psygrid.service" not in code
    assert "harden_psygrid" not in code and "deploy-oracle" not in code
    assert "140.245.226.102" not in code  # the full PSYGRID VM is never a target
    assert "129.225.112.47" in code
    install = _code_lines((ROOT / "deploy" / "live-core" / "install.sh").read_text(encoding="utf-8"))
    assert "psygrid.service" not in install and "PSYGRID_ARCHIVE" not in install
    assert "systemctl restart psygrid\n" not in install


def test_existing_production_deploy_files_are_untouched():
    text = (ROOT / ".github" / "workflows" / "deploy-oracle.yml").read_text(encoding="utf-8")
    assert "live_core" not in text and "live-core" not in text
    hardening = (ROOT / "deploy" / "psygrid.service.d" / "10-hardening.conf").read_text(encoding="utf-8")
    assert "live" not in hardening.lower()


@pytest.mark.parametrize("node_id", ["0", "1"])
def test_install_script_rejects_a_node_id_outside_the_cluster(node_id):
    result = subprocess.run(
        ["bash", str(ROOT / "deploy" / "live-core" / "install.sh"), "--render-only"],
        env={**os.environ, "NODE_ID": str(int(node_id) + 2), "NODE_COUNT": "2"},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 2


def test_each_node_is_deployed_with_its_own_ssh_key():
    code = _code_lines((ROOT / ".github" / "workflows" / "deploy-live-core.yml").read_text(encoding="utf-8"))
    assert "secrets.LIVE_CORE_NODE0_SSH_KEY" in code and "secrets.LIVE_CORE_NODE1_SSH_KEY" in code
    assert "secrets.LIVE_CORE_SSH_KEY" not in code  # the VMs do not share a key pair
    assert '-i ~/.ssh/live_core_node"${node}"' in code and "IdentitiesOnly=yes" in code
    assert "rm -f ~/.ssh/live_core_node0 ~/.ssh/live_core_node1" in code
    assert "PRIVATE KEY" not in "".join(
        path.read_text(encoding="utf-8", errors="ignore")
        for path in [*ROOT.joinpath("deploy", "live-core").iterdir(), *ROOT.joinpath(".github", "workflows").iterdir()]
        if path.is_file()
    )
