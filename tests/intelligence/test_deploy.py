"""The intelligence service is deployed beside PSYGRID without touching it."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_unit_is_resource_capped_and_isolated():
    unit = (ROOT / "deploy" / "psygrid-intelligence.service").read_text()
    for setting in ("Nice=10", "CPUQuota=100%", "MemoryMax=1200M", "OOMScoreAdjust=500", "IOSchedulingClass=idle",
                    "NoNewPrivileges=true", "ProtectHome=read-only", "Restart=always", "StartLimitBurst=5"):  # fmt: skip
        assert setting in unit, setting
    assert "-m intelligence serve" in unit


def test_deploy_installs_intelligence_only_after_psygrid_is_verified():
    workflow = (ROOT / ".github" / "workflows" / "deploy-oracle.yml").read_text()
    universe = workflow.index("Verify 990-stock universe")
    intelligence = workflow.index("Deploy intelligence service")
    assert universe < intelligence
    tail = workflow[intelligence:]
    assert "restart psygrid-intelligence" in tail
    assert "restart psygrid\n" not in tail and "stop psygrid\n" not in tail and "stop psygrid " not in tail
    port_check, restart = tail.index("sport = :18101"), tail.index("restart psygrid-intelligence")
    assert port_check < restart  # never start on a port another process holds
