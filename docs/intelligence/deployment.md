# Deployment

## What runs where

| Service | Unit | Port | Restarted by a deploy |
| --- | --- | --- | --- |
| PSYGRID (unchanged) | `psygrid` | 10000 | yes, as before |
| Intelligence | `psygrid-intelligence` (`deploy/psygrid-intelligence.service`) | 18101, localhost | yes, after PSYGRID is verified |

## The deploy workflow

`.github/workflows/deploy-oracle.yml` runs on every push to `main`. Its
existing steps are unchanged: lint, the full test suite, the PSYGRID deploy
and restart, `/health`, the 16 index endpoints and the 990-stock universe
check. Only after all of those pass, the new last step:

1. creates `~/psygrid-intelligence`;
2. imports `intelligence.api` with the VM's virtualenv (a broken install fails here, before any restart);
3. installs the unit to `/etc/systemd/system/`, `daemon-reload`, `enable`, `restart psygrid-intelligence`;
4. waits up to 60 s for `GET 127.0.0.1:18101/v2/health`;
5. checks PSYGRID's `/health` again, with the intelligence service running.

The step never restarts or reconfigures `psygrid`. If it fails, PSYGRID is
already deployed and verified; the workflow shows red for the intelligence
step only. Deploy outside market hours (09:15–15:30 IST), as for PSYGRID.

No new Python dependencies: the service uses FastAPI, uvicorn, numpy,
pandas and requests from `requirements.txt`, plus SQLite from the standard
library.

## Resource limits (from the unit)

`Nice=10`, `CPUWeight=20`, `CPUQuota=100%` (at most one core),
`IOSchedulingClass=idle`, `MemoryHigh=900M`, `MemoryMax=1200M`,
`OOMScoreAdjust=500` (the kernel ends this process before PSYGRID),
`TasksMax=64`, `Restart=always`, `RestartSec=10`, `TimeoutStopSec=30`.
Hardening: `NoNewPrivileges`, `PrivateTmp`, `ProtectSystem=full`,
`ProtectHome=read-only` with only `~/psygrid-intelligence` writable.

## First-time setup on the VM

After the first deploy that includes the service:

```bash
cd ~/Psygrid
.venv/bin/python -m intelligence keys create --name "my-engine"   # prints the key once; store it
curl -s http://127.0.0.1:18101/v2/health | jq .
curl -s -H "X-API-Key: psg_..." http://127.0.0.1:18101/v2/market | jq '.as_of'
```

On its first start the service builds per-session summaries for the last 20
archived sessions (about 4 s per session for 989 stocks, once), replays the
latest archived session to fill the event store, and then warms the
similarity cache a few sessions per minute after the close.

## Reaching `/v2` from another machine

By default the service listens on localhost only: engines on the VM use
`http://127.0.0.1:18101`. To expose it, all of these are needed, and none is
done automatically:

1. `PSYGRID_INTELLIGENCE_HOST=0.0.0.0` in `/etc/psygrid-intelligence.env`;
2. an iptables rule and an Oracle security-list rule for TCP 18101;
3. TLS in front (for example Caddy or nginx with a free Let's Encrypt
   certificate): API keys must not cross the internet in clear text;
4. the data-rights questions in [data-rights.md](data-rights.md) answered
   before anyone other than the account holder is given a key.

## Rolling back

`sudo systemctl disable --now psygrid-intelligence` stops the service
without affecting PSYGRID. Its data stays in `~/psygrid-intelligence`;
deleting that directory removes every trace. A code rollback is a revert on
`main`, deployed as usual.
