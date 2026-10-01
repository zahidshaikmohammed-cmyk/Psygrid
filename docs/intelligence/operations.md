# Operations

## Monitoring

`GET /v2/health` (no key):

| Field | Healthy value | Meaning |
| --- | --- | --- |
| `status` | `OK` | `DEGRADED` if the store fails, the live state is `STALE`, or no data has ever arrived and the last tick failed |
| `live.state` | `LIVE` in session, `CLOSED` outside | `STALE`: in session, but no new minute processed for 10 minutes (archive not moving); `WAITING_FOR_DATA`: nothing processed yet |
| `live.lag_seconds` | ≤ ~360 in session | Seconds from the last processed minute to now (archive interval plus one minute) |
| `live.errors`, `last_error` | stable | Failures counted per tick, with the latest message |
| `live.derivatives_errors` | stable | Failed derivatives fetches (one per endpoint per minute) |
| `live.last_backup` | today's file after 15:45 | Nightly backup path |
| `live.last_step_ms` | `total` well under 60 000 | Time per stage of the latest step |
| `event_store.events` | grows on trading days | |

Logs: `journalctl -u psygrid-intelligence -f`. The access log is one JSON
line per request (method, path without query string, status, ms, key id,
client).

## Troubleshooting

| Symptom | Likely cause | Action |
| --- | --- | --- |
| `/v2/*` answers `503 no intelligence computed yet` | Just started, or no archived session | Wait for the first catch-up (tens of seconds); check `ls ~/psygrid-data` |
| `live.state = STALE` in session | PSYGRID is not writing the archive | Check PSYGRID: `/health`, `archive` component, `journalctl -u psygrid`. Intelligence recovers on the next write |
| `derivatives_errors` rising | PSYGRID endpoints unavailable or market paused | Check `curl 127.0.0.1:10000/public/nifty-futures.json`. Derivatives relationships show `INSUFFICIENT_DATA` until snapshots resume; nothing else is affected |
| `401` on every request | Wrong, revoked or missing key | `python -m intelligence keys list`; create a new key |
| `429` | Rate limit | Honour `Retry-After`; raise the key's rate with a new key (`--rate-per-minute`) |
| `live.state = WAITING_FOR_DATA` for long | No archived session at all | Run the history bootstrap ([history-bootstrap.md](history-bootstrap.md)) |
| Similarity `INSUFFICIENT_HISTORY` | Fewer than 5 earlier archived sessions | Run the history bootstrap, or wait for the archive to grow |
| Anomalies use `INTRADAY` / `CROSS_SECTIONAL` baselines | Fewer than 5 earlier sessions | Expected while the archive is young; events carry `BASELINE_*` flags |
| Service restarts repeatedly | Import error after a bad deploy, or memory ceiling | `journalctl -u psygrid-intelligence -n 100`; `systemctl status` shows `oom-kill` if `MemoryMax` was hit |
| Disk growth | Caches and backups | Caches are rebuildable: delete `summaries/` and `states/` freely; backups are pruned to `BACKUP_KEEP` |

## Backups and restore

After 15:45 IST each day with events, the service copies the event store
(SQLite online backup) and the key file to
`~/psygrid-intelligence/backups/`, keeping the newest 14. On demand:
`python -m intelligence backup`. To restore:

```bash
sudo systemctl stop psygrid-intelligence
cp ~/psygrid-intelligence/backups/events-2026-10-20.db ~/psygrid-intelligence/events.db
rm -f ~/psygrid-intelligence/events.db-wal ~/psygrid-intelligence/events.db-shm
sudo systemctl start psygrid-intelligence
```

Events after the backup are regenerated from the archive by replay
(`python -m intelligence run <date>`), with the same ids. Copy backups off
the VM for protection against loss of the VM itself.

## Recovery behaviour

- A failure in one tick (fetch, archive read, engine step, backup) is logged
  and counted; the next minute retries. `Restart=always` covers crashes.
- On restart, the runner replays the latest session from its open; ids
  already stored are skipped and the cooldown state is read from the store,
  so no event is emitted twice.
- A torn last line in a derivatives file (crash mid-write) is skipped on read.
- Cache files are written atomically (temporary file, then rename).
