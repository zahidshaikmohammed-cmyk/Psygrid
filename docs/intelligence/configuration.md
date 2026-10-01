# Configuration

All settings are environment variables with safe defaults; the service needs
none of them. On the VM, put overrides in `/etc/psygrid-intelligence.env`
(read by the systemd unit if present) and run
`sudo systemctl restart psygrid-intelligence`. Out-of-range numbers are
clamped; unparseable ones fall back to the default.

| Variable | Default | Meaning |
| --- | --- | --- |
| `PSYGRID_ARCHIVE_DIR` | `~/psygrid-data` | The daily archive PSYGRID writes (read only). Must match PSYGRID's setting |
| `PSYGRID_INTELLIGENCE_DIR` | `~/psygrid-intelligence` | Event store, keys, caches, derivatives snapshots, backups. If changed, also change `ReadWritePaths` in the unit |
| `PSYGRID_INTELLIGENCE_SOURCE_URL` | `http://127.0.0.1:10000` | PSYGRID, for the read-only derivatives endpoints |
| `PSYGRID_INTELLIGENCE_HOST` | `127.0.0.1` | Bind address. `0.0.0.0` to accept remote connections (see deployment.md) |
| `PSYGRID_INTELLIGENCE_PORT` | `10001` | Port |
| `PSYGRID_INTELLIGENCE_REQUIRE_KEYS` | `true` | Require an API key on every data route. `false` only for a closed, local setup |
| `PSYGRID_INTELLIGENCE_RATE_PER_MINUTE` | `120` | Default sustained requests per minute per key |
| `PSYGRID_INTELLIGENCE_RATE_BURST` | `30` | Bucket size per key |
| `PSYGRID_INTELLIGENCE_MAX_STREAMS` | `50` | WebSocket streams in total |
| `PSYGRID_INTELLIGENCE_MAX_STREAMS_PER_KEY` | `5` | WebSocket streams per key |
| `PSYGRID_INTELLIGENCE_LIVE` | `true` | Run the live runner. `false` serves only what the store holds |
| `PSYGRID_INTELLIGENCE_RECORD_DERIVATIVES` | `true` | Record derivatives snapshots once a minute in session |
| `PSYGRID_INTELLIGENCE_SIMILARITY_SESSIONS` | `60` | Earlier sessions searched by similarity (5–1000) |
| `PSYGRID_INTELLIGENCE_BACKUP_KEEP` | `14` | Nightly backups kept |

Engine constants (thresholds, windows) are code, not configuration, so that
an event's meaning is fixed by its `engine_version`. They are listed in
[features.md](features.md) and [event-schema.md](event-schema.md).

## Files under `PSYGRID_INTELLIGENCE_DIR`

| Path | Content |
| --- | --- |
| `events.db` (+ `-wal`, `-shm`) | The event store (SQLite, WAL) |
| `api_keys.json` | Key hashes and metadata, mode 0600 |
| `derivatives/<date>.jsonl` | One derivatives snapshot per minute |
| `summaries/<date>.v3.npz` | Per-session baseline summaries (rebuildable) |
| `states/<date>.v1/` | Per-session similarity states (rebuildable) |
| `backups/` | Nightly `events-<date>.db` and `api_keys-<date>.json` |
