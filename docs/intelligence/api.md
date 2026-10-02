# `/v2` API

Served by the `psygrid-intelligence` service on port 18101 (localhost by
default). PSYGRID's `/public` API on port 10000 is unchanged and needs no key.
Interactive schema: `GET /v2/docs` and `GET /v2/openapi.json`.

## Authentication and limits

Every route except `/v2/health` and `/v2/ready` needs an API key:

```
X-API-Key: psg_1a2b3c4d_<43 characters>
# or
Authorization: Bearer psg_1a2b3c4d_<43 characters>
```

Keys are created with `python -m intelligence keys create --name <who>` (see
[deployment.md](deployment.md)). Missing, malformed, unknown or revoked keys get
`401`. Each key has a token bucket (default 120 requests a minute, bursts of
30; a key can carry its own rate). Every response carries
`X-RateLimit-Remaining`; an exhausted bucket gets `429` with `Retry-After`.

## Common response fields

Snapshot-based routes start with the header of the minute they describe:

```json
{"session_date": "2026-10-20", "as_of": "2026-10-20 13:00:00 IST", "minute_index": 224,
 "source": "LIVE_SNAPSHOT", "engine_version": "1.0.0", "feature_version": "1",
 "baseline_sessions": 20, "synthetic_data": false}
```

`as_of` is the minute boundary the engine evaluated; `minute_index` is the
0-based column of the latest completed bar (0 = 09:15). Non-finite numbers
are `null`.

## Routes

| Method and path | Purpose | Parameters |
| --- | --- | --- |
| `GET /v2/health` | Public. Service status (`OK` or `DEGRADED`), live runner state (`WAITING_FOR_DATA`, `LIVE`, `STALE`, `CLOSED`), lag, error counts, last backup, event store stats, auth status | — |
| `GET /v2/ready` | Public. `200 {"ready": true}` once a snapshot exists, else `503` | — |
| `GET /v2/meta` | Versions, feature catalogue, anomaly classes, event types, categories, severities | — |
| `GET /v2/market` | Market-wide intelligence: data quality, market features, market measures, anomaly counts, top 25 anomalies, flagged relationships by kind, sector and index features | — |
| `GET /v2/observations` | Per-instrument features and each measure's classification | `keys` (comma list), `sector`, `fields` (comma list of features), `limit` ≤ 1000 (100), `offset` |
| `GET /v2/instruments/{key}` | One instrument: features, anomalies per measure, every relationship involving it, data quality. `404` if not in the universe | — |
| `GET /v2/anomalies` | Scored anomalies, most extreme first, with evidence; plus the market measures | `minimum` = `UNUSUAL` (default) or `EXTREME`, `measure` = `volume`/`return`/`range`, `limit` ≤ 1000 |
| `GET /v2/relationships` | Relationship results, largest \|z\| first | `kind`, `subject`, `flagged_only` (default true), `limit` ≤ 5000 (200) |
| `GET /v2/events` | Event history search, newest first; with `after_seq`, oldest first after that cursor | `date`, `date_from`, `date_to`, `subject`, `instrument`, `event_type`, `category`, `scope`, `min_severity`, `after_seq`, `limit` ≤ 1000 (100) |
| `GET /v2/events/{event_id}` | One event (`evt_` + 16 hex). `404` if unknown | — |
| `GET /v2/historical-matches/market` | Market similarity: matches, outcome distributions vs base rate, uncertainty | `k` 1–50 (10) |
| `GET /v2/historical-matches/instruments/{key}` | The instrument's own earlier sessions | `k` 1–50 (10) |
| `GET /v2/stocks/{key}` | One stock, everything at `as_of`: features and anomalies, expected-response state (expected vs actual move, gap, gap in σ, market/sector/statistical contributions, pending response, delay profile), Full-packet microstructure with cadence support flags, NIFTY derivatives expectation, market-state summary, relationships, today's events, data quality, freshness. The response section reads `WARMING` while the model builds. See [research-engines.md](research-engines.md) | — |
| `GET /v2/stocks` | Stocks ranked by a response measure (largest \|value\| first; stale stocks excluded) | `by` = `response_gap_sigma` (default), `pending_response_sigma`, `response_gap`, `pending_response`; `sector`; `limit` ≤ 1000 (50) |
| `GET /v2/market/state` | Market state (correlation structure, effective dimension, dispersion, breadth, change score, percentiles, regime) and the derivatives expectation for NIFTY, BANKNIFTY and MIDCPNIFTY | — |
| `GET /v2/945` | The latest live 945 decision and its outcome. `404` before the first decision | — |
| `GET /v2/945/decisions` | Stored decisions, newest first, with the 15-minute outcome | `namespace` = `live` (default) or `replay-<version>`; `limit` ≤ 500 (30) |
| `GET /v2/945/decisions/{date}` | One decision (immutable) and its outcome | `namespace` |
| `GET /v2/945/report` | Out-of-sample performance of a namespace's decisions per horizon, with intervals and the verdict | `namespace` |
| `WS /v2/stream` | Live event stream (below) | see below |

Every event returned carries `seq`, its position in the store, usable as a
cursor. Event objects follow [event-schema.md](event-schema.md).

### Example

```bash
KEY=psg_...; BASE=http://127.0.0.1:18101
curl -s -H "X-API-Key: $KEY" "$BASE/v2/market" | jq '.anomaly_counts'
curl -s -H "X-API-Key: $KEY" "$BASE/v2/events?instrument=TCS&min_severity=MEDIUM&limit=20"
curl -s -H "X-API-Key: $KEY" "$BASE/v2/observations?keys=TCS,INFY&fields=ret_15m,rvol_15m"
```

## Errors

| Status | Meaning |
| --- | --- |
| 401 | Missing or invalid API key (`WWW-Authenticate: Bearer`) |
| 404 | Unknown instrument or event |
| 422 | A parameter failed validation (pattern, range, unknown field) |
| 429 | Rate limit exceeded (`Retry-After` seconds) |
| 503 | No snapshot yet (the engine is starting, or nothing is archived), or similarity search busy (`Retry-After: 5`) |

Bodies are `{"detail": ...}`.

## WebSocket stream: `/v2/stream`

```
ws://127.0.0.1:18101/v2/stream?api_key=<key>&after_seq=0&event_types=volume_surge,sector_divergence&instruments=TCS,INFY&min_severity=MEDIUM&snapshots=true
```

The key can also be sent as an `X-API-Key` or `Authorization` header. All
other parameters are optional:

- `after_seq`: replay stored events after this cursor first (default: only new events);
- `event_types`, `instruments` (matched against `affected_instruments` and the subject), `min_severity`: filters;
- `snapshots=false`: no per-minute snapshot messages.

Messages (JSON):

| `type` | When | Content |
| --- | --- | --- |
| `hello` | on connect | `api_version`, `event_schema`, `after_seq` (the starting cursor) |
| `event` | each matching new event, in storage order | `seq`, `event` |
| `snapshot` | each time the engine publishes a new minute | the snapshot header, `anomaly_counts`, `market_measures` |
| `heartbeat` | after 20 s without another message | `latest_seq` |
| `pong` | reply to a `ping` text message | `latest_seq` |

To resume after a disconnect, reconnect with `after_seq` set to the last
`seq` received; nothing is lost while it is in the store. The server polls the
store once a second. Connections are limited (default 50 in total, 5 per
key); beyond that, or with a bad key or parameter, the server closes the
handshake (`1008` policy violation, `1013` try again later).
