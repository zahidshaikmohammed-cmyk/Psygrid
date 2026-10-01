# Architecture

## Two processes, one direction of data

```
                    Dhan (WebSocket, REST)
                             │
             ┌───────────────▼────────────────┐
             │  psygrid  (port 10000)         │   unchanged production service
             │  ingestion, /public/* API      │
             │  daily archive, every 5 min ───┼──► ~/psygrid-data/<date>/*.csv.gz
             └───────────────┬────────────────┘                │
       read-only HTTP, once a│minute (futures, options)        │ read-only files
                             ▼                                 ▼
             ┌────────────────────────────────────────────────────────┐
             │  psygrid-intelligence  (port 10001, own systemd unit)  │
             │  LiveRunner ─► IntelligenceEngine.step(frame)          │
             │    quality → features → anomalies → relationships      │
             │    → events (SQLite) ; similarity on request           │
             │  /v2 REST + /v2/stream WebSocket                       │
             └───────────────────────┬────────────────────────────────┘
                                     ▼
                     ~/psygrid-intelligence/  (events.db, caches, backups)
```

PSYGRID knows nothing about the intelligence service. The service reads the
archive PSYGRID already writes and, once a minute, five small read-only
endpoints (`nifty-futures`, `banknifty-futures`, and three option chains).
It never writes to PSYGRID's directories or calls Dhan. If it crashes, hangs
or runs out of memory, PSYGRID is unaffected: the unit runs at lower CPU and
I/O priority, under a hard memory ceiling, with a higher OOM score.

## Modules (`intelligence/`)

| Module | Role |
| --- | --- |
| `archive.py` | Reads and validates archived days (or the same payloads in memory); rejected rows are kept with a reason |
| `frame.py` | `MarketFrame`: only bars *completed* by `as_of`, on a regular minute grid |
| `quality.py` | Per-instrument and per-frame data quality: complete, gaps, no data, rejected bars |
| `universe.py` | Sector and sector-index mapping from the existing taxonomy |
| `features.py` | 20 instrument features and 8 market features, vectorised over the universe |
| `history.py` | Per-minute summaries of past sessions (cached) and robust per-minute baselines |
| `anomaly.py` | Classification NORMAL / UNUSUAL / EXTREME / INSUFFICIENT_DATA / STALE / INVALID with evidence |
| `derivatives.py` | Per-minute derivatives snapshots: recorder and look-ahead-safe reader |
| `relationships.py` | Stock vs sector / index / sector index, sector vs sector, price vs volume, basis, spread, PCR, IV skew |
| `events.py`, `event_store.py` | Event construction (`event/1`) with cooldown and novelty; append-only SQLite store with search |
| `similarity.py` | State vectors, matching against earlier sessions, outcome distributions vs base rates |
| `engine.py` | `IntelligenceEngine.step(frame)`: the whole pipeline; `Snapshot` views; `replay_session` |
| `live.py` | `LiveRunner`: the minute loop, derivatives recording, archive following, backups, cache warming |
| `api.py`, `keys.py`, `settings.py` | The `/v2` app, API keys and rate limits, configuration |
| `synthetic.py` | Seeded synthetic markets with injected anomalies, for tests and benchmarks only |
| `__main__.py` | CLI: `days`, `replay`, `run`, `serve`, `keys`, `backup` |

## Design rules

1. **Replay and live are the same code.** Live follows the archive minute by
   minute through `IntelligenceEngine.step`; a replay of the same day
   produces the same events with the same ids (tested).
2. **No look-ahead.** A frame contains only bars completed by `as_of`;
   baselines use only earlier sessions; derivatives snapshots are visible
   only once taken; similarity searches only earlier sessions. Tests poison
   every later value and require identical outputs.
3. **Measured, not repaired.** Missing or rejected data is reported, never
   interpolated. An instrument whose latest bar is missing is `STALE`, one
   whose bar was rejected is `INVALID`, and neither is scored.
4. **Robust statistics.** Medians and scaled MADs throughout, so one bad
   print cannot move a baseline.
5. **Evidence, not advice.** Every output carries the value, the baseline,
   its sample size and the method. Nothing says buy, sell, bullish or bearish.
6. **Distributions, not predictions.** Similarity returns what followed
   similar states, beside the base rate, with a separation statistic; below
   five usable sessions it returns no distribution at all.

## Latency

The archive is written every 5 minutes, so live intelligence lags the market
by up to about 5 minutes. This is deliberate: reading the archive costs the
production process nothing. A per-minute path (polling `/public/live.json`,
which serialises the whole universe on every call) would put load on
PSYGRID during the session; it is not used. If lower latency is needed,
the archive interval (`ARCHIVE_INTERVAL_SECONDS` in `daily_archive.py`) is
the lever, and changing it is a production change to be made deliberately.
