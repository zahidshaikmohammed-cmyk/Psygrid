# Performance and resource requirements

Measured with `python tools/bench_intelligence.py` on 2026-10-01: 21 synthetic
sessions of the full 989-stock universe, written through the real archive
writer, with the last session replayed minute by minute through every engine.
The machine was a 4-vCPU, 16 GB x86 container, not the production VM; run the
tool on the VM (`python tools/bench_intelligence.py`, about 5 minutes,
self-cleaning) to measure there. Raw output:
[`benchmark-2026-10-01.json`](benchmark-2026-10-01.json).

## Per-minute engine step (989 instruments)

| Stage | Mean ms |
| --- | --- |
| Data quality | 18.3 |
| Features | 8.3 |
| Anomalies | 2.8 |
| Relationships (sector leave-one-out, index, sector index, sector pairs, price/volume, derivatives) | 109.0 |
| Events (build, cooldown, store) | 4.8 |
| **Total** | **143.1** (p50 149, p95 182, max 256) |

A minute's step costs about 0.15 s of one core: under 0.3% of the minute.
A full session (361 steps) replays in 55 s. Live, the runner processes the
five minutes covered by each archive write in under a second.

## One-off and daily costs

| Operation | Time | When |
| --- | --- | --- |
| Load one archived day (989 stocks) | 3.4 s | Each archive write in session; each replay |
| Baselines, cold (summarise 20 sessions) | 76 s | First start only; then once per new session (about 4 s) |
| Baselines, cached | 5.5 s | Once per session, at the first step |
| Similarity, cold (build states for 20 sessions) | 68 s | Done in the background after the close (3 sessions a minute) |
| Similarity search, cached states | 0.1 s | Per request; results also cached per minute |

## API latency (in-process, p50 / max over 20 requests)

| Route | p50 ms | max ms |
| --- | --- | --- |
| `/v2/market` | 4.9 | 11.1 |
| `/v2/instruments/TCS` | 3.7 | 15.1 |
| `/v2/anomalies` | 5.1 | 6.4 |
| `/v2/relationships` | 17.4 | 20.1 |
| `/v2/events?limit=100` | 19.1 | 46.4 |
| `/v2/observations?limit=1000` | 93.9 | 119.0 |
| `/v2/historical-matches/market` (cached) | 3.6 | 114.6 (first call) |

## Event volume

On the benchmark session (a synthetic market with three injected anomalies
and otherwise pure noise), the engine emitted 96 events. All three injected
anomalies were among them; the rest are what chance produces at the
documented thresholds:

| Type | Count |
| --- | --- |
| `sector_spread_shift` | 59 |
| `volume_surge`, `volume_drought` | 6, 6 |
| `return_shock` | 5 |
| `volume_without_move`, `move_without_volume` | 5, 3 |
| `sector_divergence`, `index_divergence`, `sector_index_divergence` | 4, 4, 4 |

Before the statistics were corrected during this benchmark (beta
uncertainty in divergences, the per-minute spread variance for sector pairs,
and |z| ≥ 5 for every instrument-scope event), the same session produced 919
events. Real markets have heavier tails and genuine co-movement shocks, so
expect more than this on real data; `min_severity` filters on the API and the
stream let consumers choose their own volume.

## Resource requirements

| Resource | Need | Unit limit |
| --- | --- | --- |
| CPU | ~0.15 s per minute in session; bursts of one core for 1–2 minutes at first start and after the close | `CPUQuota=100%`, `Nice=10`, `CPUWeight=20` |
| Memory | 606 MB peak (baselines for 989 stocks plus a day's arrays) | `MemoryHigh=900M`, `MemoryMax=1200M` |
| Disk, per session | baseline summary 3.5 MB, similarity states 2.5 MB, events under 1 MB, derivatives snapshots about 0.4 MB | — |
| Disk, per year (~250 sessions) | about 1.9 GB of rebuildable caches plus about 0.3 GB of events and snapshots; 14 nightly backups | — |
| Network | five localhost requests a minute in session | — |

For comparison, PSYGRID's own archive is about 6 MB a session. If the VM has
less than about 1.5 GB of free memory, lower `MemoryMax`: the service then
restarts rather than competing with PSYGRID, which the higher OOM score
protects in any case.
