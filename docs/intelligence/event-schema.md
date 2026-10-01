# Event schema, version 1

An event is one measurable, unusual observation about the market, stated with
the evidence that makes it reproducible. It never says what to trade: it says
what was measured, against what baseline, how unusual that is, and how good
the underlying data was.

Status: **final** for `event/1` (engine `1.0.0`). Fields may be *added* in a
compatible way; renaming, removing or changing the meaning of a field requires
`event/2`. Produced by `intelligence/events.py`, stored by
`intelligence/event_store.py`.

## Example

```json
{
  "schema_version": "event/1",
  "event_id": "evt_9c62c3c388821e3b",
  "event_type": "volume_surge",
  "category": "ANOMALY",
  "session_date": "2026-09-10",
  "observed_at": "2026-09-10 11:16:00 IST",
  "bar_time": "2026-09-10 11:15:00 IST",
  "bar_epoch": 1789019100,
  "scope": "INSTRUMENT",
  "subject": {"key": "TCS", "kind": "EQUITY"},
  "magnitude": {"statistic": "robust_z_log_volume", "value": 5.22768, "threshold": 3.0},
  "severity": "MEDIUM",
  "classification": "EXTREME",
  "novelty": {"lookback_sessions": 20, "prior_occurrences": 0},
  "evidence": {
    "observation": {"volume": 101141.0, "scored_value": 11.524281},
    "baseline": {
      "method": "robust median and scaled MAD of the same minute of day (+-2 min) over earlier sessions",
      "kind": "HISTORICAL", "median": 9.537628, "scale": 0.380026, "sample": 7
    },
    "context": {"sector": "INFORMATION_TECHNOLOGY", "sector_members_scored": 4, "sector_median_z": 0.394891}
  },
  "affected_instruments": ["TCS"],
  "relationships": [],
  "historical_context": null,
  "data_quality": {
    "subject_status": "COMPLETE", "subject_missing_bars": 0, "frame_coverage": 1.0,
    "baseline_sessions_complete": 7, "flags": ["BASELINE_SHORT"]
  },
  "provenance": {
    "engine": "intelligence.anomaly.volume_surge", "engine_version": "1.0.0",
    "feature_versions": {"features": "1"}, "source": "ARCHIVE_REPLAY"
  },
  "supersedes": null,
  "synthetic_data": false
}
```

`magnitude.value` can be recomputed by hand from the evidence:
`(11.524281 - 9.537628) / 0.380026 = 5.2277`.

## Fields

| Field | Type | Meaning |
| --- | --- | --- |
| `schema_version` | string | `event/1`. Independent of the API version; stored events outlive API releases. |
| `event_id` | string | Deterministic: `evt_` + the first 16 hex characters of SHA-256 over `event_type`, `subject.key`, `subject.counterpart` (or empty), `bar_epoch` and `provenance.engine_version`, joined with `\|`. A live event and its replay twin share an id. |
| `event_type` | string | From the catalogue below. |
| `category` | enum | `ANOMALY`, `RELATIONSHIP`, `DERIVATIVES`, `BREADTH`, `DISPERSION`. |
| `session_date` | string | `YYYY-MM-DD`. |
| `observed_at` | string | When the event became knowable: the frame's `as_of`, `YYYY-MM-DD HH:MM:SS IST`. |
| `bar_time`, `bar_epoch` | string, int | Open time of the latest 1m bar the evidence uses (IST text and Unix seconds). Always before `observed_at`. |
| `scope` | enum | `INSTRUMENT`, `SECTOR`, `INDEX`, `MARKET`. |
| `subject` | object | `key` and `kind` (`EQUITY`, `SECTOR`, `INDEX`, `MARKET`); for a pair, also `counterpart` and `counterpart_kind`. |
| `magnitude` | object | The one statistic the event is about: `statistic` name, signed `value`, and the `threshold` its absolute value crossed (3.0). |
| `severity` | enum | `LOW`, `MEDIUM`, `HIGH` from `abs(value) / threshold`: below 1.5, below 2.5, otherwise (that is, \|z\| < 4.5, < 7.5, ≥ 7.5). Fixed bands, never judgement. |
| `classification` | enum | `UNUSUAL` (\|z\| ≥ 3) or `EXTREME` (\|z\| ≥ 5), as in the anomaly engine. |
| `novelty` | object | `prior_occurrences`: times the same `event_type` fired for the same subject in the last `lookback_sessions` sessions held by the store, strictly before this session. |
| `evidence` | object | `observation` (the measured values), `baseline` (method, kind, centre, scale, sample size) and optional `context` (sector comparison). Enough to recompute `magnitude`. |
| `affected_instruments` | array | Equity keys the event involves (all members of both sectors for a sector pair; empty for index and market events). Searchable. |
| `relationships` | array | For relationship and derivatives events: the pair, the fitted beta and correlation, the residual statistics and both sides' moves. Empty otherwise. |
| `historical_context` | object or null | Reserved for similar past states; events are emitted with `null`. Similarity is served separately by `/v2/historical-matches`. |
| `data_quality` | object | The subject's status (`COMPLETE`, `GAPS`) and missing bars (equity subjects only), the frame's coverage, the number of earlier sessions in the baselines, and `flags`. |
| `provenance` | object | Engine name and version, feature versions, and `source`: `ARCHIVE_REPLAY` or `LIVE_SNAPSHOT`. |
| `supersedes` | string or null | The id of an event this one corrects. The engine never edits stored events. |
| `synthetic_data` | bool | `true` only when the archive day was generated by `intelligence.synthetic` (tests and benchmarks). Always `false` for recorded market data. |

### Data-quality flags

| Flag | Meaning |
| --- | --- |
| `SUBJECT_HAS_GAPS` | The subject is missing at least one completed bar today. |
| `BASELINE_SHORT` | A historical baseline was used, but with fewer than 20 earlier sessions. |
| `BASELINE_INTRADAY` | No usable history; the instrument's own earlier minutes today were the baseline. |
| `BASELINE_CROSS_SECTIONAL` | No usable history; every instrument at the same minute was the baseline. |

## Rules

1. **Evidence, not advice.** No field or event type expresses a trading view; a direction is a signed number.
2. **Reproducible.** An event is a pure function of the frame, the archive before it and the store's earlier sessions. Replaying a day with the same engine version and the same frame cadence yields the same events with the same ids.
3. **No look-ahead.** Every value comes from bars completed, and derivatives snapshots taken, by `observed_at`.
4. **Quality gates.** Nothing fires for an instrument whose latest bar is missing (`STALE`), was rejected (`INVALID`), or has no usable baseline (`INSUFFICIENT_DATA`). Weaker evidence fires with the matching flag.
5. **Append-only.** Stored events are never edited; a correction is a new event naming the one it `supersedes`. Storing an id twice is a no-op.
6. **One statistic per event.** A finding that needs several statistics becomes several events sharing `affected_instruments` and `bar_time`.
7. **Cooldown.** After an event type fires for a subject (and counterpart), it fires again within 15 minutes only if its severity rises. The cooldown state is read back from the store, so a restart does not repeat events.

## Event catalogue

| `event_type` | Category | Scope | Statistic | Fires when |
| --- | --- | --- | --- | --- |
| `volume_surge` / `volume_drought` | ANOMALY | INSTRUMENT | `robust_z_log_volume` | 1m log volume vs its minute-of-day baseline (or earlier minutes today) is ≥ +3 / ≤ −3 |
| `return_shock` | ANOMALY | INSTRUMENT | `return_sigma_z` | \|1m return\| ≥ 3 typical 1m moves at that minute of day (or vs the cross-section) |
| `range_expansion` / `range_compression` | ANOMALY | INSTRUMENT | `robust_z_log_range` | 1m high-low range vs its minute-of-day baseline is ≥ +3 / ≤ −3 |
| `sector_divergence` | RELATIONSHIP | INSTRUMENT | `residual_z` | 15-minute cumulative residual vs the leave-one-out sector median, given beta and correlation ≥ 0.3 fitted on the 60 minutes before |
| `index_divergence` | RELATIONSHIP | INSTRUMENT | `residual_z` | Same, against NIFTY 500 |
| `sector_index_divergence` | RELATIONSHIP | INSTRUMENT | `residual_z` | Same, against the stock's sector index (e.g. NIFTY IT) |
| `sector_spread_shift` | RELATIONSHIP | SECTOR | `spread_z` | The 15-minute spread between two sectors' median returns vs its mean and SD earlier today |
| `volume_without_move` | RELATIONSHIP | INSTRUMENT | `volume_z` | Volume z ≥ 3 while \|return z\| < 1 |
| `move_without_volume` | RELATIONSHIP | INSTRUMENT | `return_z` | \|Return z\| ≥ 3 while \|volume z\| < 1 |
| `basis_shift` | DERIVATIVES | INDEX | `robust_z` | (futures − spot) / spot vs its earlier values today (≥ 20 snapshots) |
| `futures_spread_shift` | DERIVATIVES | INDEX | `robust_z` | Futures top-of-book spread vs its earlier values today |
| `pcr_shift` | DERIVATIVES | INDEX | `robust_z` | Option put-call OI ratio vs its earlier values today |
| `iv_skew_shift` | DERIVATIVES | INDEX | `robust_z` | Option IV skew vs its earlier values today |
| `breadth_shift` | BREADTH | MARKET | `robust_z` | Advancers minus decliners (as a share) vs its minute-of-day baseline |
| `dispersion_shift` | DISPERSION | MARKET | `robust_z` | Cross-sectional dispersion of 15m returns vs its minute-of-day baseline |

## Decisions on the draft's open questions

1. **Severity**: three fixed bands of \|z\| / threshold. Percentile-of-past-events severity changes meaning as history grows and is not reproducible across stores; it can be added later as a separate field.
2. **Short history**: events fire with `BASELINE_SHORT` (or the fallback flag) rather than not at all. The baseline kind and sample size are always in the evidence.
3. **Sector source**: `sector_taxonomy.sector_for_symbol`. Symbols it maps to `OTHER` (about three in four of the 989) have no sector, so they get no sector comparisons; this is a documented limitation.
4. **Id and `observed_at`**: the id excludes `observed_at` and `source`, so the same evidence gives the same id live and in replay; whichever is stored first is kept.

## Storage

`<PSYGRID_INTELLIGENCE_DIR>/events.db`, SQLite in WAL mode. The table
`events` holds the full JSON plus indexed columns (`session_date`,
`bar_epoch`, `event_type`, `category`, `scope`, `subject_key`, `counterpart`,
`severity_rank`, `magnitude`, `source`). `event_instruments` maps every
affected instrument to its events. Each row's `rowid` is exposed as `seq`, a
monotonic cursor used for paging and for the event stream.
