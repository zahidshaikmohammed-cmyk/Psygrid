# Event schema, version 1 (draft for review)

An event is one measurable, unusual observation about the market, stated with
the evidence that makes it reproducible. It never says what to trade: it says
what was measured, against what baseline, how unusual that is, and how good
the underlying data was.

Status: **draft**. No detector emits events yet; this is the contract the
first detectors (Phase 3) will be built against. Comments and changes are
expected before it is frozen.

## Example

```json
{
  "schema_version": "event/1",
  "event_id": "evt_3f9a1c0b7d2e4a68",
  "event_type": "volume_surge",
  "category": "ANOMALY",
  "session_date": "2026-10-05",
  "observed_at": "2026-10-05 10:17:00 IST",
  "bar_time": "2026-10-05 10:16:00 IST",
  "scope": "INSTRUMENT",
  "subject": {"key": "RELIANCE", "kind": "EQUITY"},
  "magnitude": {"statistic": "robust_z", "value": 6.4, "threshold": 4.0},
  "severity": "HIGH",
  "novelty": {"lookback_sessions": 20, "prior_occurrences": 0},
  "evidence": {
    "observation": {"volume": 182400},
    "baseline": {
      "method": "median and MAD of the same minute-of-day",
      "window_sessions": 20,
      "sample_size": 20,
      "median": 21050,
      "mad": 6300
    },
    "context": {"sector": "ENERGY", "sector_median_robust_z": 0.8}
  },
  "affected_instruments": ["RELIANCE"],
  "relationships": [],
  "historical_context": null,
  "data_quality": {
    "subject_status": "COMPLETE",
    "subject_missing_bars": 0,
    "frame_coverage": 0.981,
    "baseline_sessions_complete": 20,
    "flags": []
  },
  "provenance": {
    "engine": "anomaly.volume_surge",
    "engine_version": "1.0.0",
    "feature_versions": {"volume_by_minute": "1"},
    "source": "ARCHIVE_REPLAY"
  },
  "synthetic_data": false
}
```

## Fields

| Field | Type | Meaning |
| --- | --- | --- |
| `schema_version` | string | `event/1`. Independent of the API version; stored events outlive API releases. |
| `event_id` | string | Deterministic: `evt_` + the first 16 hex characters of SHA-256 over `event_type`, `subject.key`, `bar_time` and `provenance.engine_version`. The same input replayed gives the same id. |
| `event_type` | string | snake_case name from the event catalogue (below). |
| `category` | enum | `ANOMALY`, `RELATIONSHIP`, `BREADTH`, `DISPERSION`, `REGIME`. |
| `session_date` | string | `YYYY-MM-DD`. |
| `observed_at` | string | When the event became knowable: the frame's `as_of`, `YYYY-MM-DD HH:MM:SS IST`. |
| `bar_time` | string | Open time of the latest bar the evidence uses. Always before `observed_at`. |
| `scope` | enum | `INSTRUMENT`, `SECTOR`, `INDEX`, `MARKET`. |
| `subject` | object | `key` (symbol, sector or index key) and `kind` (`EQUITY`, `INDEX`, `SECTOR`, `MARKET`). |
| `magnitude` | object | The one statistic the event is about: its name, value and the threshold it crossed. |
| `severity` | enum | `LOW`, `MEDIUM`, `HIGH`, from fixed bands of `magnitude.value / magnitude.threshold` (below 1.5, below 2.5, otherwise), never from judgement. |
| `novelty` | object | How many times the same `event_type` fired for the same subject in the lookback window. |
| `evidence` | object | The measured `observation`, the `baseline` it was compared with (method, window, sample size and its statistics) and any `context` comparisons (sector, index, peers). Enough to recompute `magnitude` by hand. |
| `affected_instruments` | array | Keys of every instrument the event involves. |
| `relationships` | array | For `RELATIONSHIP` events: the pair, the baseline co-movement and the current one. Empty otherwise. |
| `historical_context` | object or null | Phase 6: similar past states and the distribution of what followed. Null until then. |
| `data_quality` | object | Quality of the subject's data in the frame, the frame's coverage, how many baseline sessions were complete, and `flags` (for example `SUBJECT_HAS_GAPS`, `BASELINE_SHORT`). |
| `provenance` | object | Engine name and version, versions of the features used, and `source`: `ARCHIVE_REPLAY` or `LIVE_SNAPSHOT`. |
| `synthetic_data` | bool | Always `false`. |

## Rules

1. **Evidence, not advice.** No field or event type expresses a trading view. Words like bullish, bearish, buy or sell never appear; a measured direction is a signed number in `evidence`.
2. **Reproducible.** An event is a pure function of the frame and the archive before it. Replaying the same day with the same engine versions yields the same events with the same ids.
3. **No look-ahead.** Every value in an event comes from bars completed by `observed_at`.
4. **Quality gates.** A detector never fires for a subject whose status is `NO_DATA`. If the subject has gaps or the baseline is shorter than its window, the event may still fire but carries the matching flag.
5. **Append-only.** Stored events are never edited. A correction is a new event with `supersedes: <event_id>`.
6. **One statistic per event.** An event is about one measurement. A finding that needs several becomes several events that share `affected_instruments` and `bar_time`.

## First event catalogue (Phase 3 candidates)

| `event_type` | Category | Measures |
| --- | --- | --- |
| `return_shock` | ANOMALY | 1m or 5m return vs the instrument's own minute-of-day distribution |
| `volume_surge` | ANOMALY | Volume vs the same minute-of-day baseline |
| `range_expansion` | ANOMALY | Bar range vs recent and minute-of-day range |
| `sector_divergence` | RELATIONSHIP | Instrument return vs its sector median, against the usual spread |
| `index_divergence` | RELATIONSHIP | Instrument return vs its index, against the usual spread |
| `breadth_shift` | BREADTH | Change in advancers minus decliners vs its usual change at that time |
| `dispersion_jump` | DISPERSION | Cross-sectional return dispersion vs its usual level at that time |

## Open questions

1. Severity bands: are three fixed bands right, or should severity be the percentile of `magnitude` among past events of the same type?
2. Baseline window: 20 sessions is a starting point. Robust statistics need history; until the archive has 20 sessions, should events fire with a `BASELINE_SHORT` flag or not at all?
3. Sector source: `sector_taxonomy.sector_for_symbol` is hand-maintained (RELIANCE maps to `ENERGY`). Is it the source of truth for `context.sector` and `SECTOR` scope, or should the NSE sector classification be adopted?
4. Should the id include `observed_at` as well as `bar_time`, so a live event and its replay twin differ? (Proposed: no; the same evidence should give the same id.)
