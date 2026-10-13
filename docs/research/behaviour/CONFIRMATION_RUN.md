# Confirmation run: integrity safeguards, resources and safety (prepared, NOT yet launched)

The run executes the frozen protocol in `PREREGISTRATION.md` without changing it. Nothing below changes a hypothesis, threshold, cost or verdict rule. The files listed in `research/behaviour/prereg.lock` are checked before anything is fetched.

## What the run does

1. **Workflow job `behaviour-confirm`** (action `behaviour-confirm`, explicit dispatch only).
   - Refuses inside 08:45–15:45 IST on weekdays.
   - `python -m research.behaviour.lockcheck` stops the job if any pre-registered file differs from its SHA-256.
   - Runs the test suites and checks that the runner has at least 5 GB available.
2. **Node 0 runs `behaviour_fetch.py`** with `START=2021-10-01 END=2026-10-12`.
   - Runs under `systemd-run` at low priority: `CPUQuota=50%`, `Nice=10`, idle I/O, `MemoryMax=150M`, a 2-hour hard limit.
   - Uses the token authority's token on the node; the token never leaves the node.
   - Streams gzip-compressed candles over SSH to the runner. Nothing is written to disk on either side.
3. **The runner runs `research.behaviour.confirm_run`** in RAM:
   - an integrity audit of every session;
   - in-sample → confirmation → contaminated, through the frozen `confirm.prepare` / `run` / `evaluate`.
4. **Committed:** aggregates and integrity summaries only. No candles, no prices, no per-trade rows.
   - The same files go to a 30-day artifact.
   - On an abort, the integrity outputs and an `ABORTED` summary are still committed.

## Data-integrity audit (`data.session_audit`, not part of the frozen set)

**Every session that has any row is listed in `session_audit.csv.gz`**, with:
- first and last minute;
- rows before 09:15, inside 09:15–15:29 and from 15:30;
- missing minutes;
- whether 09:15 and 15:29 are present;
- identical and conflicting duplicate timestamps;
- timestamps that are not on the minute;
- a status.

Only `KEPT` and `KEPT_FILLED` sessions reach the analysis. Every other status is counted per instrument and per window.

| Status | Rule | Effect |
|---|---|---|
| KEPT | 375 minutes, 09:15 present, no conflicts | used |
| KEPT_FILLED | ≥ 95% of minutes and 09:15 present | used; missing minutes are forward-filled (o=h=l=c=previous close, volume 0) and counted |
| EXCLUDED_NO_0915 | first minute missing | excluded, listed |
| EXCLUDED_INCOMPLETE | < 95% of minutes | excluded, listed |
| EXCLUDED_DUPLICATE_CONFLICT | one timestamp with two different candles | excluded, listed |
| EXCLUDED_NO_REGULAR_SESSION | no minute in 09:15–15:29 (e.g. Muhurat) | excluded, listed |

- **Identical duplicates** are removed and counted.
- **Rows from 15:30** (present in 2021) are ignored and counted per session.
- **Missing sessions:** sessions absent against NIFTY's calendar, between an instrument's first and last session, are counted and listed (the first 50 dates).
- **Anomalies file:** `session_anomalies.csv` lists every session that is not plain `KEPT`, or that has duplicates, rows before 09:15 or off-minute timestamps.
- **No eligibility rule changes:** an instrument with fewer than 150 complete sessions in a window is still NOT_APPLICABLE there (the frozen `confirm.MIN_SESSIONS_ELIGIBLE`).

**Request boundary.** Probes 38351078556 and 38351580582 showed that Dhan omits the 09:15 candle on the first day of a request. Each request therefore starts 4 days before its first new day, and the fetcher drops the lead-in rows. The analysis also discards any row outside 2021-10-01 .. 2026-10-12, so the fetched span, and therefore the 20-session warm-up, starts on 2021-10-01 exactly as pre-registered. Any session still missing 09:15 is reported, not hidden.

## Resources and API load

**Requests**
- 22 per instrument × 102 instruments, plus 1 interval probe = **2,245**.
- A hard cap of 2,600.
- One retry per failed request, counted against the cap.
- Spacing 0.6 s, so at most 1.67 requests per second. Dhan's published data-API limit is higher.
- **Runtime:** about 60–75 minutes. The 4-year discovery fetch (about 1,735 requests) took 50 minutes end to end in run 38279444111.

**Memory, runner**
- About 21 MB of 1-minute arrays per instrument over 1,250 sessions (measured on synthetic data).
- About 2.2 GB for 102 instruments, plus about 100 MB transient per instrument while parsing.
- The analysis adds under 1 MB per instrument per window.
- The parse stops if available memory falls below 1 GB.

**Memory, node 0**
- One 89-day response at a time, under a 150 MB systemd cap (node 0 had 468 MB free during market hours; a fetch exceeding the cap is killed alone).

## Abort conditions

On any of these the stream ends with `STOP <reason>` and the analysis makes no verdicts:

- the first HTTP 429;
- 2,600 requests;
- 10 failed requests in a row;
- node `MemAvailable` below 300 MB;
- the local `/health/node` on two consecutive checks (one before each instrument) being unreachable, slower than 3 s, in CONFIG_ERROR, or showing a new supervisor hard reset;
- reaching 08:45 IST on a weekday.

In addition:
- a truncated stream (no `END`), a 5-minute fallback, or low runner memory also give `ABORTED`;
- `systemd` kills the fetch after 2 hours.

Only the health status word, supervisor state and response time are printed.

## Labels the report carries

- 2021-10-01 .. 2022-10-12 is a **historical confirmation period that predates the discovery data**. It is not a forward chronological test.
- **Survivorship:**
  - H5 is capped at INCONCLUSIVE;
  - the bias on H1–H4 and H6 is stated as residual.
- **Corporate actions:**
  - flagged sessions and the ±5 sessions around them are excluded by the frozen `integrity.py`;
  - stocks with flags have their per-stock rows marked `CA_UNCERTAIN`;
  - per-stock results are descriptive only.
- **What a non-confirmation means:** no edge was established under the pre-registered test. It does not prove that no edge exists.
- **Same-week correlation and trades per week** are measured on the in-sample window and reported with a recomputed minimum detectable effect. This does not change any decision rule.
