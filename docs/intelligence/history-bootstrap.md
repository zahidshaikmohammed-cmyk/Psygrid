# History bootstrap

The Intelligence engine compares every minute with the same minute in earlier
sessions. A fresh VM has no archive, so the engine would wait weeks for
history. The bootstrap fills the archive with **genuine Dhan 1m history**
in exactly the format PSYGRID's own daily archive uses. Intelligence then
discovers the days automatically, and PSYGRID's live archive keeps appending
new sessions after them.

## How much, and why

| Sessions | What it activates |
| --- | --- |
| 5 | The minimum for any historical baseline and for similarity results |
| **20 (default)** | The full baseline window (`history.build_baselines(window_sessions=20)`): no `BASELINE_SHORT` flags; similarity without `FEW_SESSIONS` |
| 60 | Similarity's full lookback (`PSYGRID_INTELLIGENCE_SIMILARITY_SESSIONS`) |

Start with 20. Deeper history is an explicit, resumable expansion:
`bootstrap-history run --sessions 60` downloads only the sessions not yet
archived.

## Cost (989 equities + 16 indices)

| | 20 sessions | 60 sessions |
| --- | --- | --- |
| Intraday requests (one per instrument per ≤ 90 calendar days) | 1,005 | 1,005 (one span) or 2,010 |
| Daily requests (previous close and day open, one per equity) | 989 | 989 |
| Calendar probe | 1 | 1 |
| Total, and share of Dhan's 100,000/day Data API limit | 1,995 (2.0%) | about 2,000–3,000 (2–3%) |
| Time at 2 requests/s | about 17 min | 17–25 min |
| Archive size (about 6.2 MB a session) | about 124 MB | about 372 MB |

`bootstrap-history plan` prints these numbers for the current archive
without any API call or credentials.

## Commands

```bash
python -m intelligence bootstrap-history plan [--sessions 20] [--rate 2]
python -m intelligence bootstrap-history run  [--sessions 20] [--rate 2] [--workers 2] [--env-file FILE]
python -m intelligence bootstrap-history status
python -m intelligence bootstrap-history verify
python -m intelligence validate-replay [DATE]
```

On the VM, use the **Intelligence history (VM operations)** workflow
(Actions → Run workflow) with action `inspect`, `plan`, `run`, `status`,
`verify` or `validate`. `run` starts the bootstrap as a transient systemd
unit (`Nice=15`, `CPUQuota=50%`, `MemoryMax=700M`, idle I/O, at most 2 hours).
It uses the Dhan credentials of the running PSYGRID process, copied to a
root-only tmpfs file that is deleted when the unit stops. It then verifies the
archive, waits for `/v2/health` to leave `WAITING_FOR_DATA`, and runs replay
validation.

## Guarantees

- **Genuine data only.** Values are written exactly as Dhan returned them (the
  same 4-decimal formatting as the live archive). The previous close is the
  prior session's daily close and the open is the day's daily open, both
  from Dhan. Rows that fail validation are left out and counted in the
  manifest (`excluded_rows_in_batch`): non-numeric values, broken OHLC
  geometry, bars outside 09:15–15:30, and duplicate minutes. Nothing is
  repaired, interpolated or invented. An instrument Dhan has no data for is
  listed in `missing_equities`.
- **The real calendar.** Sessions come from Dhan's NIFTY daily candles, so
  holidays are never guessed. Today is never written.
- **Live data is never touched.** A day directory written by PSYGRID's live
  archive is never modified, and a day the live archive writes while a run is
  in progress is kept.
- **Resumable and idempotent.** Each instrument's download is staged to its
  own file under `<archive>/.bootstrap/<batch>/`, written atomically with a
  SHA-256 sidecar; that file is the progress record. A rerun skips staged
  instruments, downloads again any stage whose digest does not match, and
  resumes an interrupted batch before starting a new one. Day directories are
  assembled in a temporary folder, verified, made durable and only then moved
  into the archive, oldest first. A bootstrapped day whose files no longer
  match their manifest digests is rebuilt on the next run. Staging is deleted
  after a fully verified run.
- **Bounded load.** A shared token bucket limits requests (2/s default, 4/s
  maximum, against Dhan's 5/s per-account Data API limit that the live feed
  shares). There are 2 workers by default (4 maximum). Each worker holds one
  instrument's response at a time; measured peak memory is 89 MB at 300 symbols and stays under about 200 MB at 989 (the verifier's duplicate check is the largest part). The
  instrument master is streamed, never held whole.
- **Market-hours guard.** A run refuses to start between 08:45 and 15:45 IST
  on weekdays, and stops itself at the next checkpoint if one runs into that
  window. Rerun later to resume.
- **Failures.** 429 and Dhan's `DH-904` cool down every worker (honouring
  `Retry-After`). 5xx and network errors back off exponentially (6 attempts).
  `DH-907` means no data. An instrument that still fails is recorded and
  retried on the next run, and sessions are assembled only when every
  resolved instrument is staged. An authentication or entitlement error
  (401, `DH-901`/`902`/`903`) stops the run at once.
- **Tokens.** The bootstrap uses an access token from its environment
  (`DHAN_ACCESS_TOKEN`, or the variable named by `DHAN_TOKEN_VAR`). It never
  generates one unless run with `--generate-token` (the workflow's
  `generate_token` input), because a newly generated token might expire the
  one PSYGRID is using. On the production VM, PSYGRID holds only `DHAN_PIN`
  and `DHAN_TOTP_SECRET` and generates a token on every start, so the
  bootstrap needs `generate_token`. Use it outside market hours, ideally
  before a non-trading day. If Dhan expires PSYGRID's token, PSYGRID's
  existing auth guard regenerates its own on the next authentication failure.
  The bootstrap never regenerates mid-run: on an authentication error it stops,
  and a rerun resumes.

## Integrity checks (`bootstrap-history verify`)

For every archived day, bootstrapped or live, the checker reads every file in
full and reports:

- headers;
- timestamps that fail to parse, fall on another day, are not minute-aligned
  or lie outside the session;
- duplicate (instrument, minute) pairs;
- OHLC geometry and negative volume;
- row counts and SHA-256 digests against the manifest;
- symbols outside the universe;
- universe symbols with no rows.

## Manifest of a bootstrapped day

```json
{
  "session_date": "2026-10-01", "source": "DHAN_HISTORICAL_API", "synthetic_candles": false,
  "files": {"equity_1m.csv.gz": {"rows": 368745, "sha256": "…", "written_at": "…"}, "…": "…"},
  "bootstrap": {"tool_version": "1", "batch": "2026-09-03_2026-10-01", "expected_equities": 989,
                "equities_with_data": 983, "missing_equities": ["…"], "unresolved_instruments": [],
                "excluded_rows_in_batch": {"outside_session": 0, "invalid_ohlc": 0}}
}
```
