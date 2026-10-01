# Testing

```bash
python -m pip install -r requirements-dev.txt
ruff check . && ruff format --check .
python -m pytest -q                       # everything (CI and the deploy workflow run this)
python -m pytest tests/intelligence -q    # intelligence only
```

The intelligence tests use seeded synthetic archives written through the real
`DailyArchive` writer (`tests/intelligence/conftest.py`): 8 sessions of 30
real symbols, with three known anomalies on the last day (a ×12 volume surge
in TCS at minute 120, a +2% move in HDFCBANK at minute 150, and a 30-minute
drift of SUNPHARMA away from its sector from minute 200). Detectors are
tested against these known answers. Do not add `tests/intelligence/__init__.py`:
it would shadow the `intelligence` package.

| File | What it proves |
| --- | --- |
| `test_foundation.py` | Archive validation and rejection reasons, frames contain only completed bars, quality accounting, replay |
| `test_features.py` | Feature values against hand computations, NaN instead of guesses, baselines from earlier sessions only, caching |
| `test_anomaly.py` | Thresholds, injected anomalies found with full evidence, low false-positive rate, STALE/INVALID never scored, fallbacks |
| `test_relationships.py` | Sector divergence found and absent before it starts, gating (`NOT_ESTABLISHED`, `INSUFFICIENT_DATA`), no future returns, price/volume, derivatives series, torn files |
| `test_events.py` | Severity bands, deterministic ids, evidence, no advice words, determinism, cooldown and restart, store search, idempotence, backup, novelty |
| `test_similarity.py` | Past states equal live states, outcomes, one match per session, no later sessions, insufficient history, caching |
| `test_pipeline.py` | Full-session determinism, poisoned-future tests for every engine, live payloads equal the archive, strict JSON views |
| `test_service.py` | Keys, rate limits, live following equals replay, restart without repeats, derivatives recording, failure isolation, backups, every `/v2` route, validation, 401/404/422/429/503, the stream (backfill, filters, auth, limits), no import of the production app |
| `test_deploy.py` | The unit's resource caps and hardening; the deploy step runs only after PSYGRID is verified and never restarts it |

PSYGRID's own suites (contract tests pinning all 91 routes and their response
shapes, market hours, archive, startup) are unchanged and still pass; the
intelligence work changed no production module.

`tests/test_dhan_990_resolution.py` needs network access to Dhan's instrument
master and fails in sandboxes without it; it passes in CI.
