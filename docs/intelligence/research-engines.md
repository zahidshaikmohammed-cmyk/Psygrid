# Research engines

Four engines measure how the market is behaving at each minute, and one
harness tests whether any of it carries information about what comes next.
Everything uses only data available at `as_of`. Nothing is a recommendation,
and no output uses buy/sell language. Every claim is either tested by the
evaluation harness or labelled as a description.

| Engine | Module | What it measures |
| --- | --- | --- |
| Expected response | `intelligence/response.py` | Each stock's expected move given the market, its sector and statistical factors; the gap between expected and actual; the response still owed through lags |
| Market state | `intelligence/market_state.py` | Correlation structure, eigen concentration, effective dimension, dispersion, breadth, move concentration, change score, descriptive regime |
| Microstructure | `intelligence/microstructure_engine.py` | Packet cadence first, then OFI, signed flow, impact slope, replenishment, and a move classification, each gated by cadence |
| Derivatives expectation | `intelligence/expectation.py` | ATM IV, straddle, implied move, realised-variance budget consumed against the time-of-day norm, skew, OI, basis, tail z, each with uncertainty |
| Evaluation harness | `intelligence/evaluation.py` | Walk-forward out-of-sample test of the response signals |

## Expected response

For stock *i* at minute *t*:

```
r_i,t = Σ_l b_i,l · m_t−l + Σ_l g_i,l · s_i,t−l + Σ_k d_i,k · p_k,t + e_i,t      l = 0..5
```

- `m` is NIFTY 500's 1m return. NIFTY is the fallback, then the cross-sectional median; the source is reported.
- `s_i` is the leave-one-out mean market-residual return of the stock's sector peers. Only taxonomy sectors with at least 3 members count.
- `p` are 5 statistical factors. Their loadings are the principal components of what the lag model leaves in the training sessions; their realisations are estimated each minute by cross-sectional regression.

The fit has two stages: a ridge on the market and sector lags, then a ridge
on the factor realisations. The statistical factors are taken *after* the lag
model on purpose. Factors taken before it absorb the common delayed response
to the market and hide the effect being measured. A synthetic test
(`test_evaluation.py`) proves this.

Training uses the 20 qualified sessions strictly before the day. The model
covers every stock seen in those sessions, and each live minute is aligned to
it (a stock with no bar yet is missing, never zero). Models are cached at
`<store>/response/<date>.t20.v1.npz`. The live runner builds the next
session's model after 15:45 or before 09:00, so the session only loads it.

Outputs per stock (`/v2/stocks/{key}` → `expected_response`):

- `expected_response`, `actual_response` and `response_gap` (expected − actual) over the last 15 minutes. `response_gap_sigma` is the gap in units of the stock's residual σ·√minutes.
- `contributions`: the market, sector, statistical and residual parts.
- `pending_response` and `pending_response_sigma`: the part of the expected move the lag structure still owes for factor moves already seen. This is the only forward-looking quantity, and the harness tests it.
- The delay profile: lag coefficients, delay index (1 − immediate share), half-life and mean lag.

## Evaluation harness

`python -m intelligence evaluate` (VM: workflow action `evaluate`, outside
market hours). For each session with 20 sessions before it, the model is fitted
on those sessions only, then evaluated every 5 minutes:

- **Target**: the forward 1/5/15/30-minute return with only the *contemporaneous* (lag-0) market, sector and statistical exposure removed. Anything the lags predict stays in the target, where a signal can find it.
- **Statistics**: daily Spearman rank IC, then mean IC, t, p and hit rate across days. Also first-half and second-half stability, IC by time of day, IC by liquidity (turnover terciles), and IC by regime (above or below median dispersion).
- **Economics**: top-minus-bottom quintile in bps, net of a Roll-spread cost estimate.
- **Placebos**: the time-shuffled signal, the reversed-time session, and the sign flip.
- **Multiple testing**: Benjamini–Hochberg at q = 0.05 over every signal × horizon × subset.
- **Verdict** (pre-registered; every verdict carries its reason):
  - `SUPPORTED` only if a hypothesis survives FDR, has positive IC, keeps the same sign in both halves, and both placebos are insignificant;
  - `REJECTED` if it is significant in the wrong direction, or significant with an equally significant placebo (the effect is not about timing);
  - `UNPROVEN` otherwise.
- **Also reported**: a 95% interval for the mean IC, the IC information ratio, top-decile turnover, the median and volatility of the top-minus-bottom spread, and the share of days with a positive net return.

Stale stocks (no bar in the latest minute) are excluded. Mid-price
comparisons need the Full-packet archive, which starts with the first
recorded session.

The report is written to `<store>/evaluation/latest.json`. Results on real
data are recorded in [completion-matrix.md](completion-matrix.md).

## Market state

Over the trailing 30 minutes:

- mean pairwise correlation of the stocks with at least 80% of bars;
- absorption ratio (top-5 eigen share), leading-eigenvalue share, and effective dimension (participation ratio). These come from the window × window Gram matrix, which is cheap for 989 stocks;
- dispersion of window returns (bps), and breadth over the window and since the open;
- move concentration: the share of the turnover-weighted move from the top 10 contributors. **NSE index weights are not available, so traded value is the weight.** This is a proxy and is labelled so;
- change score: the larger |change| in correlation or dimension against the previous window, as a percentile of the same changes in earlier sessions.

Two more measures:
- market volatility: the standard deviation of the market factor's 1m returns in the window, in bps;
- sector synchronisation: the mean within-sector correlation minus the all-pairs mean correlation.

Each value is placed as a percentile among earlier qualified sessions at the same time of day (±30
minutes). Profiles are cached in `<store>/market_state/`.

**State** (`state`) follows a rule fixed before looking at any results:

| State | Percentile rule |
| --- | --- |
| `STRESSED` | mean correlation ≥ 0.8, and dispersion or market volatility ≥ 0.8: moving together, and far |
| `DISLOCATED` | dispersion ≥ 0.9 with mean correlation ≤ 0.5: moving far, but apart |
| `TRANSITION` | change score ≥ 0.9: the correlation structure is changing fast |
| `NORMAL` | otherwise; `UNCALIBRATED` without history |

`persistence` is the share of earlier 15-minute steps in the same state that were still in it 15 minutes
later. The finer descriptive `regime` labels (`COUPLED_STRESS`, `COUPLED`, `DISPERSED`, `QUIET`, `TYPICAL`,
prefixed `SHIFTING_`) remain. Whether the state adds predictive value is tested:
- in the evaluation harness, by splitting by regime;
- in 945, by splitting by market state and through v2's market-context features.

## Microstructure

Input is the recorder's per-minute features from Dhan Full packets: the live
stream in the session, `microstructure_1m.csv.gz` afterwards. The engine
measures cadence before anything else:

| Metric family | Supported when | Otherwise |
| --- | --- | --- |
| quote (OFI, depletion and replenishment, imbalance) | median ≥ 20 packets/min and median largest gap ≤ 10 s | `null`, with the reason |
| trade (signed flow, impact slope) | median ≥ 5 trade packets/min | `null`, with the reason |

Outputs:

- OFI normalised by mean top-of-book depth;
- signed-flow share (quote rule, then tick rule);
- an impact slope: mid change in bps per unit of signed volume, where the unit is the mean minute volume;
- replenish ratio, spread and depth;
- a classification: `AGGRESSIVE_CONSUMPTION`, `LIQUIDITY_WITHDRAWAL`, `ABSORPTION`, `QUIET`, `MIXED` or `UNSUPPORTED`.

All of these are packet-level approximations. Dhan sends snapshots, not
exchange events. The real Dhan cadence per stock is first measured at the
first live session after deployment.

## Derivatives expectation

Input is the latest full option-chain snapshot recorded by `as_of`
(`<store>/chains/<date>.csv.gz`, once a minute per index), plus index 1m
bars and the futures snapshots.

| Output | Definition |
| --- | --- |
| `atm_iv`, `implied_daily_move_pct` | Mean of call and put IV at the strike nearest spot, and IV/√252 |
| `straddle`, `straddle_pct`, `implied_sd_to_expiry_pct` | ATM call + put (mid when both quotes exist); straddle/spot ÷ √(2/π) |
| `realised_var`, `budget_consumed` | Σ r² of index 1m returns since the open; that over IV²/252 |
| `expected_share`, `expected_share_band`, `budget_ratio` | Median (10–90%) share of a day's realised variance arrived by this minute in earlier sessions; budget consumed over that share |
| `skew` | 25-delta risk reversal when deltas are served, else the IV difference 3% out of the money each side (method reported) |
| `put_call_oi`, `ce_oi_change`, `pe_oi_change` | From the chain |
| `basis_bps`, `basis_z` | Futures − spot, and its z against the session's earlier readings |
| `tail_z` | Latest 1m index return in units of the 1m move IV implies |
| `uncertainty` | Snapshot age (stale beyond 180 s), ATM bid-ask width as a share of the straddle, strikes with IV, sessions behind the profile |

There are no dealer-positioning or gamma claims. Stock option chains are not
recorded each minute; only index chains are.

## The minute stream (real-time path)

PSYGRID's recorder appends one block per closed minute, 3 s after the minute
ends, to `<archive>/.live/<date>/bars_1m.csv` (all 989 stocks plus index bars
as `IDX:<key>`) and `micro_1m.csv`. Each block ends with `#END <minute> <rows>`
in the same write call, so a reader never sees half a minute
(`intelligence/stream.py`).

During the session the live runner polls every 2 s (one `stat()`). It merges
the archive and the stream (archived bars win) and steps the engine through
each new minute. Live results equal a replay of the same bars; a test proves
this. Set `PSYGRID_INTELLIGENCE_STREAM=0` to follow only the five-minute
archive.

Measured at 989 stocks (`tools/bench_stream.py`, sandbox):

| Stage | Time |
| --- | --- |
| Snapshot after a block lands (merge + engine step) | median 1.11 s, max 1.23 s |
| Research views for the minute (response state, market state, expectation, microstructure index) | median 0.26 s |
| `/v2/stocks/{key}` once the minute is computed | 17 ms |
| `/v2/stocks` ranking | 2 ms |
| **Bar close → snapshot** | **4.1–7.2 s** (7.5 s including the research views), against up to 360 s from the archive alone |
| Response model, cold build from 20 sessions | 70 s, peak +290 MB above a day load; built outside market hours |

## Data preservation, retention and disk budget

| Data | Where | Retention | Budget |
| --- | --- | --- | --- |
| 1m equity and index bars | `<archive>/<date>/` (gzip) | Permanent | ≈ 6 MB/day |
| Full-packet features per stock-minute (depth levels 1 and 5, quantities, order counts, total buy/sell quantity, average price, LTP/LTQ/LTT, spread, OFI, flow) | Stream, then `<archive>/<date>/microstructure_1m.csv.gz` with a sha256 manifest | Stream 2 days; archive permanent | ≈ 30–60 MB/day compressed (to be confirmed by the first real session) |
| Raw 5-level depth snapshots (`PSYGRID_RAW_DEPTH_SYMBOLS` only) | `depth_snapshots.csv.gz` | Permanent | 300 MB/day cap |
| 20-level option depth per contract-minute (NIFTY/BANKNIFTY/MIDCPNIFTY/SENSEX nearest 25 strikes, NIFTY 50 stock options while their depth batch is active): cadence, spread, 5- and 20-level depth and imbalance, top-of-book OFI, liquidity added and removed, largest resting order per side, OI and volume | Stream, then `<archive>/<date>/option_depth_1m.csv.gz` with a sha256 manifest (`option_depth_recorder.py`) | Stream 2 days; archive permanent | ≈ 5–10 MB/day compressed (≈ 150 contracts × 375 minutes) |
| Full index option chains, every strike, once a minute | `<store>/chains/<date>.csv.gz` | Permanent | 150 MB/day cap |
| Futures and option aggregates per minute | `<store>/derivatives/<date>.jsonl` | Permanent | < 1 MB/day |

The recorder stops writing when free disk falls below 2 GB. It drops (and
counts) minutes older than 10 minutes if it falls behind. It switches itself
off if it uses more than 15% of one core for 5 consecutive seconds. Its
status is in `<archive>/.live/status.json`. The feed path calls it in a
`try`; any error disables the recorder and never touches ingestion.
