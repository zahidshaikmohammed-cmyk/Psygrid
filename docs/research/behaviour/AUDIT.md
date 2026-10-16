# Audit of the first-generation pattern mining (`deploy/live-core/pattern_mine.py`)

Run audited: GitHub Actions run 38262906763 (2026-10-12 14:26–14:50 UTC, commit `a1d253b`),
action `pattern-mine`, node 0. The workflow concluded "success". That only means the script ran to
completion and the nodes were healthy afterwards; it is **not** evidence that the research design was
valid. This audit lists what the run did and did not establish.

## 1. What was tested

All tests used 15-minute bars built from Dhan 5-minute candles. There were six context families,
and each context was traded in the direction its discovery-period sum favoured:

| Family | Context | Entry | Exit |
|---|---|---|---|
| SLOT | each of 24 fifteen-minute slots, every day | open of the slot (decided in advance) | close of the same slot |
| DOW | weekday | 09:15 open | 15:15 close |
| DOWSLOT | weekday × slot (120) | slot open | slot close |
| MOTIF | 3 consecutive 15-minute candles, each one of 5 classes (scaled by the stock's median 15-minute move), × 3 parts of day | next bar open | close two bars later (30 minutes) |
| OPEN | gap bucket (5) × first 15-minute candle class (5) | 09:30 open | 15:15 close |
| HOUR1 | gap bucket × first-hour move bucket × first-hour range bucket | 10:15 open | 15:15 close |

**Not tested at all:**
- volume, relative volume and VWAP;
- EMA or any trend structure;
- pullbacks (shallow, deep, failed), liquidity sweeps, breakouts other than the opening, and reclaims;
- volatility compression and expansion;
- intraday stops, targets, or favourable/adverse excursion;
- 1-minute and 5-minute resolution: the 5-minute candles were collapsed to 15 minutes;
- conditioning on NIFTY, sector or regime;
- stability across years and regimes;
- walk-forward testing;
- any cost other than one flat 0.10%.

## 2. Coverage per symbol (from the run log)

| Coverage | Instruments |
|---|---|
| Evaluated | 100 of 102 |
| Skipped (fewer than 150 discovery sessions) | ATHERENERG (110), ENRIN (78) |
| 975 sessions, 2022-10-14 to 2026-10-12 | 89 instruments, including NIFTY, BANKNIFTY and SENSEX |
| 974 sessions | RELIANCE, ITC, VEDL, HINDUNILVR, TMPV |
| 950 sessions | KAYNES (from 2022-11-23) |
| 768 sessions | JIOFIN (listed August 2023) |

- **Contexts per instrument:** 268–378 (those with at least 30 discovery trades). Total tested: 35,435.
- **Sessions dropped:** any session missing one of its 24 fifteen-minute buckets. About 4 of the 979
  NIFTY sessions were lost this way, including special and Muhurat sessions. Gaps inside a bucket were
  not counted or reported.
- **The last 15 minutes (15:15–15:30) were excluded** everywhere.

## 3. Timing and entry assumptions

- **09:15 entries are not executable.** SLOT, DOW and DOWSLOT contexts at 09:15 entered at the
  opening-auction print. Most "short the 09:15 candle" and "every Tuesday" results depend on that
  price.
- **Fills are optimistic.** MOTIF, OPEN and HOUR1 decide at a bar's close and fill at the next bar's
  open, with zero latency and zero slippage beyond the flat cost.
- **No look-ahead in the scaling.** The candle-class scale (σ, the median 15-minute move) came from
  discovery sessions only.
- **The gap reference is correct:** the previous kept session's close.
- **One trade per context per session**, so trades of one context do not overlap within a day.

## 4. Statistical assumptions

- **The t-statistics assumed independent trades:** t = mean / (sd/√n). They ignored:
  - serial dependence across days;
  - market moves shared by every stock on the same day;
  - the heavy overlap between nested contexts (DOWSLOT inside SLOT; OPEN and HOUR1 share gap buckets).
- **The luck baseline was wrong.** The report said "up to 0.0027 × tests ≈ 96 would reach t ≥ 3 by
  luck". That assumes independent tests, and more importantly it ignores the selection filter, which
  required **net-of-cost** mean > 0 and t ≥ 3.
  - Under a null where the **gross** mean is zero, a context only passes if noise alone exceeds the
    0.10% cost by three standard errors.
  - For typical samples (n ≈ 40–300, trade sd 0.2–1%), that probability is essentially zero.
  - **The earlier statement that "the real data found fewer patterns than chance" was therefore
    incorrect.** Two passes is what you expect when no context pays 0.10% with t ≥ 3. It says
    nothing about whether recurring gross behaviour exists.
- **No FDR control, and the holdout check was too weak.** Each pass was checked only for a positive
  holdout mean (t ≥ 2 for "YES").
- **The per-instrument "strongest pattern" table was the maximum t of about 350 contexts.** It was
  selection-biased and should not be read as a fingerprint.

## 5. Does the fixed 0.10% cost change the conclusion? Yes, materially

- **The cost is large relative to the moves.** The typical 15-minute move was 0.06–0.31% (index
  0.06%, IDEA 0.31%). For 15- and 30-minute contexts, a 0.10% round trip is comparable to the entire
  expected move. The t ≥ 3 screen on net returns then needs gross edges of about 0.25% or more on
  samples of around 40. The screen tested "is there a large edge that pays 0.10%", not "is there
  recurring behaviour".
- **Many contexts were gross-positive but net-negative in the holdout.** Examples from the
  per-instrument table (holdout gross vs net):
  - ITC, Thursday short: +0.48% gross / +0.38% net.
  - RELIANCE, Tuesday short: +0.47% / +0.37%.
  - TIINDIA, Thursday 09:15 short: +0.17% / +0.07%.
  - MARUTI, Thursday 09:15 short: +0.27% / +0.17%.
  - Several 09:15 shorts were positive gross but near zero or negative net.
- **One flat cost is wrong in both directions.** It is far too high for liquid large caps: HDFCBANK at
  ₹705 with a ₹0.05 tick, where a one-tick spread is about 0.007%. It is too low for low-priced names:
  IDEA at ₹12.7, where one ₹0.01 tick is about 0.08%.

The second-generation engine therefore reports gross results, three cost scenarios, and a per-stock
spread estimated from 1-minute data.

## 6. What the first run supports, and what it does not

- **Supported:** none of these simple 15-minute contexts showed a discovery-period edge large enough
  to clear a flat 0.10% at t ≥ 3. The two that did (JIOFIN gap/first-candle short, TIINDIA Thursday
  09:15 short) were not significant in the holdout.
- **Not supported:**
  - that stocks have no recurring behaviour;
  - that no edge exists at realistic, stock-specific costs;
  - that nothing exists in the families listed in section 1 as untested.

`research/behaviour/` (the second-generation engine) addresses each gap. `FINDINGS.md` reports its
results.
