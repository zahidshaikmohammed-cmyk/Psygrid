# Second-generation stock behaviour discovery — findings

Run: GitHub Actions 38279444111, 2026-10-12, commit `c5af8b0`. Post-hoc tables: run 38288465542 (reads that run's artifact; no new data).
Machine-readable results: `research/behaviour/results/run-38279444111/`.
Engine: `research/behaviour/` (tests: `tests/test_behaviour.py`). Audit of the first-generation run: `AUDIT.md`.

## Data and design (what was actually done)

**Data**
- **Instruments:** 102 — NIFTY, BANKNIFTY, SENSEX and the 99 F&O stocks with the highest full-session traded value on 2026-10-12.
- **Candles:** genuine **1-minute** OHLCV from Dhan, 2022-10-13 to 2026-10-12. Dhan accepted 89-day 1-minute requests, and all 102 instruments streamed completely.
- **Sessions:** 975 per instrument (RELIANCE 974; JIOFIN 768, listed 2023; ATHERENERG 353 and ENRIN 321, both listed 2025).
- **Missing data:** 0.002–0.2% of minutes were forward-filled. Sessions with less than 95% of their minutes were dropped.
- **Storage:** nothing raw was stored. Node 0 streamed the data over SSH into the runner's RAM; only aggregate statistics were kept.

**Decisions and outcomes**
- **Decision bars:** decisions are made on 5-minute bars built from the 1-minute data. The 1-minute data is also used for each stock's spread estimate.
- **No look-ahead:** every feature at bar t uses only bars up to t plus earlier sessions. Rolling baselines use the 20 previous sessions. Tests cover this.
- **Entry and exit:** entry at the next bar's open, exit after 15m, 30m, 60m or at 15:15. One trade per hypothesis per session.
- **Excursions and comparisons:** MFE/MAE per trade, plus the excess return over beta × NIFTY and over the stock's own sector peers.

**What was searched**
- **Size:** 3,452–5,304 hypotheses × horizons per instrument, **498,252 tests in total** (381,580 with enough discovery data).
- **Events:** trend continuation and exhaustion; EMA20 (shallow) pullbacks, deep pullbacks and failed pullbacks (trend failure); opening-range breaks and traps; liquidity sweeps and reclaims; failed breakouts; previous-day high/low breaks and rejects; squeeze (compression → expansion) breaks; relative-volume thrusts; VWAP reclaims and losses.
- **Conditions each event was paired with:** time of day, EMA9/EMA20 trend, VWAP side, relative volume, day volatility regime, NIFTY momentum, strength vs NIFTY, strength vs sector.
- **Data-driven families:** 15-minute slots, weekday × slot, single 5-minute candles and 3-candle sequences by time of day, 36-cell market-state vectors × time of day, and gap × first-candle openings.

**Validation**
- **Periods:** discovery up to Sep 2024, validation Oct 2024 – Sep 2025, holdout Oct 2025 – Oct 2026 (never used to select).
- **Walk-forward:** rolling 18 months of training, then 3 months of testing.
- **Statistics:**
  - standard errors clustered by calendar week (handles serial and same-week market dependence);
  - Benjamini–Hochberg FDR per stock and globally;
  - a moving-block (4-week) bootstrap for holdout confidence intervals;
  - three cost scenarios with a per-stock 1-minute Corwin–Schultz spread (median 0.028%, maximum 0.10% for SUZLON). Round-trip **low = 0.03% + spread, base = 0.07% + spread, high = 0.13% + 2×spread**, which gives a base cost of 0.08–0.17% depending on the stock.

**Engine self-check:** on synthetic data, a planted behaviour is found and VALIDATED, and pure-noise stocks produce nothing (`tests/test_behaviour.py`, also run inside the workflow).

## 1. Descriptive behaviour exists — but it is small and concentrated

- **There is more structure than chance.** 4,241 eligible tests reach a **gross** |t| ≥ 3, against about 1,030 expected from independent noise. 46 reach t ≥ 5, against about 0.2 expected. Tests overlap heavily, so this excess is an upper bound, but genuine non-random intraday structure exists.
- **Few stocks have a reliable, stable stock-specific behaviour.** The strict bar: per-stock FDR q ≤ 0.05 on excess over the stock's own time-of-day baseline, |t| ≥ 3, at least 60 occurrences and at least one every four weeks, and the same sign in all (or all but one) years.
  - **34 behaviours in only 13 of 102 instruments** pass it: BSE 13, TIINDIA 8, ANANDRATHI 2, AUROPHARMA 2, and one each in SBIN, TITAN, DELHIVERY, SUZLON, PFC, TMPV, TRENT, ASHOKLEY and DLF.
  - **89 instruments, including all three indices, have none.**
  - Loosening the bar to q ≤ 0.10 gives 75 behaviours across 20 stocks.
- **23 of the 34 point the same way in the holdout** (excess over baseline has the same sign).
- **Examples, all descriptive:**
  - BSE tends to keep falling after weakness events: failed downside breakouts when weak against NIFTY, squeeze breaks down on compressed days, and down-trend states below VWAP.
  - TIINDIA tends to weaken in the 13:15–14:15 window (4/4 years, t 6.6). In the holdout that excess fell to about zero.
- **Several popular "habits" are not real:**
  - **The first 15 minutes reversing the day:** median across instruments 49%, a coin flip.
  - **Gap-fill rate** (gaps of 0.3% or more that touch the previous close the same day): median 71%. That is a property of volatility, not a trading signal (see below).

## 2. Tradable edges after costs: none validated

| Cost scenario | Discovery candidates (of 381,580) | Pass validation | Holdout (≥ 20 trades), net > 0 |
|---|---|---|---|
| zero (gross, not achievable) | 3,169 | 40 | 36 of 40, mean +0.075% |
| low (0.03% + spread) | 62 | **0** | — |
| base (0.07% + spread) | 3 | **0** | — |
| high (0.13% + 2×spread) | 0 | 0 | — |

- **At realistic costs, nothing survives.** At base cost the three discovery candidates (MAXHEALTH, ANGELONE, SONACOMS) all failed validation. The engine's tiers are VALIDATED_EDGE 0, PROMISING 0, FAILED_HOLDOUT 0, DISCOVERY_ONLY 3.
- **The gross-only survivors are mostly drift, not patterns.** Most are "short LICI to 15:15" in dozens of variants, including "enter 11:00 daily, short to close". That is LICI's multi-year intraday downward drift, not a conditional behaviour, and it is exactly what costs and a beta/sector adjustment are meant to strip out.
- **Even at zero cost, the holdout average is +0.075% per trade.** That is about the size of the cheapest realistic round trip.

## 3. Walk-forward: no out-of-sample evidence

The aggregate looks positive: 2,410 trades, net (base) +0.088% per trade, week-clustered t 2.5. It is entirely one bet:

- **The last window (Jul–Oct 2026) drives it:** 2,075 trades, but 65 rules on **one stock, ANANDRATHI**. These are variants of "short to 15:15 after weakness" during a falling quarter for that stock, so the 65 rules are effectively a single correlated position. They earned +0.096% per trade at base cost and +0.016% at high cost.
- **Every other window combined:** 335 trades, 11 stocks, net (base) **+0.038%, t 0.38**; net (high) −0.059%.
- **By family:** only EVENT is positive, and that is the ANANDRATHI cluster. DOWSLOT, MOTIF and CANDLE are around zero or negative.

## 4. Promising hypotheses (not edges)

A behaviour is listed here only if it is recurring, persists in the holdout, and was cost-positive before the holdout. The holdout figures are net at base cost, in the behaviour's direction:

| Stock | Behaviour | Holdout trades | Holdout net (base) |
|---|---|---|---|
| TMPV | short to 15:15 after a previous-day-low break while NIFTY is flat | 59 | +0.188%, 51% wins |
| BSE | short 60m after a downside squeeze break on a compressed-volatility day | 43 | +0.146% |
| BSE | short 30m, same behaviour | 43 | +0.046% |
| BSE | short to 15:15 after a failed downside breakout while weak against NIFTY | 97 | +0.026% |

The other 8 "promising" rows were negative after base cost in the holdout.

These were found by screening about 500k tests. Their holdout numbers are honest (the holdout was not used to find them), but the samples are small and none passed the formal FDR → validation funnel.

**Data caveats:**
- TMPV is Tata Motors Passenger Vehicles. Its history crosses the October 2025 demerger, so the price series around that date needs checking.
- BSE had a 2:1 bonus in 2025.

**The only defensible use of these is forward paper-tracking. They are not for real money.**

## 5. What the evidence establishes, and what it does not

**Established**
- Intraday price action in these instruments is not pure noise: there is measurable gross structure.
- Stable, stock-specific recurring behaviour exists in a minority of names (13 of 102), concentrated in BSE and TIINDIA.
- None of it, alone or as a walk-forward selection process, produces a validated out-of-sample edge after realistic costs (0.08–0.17% round trip on the underlying).
- Typical recurring effects are 0.05–0.15% per trade, the same size as the costs.

**Not established, and what this search still misses**
- **Option-specific execution:** results are on the underlying. Real option P&L adds premium decay, IV moves and wider option spreads, so it would be worse, not better.
- **Signals finer than 5 minutes:** order-book or depth signals, and 1-minute decision bars. The 1-minute data was used only for bars and spreads.
- **Multi-day and overnight patterns:** this search was intraday by design.
- **Fundamentals- and news-driven behaviour:** results, F&O ban days, index rebalances.
- **Cross-stock lead–lag beyond sector averages.**
- **Learning models over all features jointly:** the search used interpretable single conditions.

None of these extensions removes the cost hurdle shown above.

## 6. Per-instrument fingerprints (all 102)

How to read the table:
- **Recurring (persisting):** the number of behaviours that pass the strict recurrence bar, and how many of those keep the same sign in the holdout.
- **Strongest recurring behaviour:** the most reliable behaviour measured before the holdout. It is marked *below recurrence bar* when it fails the stock-level FDR or stability requirements.
- **Holdout excess:** that behaviour's excess over the stock's own baseline in the holdout.
- **Full detail** per stock (top 10 behaviours, frequency, win rate, median move, MFE/MAE, vs NIFTY, vs sector, stability by year and regime, net after cost): `fingerprints.json`.

| Instrument | Sector | Sessions (D/V/H) | Spread est. % | Base cost % | Typical 5m range % | Beta | Gap-fill % | Recurring (persisting) | Strongest recurring behaviour before the holdout | Holdout excess % | Tradable edge |
|---|---|---|---|---|---|---|---|---|---|---|---|
| NIFTY | INDEX | 478/247/250 | 0.010 | 0.080 | 0.073 | 1.0 | 44.0 | 0 (0) | enter 11:15 on Mon → up over 15m, excess +0.023% (t 3.4, 0.891/wk, years 3/3) *(below recurrence bar)* | +0.009 | none validated |
| BANKNIFTY | INDEX | 478/247/250 | 0.010 | 0.080 | 0.097 | 1.026 | 49.7 | 0 (0) | breakout_up · trend=down → down over 15m, excess +0.059% (t 4.0, 0.667/wk, years 2/3) *(below recurrence bar)* | +0.003 | none validated |
| SENSEX | INDEX | 478/247/250 | 0.010 | 0.080 | 0.073 | 0.984 | 44.7 | 0 (0) | 5m candle bigUp · 14:30-15:05 → up over 15m, excess +0.038% (t 3.8, 1.288/wk, years 4/4) *(below recurrence bar)* | +0.025 | none validated |
| ADANIPORTS | LOGISTICS_TRANSPORT | 478/247/250 | 0.031 | 0.101 | 0.204 | 1.479 | 72.2 | 0 (0) | rvol_thrust_up · tod=11:00-13:00 → up over 30m, excess +0.179% (t 3.2, 1.051/wk, years 3/3) *(below recurrence bar)* | -0.078 | none validated |
| RELIANCE | ENERGY | 477/247/250 | 0.019 | 0.089 | 0.141 | 1.19 | 59.8 | 0 (0) | failed_breakout_dn · rvol=normal → down over 15m, excess +0.025% (t 3.6, 3.218/wk, years 3/4) *(below recurrence bar)* | +0.005 | none validated |
| HDFCBANK | BANKING | 478/247/250 | 0.020 | 0.090 | 0.135 | 1.023 | 62.4 | 0 (0) | vwap_loss_dn · rvol=low → up over 60m, excess +0.069% (t 3.6, 1.327/wk, years 3/3) *(below recurrence bar)* | -0.025 | none validated |
| ICICIBANK | BANKING | 478/247/250 | 0.023 | 0.093 | 0.147 | 0.921 | 63.7 | 0 (0) | or_trap_dn · vsNifty=strong → down over 30m, excess +0.125% (t 3.5, 0.5/wk, years 3/3) *(below recurrence bar)* | +0.018 | none validated |
| BSE | FINANCIAL_SERVICES | 478/247/250 | 0.036 | 0.106 | 0.243 | 1.201 | 68.7 | 13 (11) | or_break_dn · vwap=above → down over 15m, excess +0.249% (t 4.7, 0.417/wk, years 3/3) | +0.015 | none validated; promising hypothesis |
| DMART | CONSUMER_FMCG | 478/247/250 | 0.025 | 0.095 | 0.164 | 0.549 | 72.4 | 0 (0) | deep_pullback_up · tod=09:45-11:00 → down over 60m, excess +0.099% (t 4.1, 2.84/wk, years 4/4) *(below recurrence bar)* | -0.025 | none validated |
| SBIN | BANKING | 478/247/250 | 0.026 | 0.096 | 0.159 | 1.124 | 65.1 | 1 (0) | trend mixed, 30m momentum neutral, below VWAP, rvol normal · 14:30-15:05 → up over 15m, excess +0.059% (t 4.4, 0.641/wk, years 3/3) | -0.000 | none validated |
| INFY | INFORMATION_TECHNOLOGY | 478/247/250 | 0.021 | 0.091 | 0.146 | 1.048 | 51.3 | 0 (0) | pdh_break · vsNifty=strong → up over 60m, excess +0.167% (t 4.1, 1.308/wk, years 3/3) *(below recurrence bar)* | +0.134 | none validated |
| BHARTIARTL | TELECOM | 478/247/250 | 0.022 | 0.092 | 0.157 | 0.777 | 72.3 | 0 (0) | trend up, 30m momentum strong, above VWAP, rvol normal · 11:00-13:00 → up over 30m, excess +0.049% (t 3.5, 2.244/wk, years 4/4) *(below recurrence bar)* | -0.013 | none validated |
| APOLLOHOSP | PHARMA_HEALTHCARE | 478/247/250 | 0.024 | 0.094 | 0.155 | 0.612 | 72.0 | 0 (0) | enter 13:00 on Fri → up over 15m, excess +0.058% (t 4.3, 0.891/wk, years 3/3) *(below recurrence bar)* | +0.003 | none validated |
| KOTAKBANK | BANKING | 478/247/250 | 0.024 | 0.094 | 0.155 | 0.981 | 65.7 | 0 (0) | 5m candles flat-flat-bigDn · 09:45-11:00 → up over 60m, excess +0.108% (t 3.7, 0.474/wk, years 3/3) *(below recurrence bar)* | +0.090 | none validated |
| TCS | INFORMATION_TECHNOLOGY | 478/247/250 | 0.019 | 0.089 | 0.142 | 0.876 | 58.1 | 0 (0) | pdl_break · vwap=above → up over 30m, excess +0.162% (t 4.1, 0.468/wk, years 3/3) *(below recurrence bar)* | -0.095 | none validated |
| AXISBANK | BANKING | 478/247/250 | 0.025 | 0.095 | 0.170 | 1.025 | 71.7 | 0 (0) | failed_breakout_dn · rvol=high → down over 30m, excess +0.087% (t 3.3, 1.359/wk, years 3/3) *(below recurrence bar)* | +0.078 | none validated |
| TECHM | INFORMATION_TECHNOLOGY | 478/247/250 | 0.027 | 0.097 | 0.184 | 1.008 | 64.9 | 0 (0) | or_trap_dn · tod=13:00-14:30 → down over 30m, excess +0.078% (t 3.7, 0.782/wk, years 3/3) *(below recurrence bar)* | -0.026 | none validated |
| MCX | FINANCIAL_SERVICES | 478/247/250 | 0.030 | 0.100 | 0.241 | 1.199 | 71.6 | 0 (0) | trend down, 30m momentum weak, below VWAP, rvol normal · 11:00-13:00 → down over 60m, excess +0.142% (t 3.9, 2.192/wk, years 4/4) *(below recurrence bar)* | +0.124 | none validated |
| SUZLON | OTHER | 478/247/250 | 0.100 | 0.170 | 0.283 | 1.351 | 71.7 | 1 (1) | or_break_dn · dayvol=expanded → up over 15m, excess +0.386% (t 4.5, 0.859/wk, years 3/4) | +0.004 | none validated |
| M&M | AUTOMOBILE | 478/247/250 | 0.025 | 0.095 | 0.178 | 1.209 | 70.1 | 0 (0) | failed_breakout_dn · vsSector=inline → down over 15m, excess +0.054% (t 3.7, 1.442/wk, years 3/3) *(below recurrence bar)* | +0.030 | none validated |
| ATHERENERG | OTHER | 0/103/250 | 0.040 | 0.110 | 0.301 | 0.525 | 75.5 | 0 (0) | breakout_dn · all → down over 30m, excess +0.184% (t 3.2, 3.227/wk, years 1/1) *(below recurrence bar)* | +0.095 | none validated |
| BAJFINANCE | FINANCIAL_SERVICES | 478/247/250 | 0.024 | 0.094 | 0.173 | 1.102 | 67.0 | 0 (0) | 5m candles dn-dn-flat · 11:00-13:00 → up over 15m, excess +0.045% (t 3.5, 1.513/wk, years 4/4) *(below recurrence bar)* | +0.034 | none validated |
| SAIL | METALS_MINING | 478/247/250 | 0.043 | 0.113 | 0.233 | 1.577 | 63.4 | 0 (0) | rvol_thrust_dn · nifty=down → up over 60m, excess +0.242% (t 3.9, 1.372/wk, years 3/3) *(below recurrence bar)* | +0.096 | none validated |
| TATASTEEL | METALS_MINING | 478/247/250 | 0.037 | 0.107 | 0.182 | 1.357 | 60.7 | 0 (0) | 5m candles up-flat-up · 11:00-13:00 → up over 15m, excess +0.057% (t 3.7, 1.41/wk, years 4/4) *(below recurrence bar)* | -0.017 | none validated |
| DELHIVERY | LOGISTICS_TRANSPORT | 478/247/250 | 0.030 | 0.100 | 0.219 | 0.931 | 77.1 | 1 (1) | vwap_reclaim_up · rvol=low → down over EOD, excess +0.312% (t 4.4, 1.301/wk, years 4/4) | +0.125 | none validated |
| INDUSINDBK | BANKING | 478/247/250 | 0.027 | 0.097 | 0.193 | 1.223 | 71.7 | 0 (0) | enter 10:15 on Mon → up over 15m, excess +0.077% (t 3.5, 0.891/wk, years 3/3) *(below recurrence bar)* | +0.047 | none validated |
| LT | CAPITAL_GOODS_ENGINEERING | 478/247/250 | 0.024 | 0.094 | 0.154 | 1.121 | 62.1 | 0 (0) | 5m candles up-up-dn · 13:00-14:30 → up over EOD, excess +0.228% (t 4.0, 0.391/wk, years 3/3) *(below recurrence bar)* | +0.253 | none validated |
| IDEA | OTHER | 478/247/250 | 0.100 | 0.170 | 0.384 | 1.463 | 84.8 | 0 (0) | enter 10:15 on Tue → down over 15m, excess +0.145% (t 3.6, 0.91/wk, years 3/3) *(below recurrence bar)* | -0.040 | none validated |
| COALINDIA | METALS_MINING | 478/247/250 | 0.030 | 0.100 | 0.180 | 0.898 | 68.6 | 0 (0) | squeeze_break_up · trend=down → down over 60m, excess +0.104% (t 3.8, 0.718/wk, years 3/3) *(below recurrence bar)* | -0.024 | none validated |
| NTPC | ENERGY | 478/247/250 | 0.028 | 0.098 | 0.178 | 1.017 | 73.1 | 0 (0) | squeeze_break_dn · vsSector=strong → down over 60m, excess +0.155% (t 3.5, 0.526/wk, years 3/3) *(below recurrence bar)* | +0.036 | none validated |
| ETERNAL | MEDIA_INTERNET | 478/247/250 | 0.043 | 0.113 | 0.235 | 1.242 | 68.5 | 0 (0) | rvol_thrust_up · dayvol=normal → up over 60m, excess +0.199% (t 3.7, 1.731/wk, years 3/3) *(below recurrence bar)* | +0.082 | none validated |
| ITC | CONSUMER_FMCG | 478/246/250 | 0.024 | 0.094 | 0.140 | 0.599 | 69.6 | 0 (0) | squeeze_break_dn · vsNifty=strong → up over 60m, excess +0.105% (t 3.5, 0.481/wk, years 3/3) *(below recurrence bar)* | +0.098 | none validated |
| POLYCAB | OTHER | 478/247/250 | 0.026 | 0.096 | 0.191 | 1.038 | 73.9 | 0 (0) | 5m candles flat-flat-bigDn · 13:00-14:30 → down over EOD, excess +0.390% (t 3.9, 0.449/wk, years 3/3) *(below recurrence bar)* | +0.010 | none validated |
| INDIGO | TRAVEL_LEISURE | 478/247/250 | 0.024 | 0.094 | 0.178 | 1.063 | 67.6 | 0 (0) | failed_breakout_up · rvol=normal → up over 60m, excess +0.063% (t 3.1, 2.744/wk, years 3/4) *(below recurrence bar)* | +0.021 | none validated |
| PAYTM | MEDIA_INTERNET | 478/247/250 | 0.032 | 0.102 | 0.245 | 1.293 | 68.0 | 0 (0) | trend down, 30m momentum strong, below VWAP, rvol high · 13:00-14:30 → down over EOD, excess +0.445% (t 3.7, 0.385/wk, years 3/3) *(below recurrence bar)* | +0.187 | none validated |
| MARUTI | AUTOMOBILE | 478/247/250 | 0.021 | 0.091 | 0.143 | 0.821 | 75.3 | 0 (0) | failed_breakout_up · tod=11:00-13:00 → up over 15m, excess +0.036% (t 3.9, 2.301/wk, years 4/4) *(below recurrence bar)* | +0.039 | none validated |
| BHEL | CAPITAL_GOODS_ENGINEERING | 478/247/250 | 0.044 | 0.115 | 0.258 | 1.577 | 71.1 | 0 (0) | vwap_reclaim_up · vsSector=inline → up over 15m, excess +0.088% (t 3.9, 1.506/wk, years 3/3) *(below recurrence bar)* | -0.002 | none validated |
| DIVISLAB | PHARMA_HEALTHCARE | 478/247/250 | 0.028 | 0.098 | 0.175 | 0.67 | 75.3 | 0 (0) | vwap_reclaim_up · vsNifty=inline → down over 30m, excess +0.087% (t 4.0, 1.673/wk, years 3/4) *(below recurrence bar)* | +0.026 | none validated |
| TITAN | OTHER | 478/247/250 | 0.024 | 0.094 | 0.157 | 0.699 | 69.6 | 1 (0) | 5m candles dn-dn-up · 11:00-13:00 → down over 60m, excess +0.147% (t 4.8, 0.519/wk, years 3/3) | -0.117 | none validated |
| BEL | DEFENCE_AEROSPACE | 478/247/250 | 0.039 | 0.109 | 0.193 | 1.157 | 67.7 | 0 (0) | trend up, 30m momentum strong, below VWAP, rvol normal · 13:00-14:30 → down over 15m, excess +0.081% (t 3.7, 0.397/wk, years 3/3) *(below recurrence bar)* | +0.019 | none validated |
| POLICYBZR | FINANCIAL_SERVICES | 478/247/250 | 0.032 | 0.102 | 0.246 | 1.027 | 74.8 | 0 (0) | trend mixed, 30m momentum neutral, above VWAP, rvol high · 13:00-14:30 → down over 15m, excess +0.148% (t 3.7, 0.442/wk, years 3/3) *(below recurrence bar)* | +0.090 | none validated |
| HCLTECH | INFORMATION_TECHNOLOGY | 478/247/250 | 0.023 | 0.093 | 0.166 | 0.956 | 62.8 | 0 (0) | pdl_break · tod=13:00-14:30 → up over 15m, excess +0.070% (t 3.3, 0.455/wk, years 3/3) *(below recurrence bar)* | +0.041 | none validated |
| ADANIENT | DIVERSIFIED | 478/247/250 | 0.033 | 0.103 | 0.219 | 1.676 | 68.3 | 0 (0) | ema20_pullback_up · nifty=up → down over 15m, excess +0.077% (t 4.2, 2.699/wk, years 3/4) *(below recurrence bar)* | -0.025 | none validated |
| LAURUSLABS | PHARMA_HEALTHCARE | 478/247/250 | 0.030 | 0.100 | 0.215 | 0.955 | 76.5 | 0 (0) | 5m candles bigUp-flat-flat · 09:45-11:00 → down over 30m, excess +0.164% (t 4.1, 0.628/wk, years 3/3) *(below recurrence bar)* | -0.069 | none validated |
| KALYANKJIL | CONSUMER_FMCG | 478/247/250 | 0.043 | 0.113 | 0.263 | 1.253 | 76.6 | 0 (0) | 5m candles flat-dn-dn · 11:00-13:00 → up over 15m, excess +0.095% (t 4.2, 1.359/wk, years 3/3) *(below recurrence bar)* | +0.083 | none validated |
| OFSS | INFORMATION_TECHNOLOGY | 478/247/250 | 0.022 | 0.092 | 0.198 | 1.139 | 72.2 | 0 (0) | or_trap_dn · tod=11:00-13:00 → down over 60m, excess +0.153% (t 4.2, 1.09/wk, years 3/3) *(below recurrence bar)* | -0.053 | none validated |
| VEDL | METALS_MINING | 478/247/249 | 0.030 | 0.101 | 0.202 | 1.251 | 59.4 | 0 (0) | trend_fail_dn · vsNifty=inline → down over 15m, excess +0.086% (t 3.5, 1.25/wk, years 3/3) *(below recurrence bar)* | -0.058 | none validated |
| TVSMOTOR | AUTOMOBILE | 478/247/250 | 0.026 | 0.096 | 0.191 | 0.987 | 75.1 | 0 (0) | enter 13:00 on Tue → up over 30m, excess +0.109% (t 3.8, 0.91/wk, years 3/3) *(below recurrence bar)* | -0.059 | none validated |
| HINDUNILVR | CONSUMER_FMCG | 478/247/249 | 0.020 | 0.090 | 0.140 | 0.472 | 72.7 | 0 (0) | 5m candle flat · 14:30-15:05 → down over 15m, excess +0.019% (t 3.4, 4.481/wk, years 3/4) *(below recurrence bar)* | -0.007 | none validated |
| EICHERMOT | AUTOMOBILE | 478/247/250 | 0.026 | 0.096 | 0.172 | 0.968 | 64.8 | 0 (0) | trend up, 30m momentum strong, above VWAP, rvol high · 09:15-09:45 → down over 60m, excess +0.213% (t 3.9, 0.987/wk, years 3/3) *(below recurrence bar)* | -0.088 | none validated |
| TMPV | AUTOMOBILE | 478/247/249 | 0.025 | 0.095 | 0.169 | 1.183 | 64.9 | 1 (1) | pdl_break · nifty=flat → down over EOD, excess +0.314% (t 5.0, 0.846/wk, years 3/3) | +0.243 | none validated; promising hypothesis |
| TATAELXSI | INFORMATION_TECHNOLOGY | 478/247/250 | 0.026 | 0.096 | 0.151 | 0.979 | 66.3 | 0 (0) | failed_breakout_up · vsNifty=weak → up over EOD, excess +0.165% (t 3.7, 1.179/wk, years 3/4) *(below recurrence bar)* | +0.019 | none validated |
| SOLARINDS | OTHER | 478/247/250 | 0.026 | 0.096 | 0.207 | 0.722 | 76.9 | 0 (0) | breakout_up · dayvol=compressed → up over 30m, excess +0.119% (t 4.1, 1.103/wk, years 3/3) *(below recurrence bar)* | +0.011 | none validated |
| HAL | DEFENCE_AEROSPACE | 478/247/250 | 0.029 | 0.099 | 0.192 | 1.118 | 66.3 | 0 (0) | ema20_pullback_up · vsNifty=strong → up over 15m, excess +0.073% (t 3.6, 2.353/wk, years 4/4) *(below recurrence bar)* | +0.024 | none validated |
| ADANIPOWER | ENERGY | 478/247/250 | 0.055 | 0.125 | 0.245 | 1.33 | 68.9 | 0 (0) | trend_fail_dn · rvol=low → down over 60m, excess +0.146% (t 4.1, 1.263/wk, years 3/3) *(below recurrence bar)* | -0.071 | none validated |
| ULTRACEMCO | CEMENT | 478/247/250 | 0.023 | 0.093 | 0.150 | 1.034 | 66.7 | 0 (0) | trend up, 30m momentum neutral, below VWAP, rvol normal · 14:30-15:05 → down over 15m, excess +0.058% (t 3.9, 0.583/wk, years 3/3) *(below recurrence bar)* | +0.010 | none validated |
| COFORGE | INFORMATION_TECHNOLOGY | 478/247/250 | 0.030 | 0.100 | 0.213 | 1.109 | 66.1 | 0 (0) | squeeze_break_up · trend=down → down over 60m, excess +0.122% (t 3.8, 0.833/wk, years 3/3) *(below recurrence bar)* | +0.050 | none validated |
| HINDALCO | METALS_MINING | 478/247/250 | 0.025 | 0.095 | 0.195 | 1.265 | 57.4 | 0 (0) | 5m candles dn-flat-dn · 13:00-14:30 → up over 30m, excess +0.099% (t 4.0, 1.16/wk, years 3/3) *(below recurrence bar)* | -0.021 | none validated |
| PFC | ENERGY | 478/247/250 | 0.032 | 0.102 | 0.236 | 1.674 | 66.7 | 1 (1) | breakout_dn · vwap=above → up over 15m, excess +0.102% (t 4.5, 1.115/wk, years 4/4) | +0.057 | none validated |
| DIXON | OTHER | 478/247/250 | 0.029 | 0.099 | 0.218 | 1.478 | 68.1 | 0 (0) | squeeze_break_up · vwap=below → down over EOD, excess +0.216% (t 4.3, 0.981/wk, years 3/4) *(below recurrence bar)* | -0.027 | none validated |
| INDUSTOWER | TELECOM | 478/247/250 | 0.033 | 0.103 | 0.234 | 1.13 | 71.3 | 0 (0) | enter 12:00 on Mon → up over 30m, excess +0.162% (t 3.6, 0.891/wk, years 3/3) *(below recurrence bar)* | +0.003 | none validated |
| PNBHOUSING | FINANCIAL_SERVICES | 478/247/250 | 0.034 | 0.104 | 0.248 | 1.274 | 72.2 | 0 (0) | pdl_reclaim · vsNifty=weak → down over 60m, excess +0.304% (t 3.5, 0.756/wk, years 3/3) *(below recurrence bar)* | -0.041 | none validated |
| BHARATFORG | AUTOMOBILE | 478/247/250 | 0.028 | 0.098 | 0.208 | 1.302 | 73.0 | 0 (0) | pdh_reject · vwap=below → down over EOD, excess +0.380% (t 3.7, 1.301/wk, years 3/3) *(below recurrence bar)* | +0.125 | none validated |
| DRREDDY | PHARMA_HEALTHCARE | 478/247/250 | 0.023 | 0.093 | 0.153 | 0.491 | 67.4 | 0 (0) | rvol_thrust_up · nifty=down → down over EOD, excess +0.255% (t 3.8, 0.936/wk, years 3/3) *(below recurrence bar)* | +0.089 | none validated |
| KAYNES | CAPITAL_GOODS_ENGINEERING | 453/247/250 | 0.035 | 0.105 | 0.264 | 1.232 | 72.1 | 0 (0) | squeeze_break_dn · vsNifty=inline → down over 60m, excess +0.208% (t 4.0, 0.88/wk, years 3/3) *(below recurrence bar)* | -0.076 | none validated |
| RECLTD | ENERGY | 478/247/250 | 0.035 | 0.105 | 0.223 | 1.503 | 69.2 | 0 (0) | ema20_pullback_up · nifty=flat → up over 30m, excess +0.084% (t 3.5, 3.506/wk, years 4/4) *(below recurrence bar)* | -0.004 | none validated |
| ONGC | ENERGY | 478/247/250 | 0.029 | 0.099 | 0.171 | 0.856 | 64.5 | 0 (0) | ema20_pullback_up · tod=09:45-11:00 → down over 15m, excess +0.058% (t 3.8, 1.801/wk, years 3/3) *(below recurrence bar)* | +0.006 | none validated |
| AUROPHARMA | PHARMA_HEALTHCARE | 478/247/250 | 0.026 | 0.096 | 0.199 | 0.666 | 76.5 | 2 (0) | rvol_thrust_up · all → up over 15m, excess +0.106% (t 4.3, 2.692/wk, years 4/4) | -0.012 | none validated |
| SUNPHARMA | PHARMA_HEALTHCARE | 478/247/250 | 0.023 | 0.093 | 0.149 | 0.543 | 75.5 | 0 (0) | squeeze_break_up · tod=13:00-14:30 → up over EOD, excess +0.134% (t 4.1, 1.032/wk, years 3/3) *(below recurrence bar)* | -0.011 | none validated |
| POWERINDIA | CAPITAL_GOODS_ENGINEERING | 478/247/250 | 0.017 | 0.087 | 0.235 | 0.782 | 78.5 | 0 (0) | ema20_pullback_dn · nifty=flat → down over 30m, excess +0.124% (t 4.3, 3.038/wk, years 4/4) *(below recurrence bar)* | -0.059 | none validated |
| LICHSGFIN | FINANCIAL_SERVICES | 478/247/250 | 0.025 | 0.096 | 0.196 | 1.188 | 72.4 | 0 (0) | squeeze_break_up · vwap=below → down over 30m, excess +0.086% (t 3.9, 0.968/wk, years 3/3) *(below recurrence bar)* | +0.110 | none validated |
| ANGELONE | OTHER | 478/247/250 | 0.038 | 0.108 | 0.247 | 1.426 | 72.1 | 0 (0) | trend up, 30m momentum strong, above VWAP, rvol high · 14:30-15:05 → up over 30m, excess +0.292% (t 4.1, 0.872/wk, years 3/3) *(below recurrence bar)* | +0.186 | none validated |
| TORNTPHARM | PHARMA_HEALTHCARE | 478/247/250 | 0.022 | 0.092 | 0.179 | 0.393 | 78.7 | 0 (0) | or_trap_dn · vsSector=weak → up over 60m, excess +0.165% (t 4.1, 1.115/wk, years 3/3) *(below recurrence bar)* | +0.069 | none validated |
| ADANIGREEN | ENERGY | 478/247/250 | 0.037 | 0.107 | 0.237 | 1.499 | 67.0 | 0 (0) | trend mixed, 30m momentum weak, below VWAP, rvol normal · 09:45-11:00 → down over 60m, excess +0.156% (t 3.8, 0.628/wk, years 3/3) *(below recurrence bar)* | -0.059 | none validated |
| ANANDRATHI | OTHER | 474/247/250 | 0.020 | 0.090 | 0.208 | 0.337 | 74.8 | 2 (1) | deep_pullback_up · trend=up → down over 15m, excess +0.078% (t 4.6, 3.506/wk, years 3/4) | +0.026 | none validated |
| PERSISTENT | INFORMATION_TECHNOLOGY | 478/247/250 | 0.031 | 0.101 | 0.214 | 1.179 | 64.8 | 0 (0) | sweep_high_reject · rvol=high → up over EOD, excess +0.190% (t 3.8, 2.269/wk, years 4/4) *(below recurrence bar)* | +0.026 | none validated |
| TRENT | CONSUMER_FMCG | 478/247/250 | 0.029 | 0.099 | 0.206 | 1.152 | 79.8 | 1 (0) | ema20_pullback_up · rvol=low → down over 15m, excess +0.062% (t 4.6, 2.647/wk, years 4/4) | -0.010 | none validated |
| CHOLAFIN | FINANCIAL_SERVICES | 478/247/250 | 0.030 | 0.100 | 0.216 | 1.18 | 68.8 | 0 (0) | pdh_reject · tod=09:15-09:45 → down over 30m, excess +0.180% (t 3.5, 1.032/wk, years 3/3) *(below recurrence bar)* | -0.014 | none validated |
| SHRIRAMFIN | OTHER | 478/247/250 | 0.028 | 0.098 | 0.217 | 1.532 | 67.8 | 0 (0) | 5m candles dn-dn-up · 13:00-14:30 → down over 15m, excess +0.098% (t 3.5, 0.436/wk, years 3/3) *(below recurrence bar)* | +0.072 | none validated |
| ASHOKLEY | AUTOMOBILE | 478/247/250 | 0.045 | 0.115 | 0.180 | 1.181 | 69.9 | 1 (1) | or_trap_dn · rvol=low → down over 30m, excess +0.105% (t 4.7, 1.308/wk, years 4/4) | +0.030 | none validated |
| NESTLEIND | CONSUMER_FMCG | 478/247/250 | 0.024 | 0.094 | 0.150 | 0.536 | 81.3 | 0 (0) | or_trap_up · trend=up → up over 30m, excess +0.076% (t 3.6, 1.558/wk, years 4/4) *(below recurrence bar)* | -0.040 | none validated |
| MAXHEALTH | PHARMA_HEALTHCARE | 478/247/250 | 0.028 | 0.098 | 0.209 | 0.695 | 75.3 | 0 (0) | 5m candles up-flat-up · 11:00-13:00 → down over 15m, excess +0.090% (t 3.6, 1.128/wk, years 3/3) *(below recurrence bar)* | -0.022 | none validated |
| VBL | CONSUMER_FMCG | 478/247/250 | 0.029 | 0.099 | 0.194 | 0.808 | 71.0 | 0 (0) | rvol_thrust_up · tod=13:00-14:30 → down over 30m, excess +0.147% (t 4.0, 0.801/wk, years 3/3) *(below recurrence bar)* | -0.039 | none validated |
| HDFCAMC | FINANCIAL_SERVICES | 478/247/250 | 0.027 | 0.097 | 0.194 | 1.283 | 72.8 | 0 (0) | rvol_thrust_up · vsSector=weak → up over 60m, excess +0.220% (t 3.5, 0.885/wk, years 3/3) *(below recurrence bar)* | +0.070 | none validated |
| HINDPETRO | ENERGY | 478/247/250 | 0.033 | 0.103 | 0.214 | 1.344 | 71.4 | 0 (0) | enter 09:30 on Thu → up over 15m, excess +0.165% (t 3.7, 0.91/wk, years 3/3) *(below recurrence bar)* | -0.028 | none validated |
| FEDERALBNK | BANKING | 478/247/250 | 0.034 | 0.104 | 0.196 | 0.892 | 75.5 | 0 (0) | sweep_high_reject · vsNifty=inline → down over 30m, excess +0.074% (t 4.2, 1.853/wk, years 3/3) *(below recurrence bar)* | -0.054 | none validated |
| JIOFIN | OTHER | 271/247/250 | 0.031 | 0.101 | 0.180 | 1.411 | 59.2 | 0 (0) | 5m candles flat-flat-up · 11:00-13:00 → down over 15m, excess +0.047% (t 4.2, 3.225/wk, years 2/3) *(below recurrence bar)* | -0.014 | none validated |
| SONACOMS | AUTOMOBILE | 478/247/250 | 0.030 | 0.100 | 0.218 | 1.044 | 70.3 | 0 (0) | enter 13:45 on Fri → down over 60m, excess +0.129% (t 3.7, 0.891/wk, years 3/3) *(below recurrence bar)* | -0.029 | none validated |
| GMRAIRPORT | OTHER | 478/247/250 | 0.060 | 0.130 | 0.233 | 1.319 | 75.3 | 0 (0) | trend_fail_up · nifty=down → up over 30m, excess +0.086% (t 3.5, 1.955/wk, years 4/4) *(below recurrence bar)* | -0.009 | none validated |
| MANAPPURAM | FINANCIAL_SERVICES | 478/247/250 | 0.032 | 0.102 | 0.237 | 1.322 | 68.6 | 0 (0) | deep_pullback_up · tod=13:00-14:30 → down over 15m, excess +0.066% (t 3.9, 2.494/wk, years 4/4) *(below recurrence bar)* | -0.008 | none validated |
| GLENMARK | OTHER | 478/247/250 | 0.026 | 0.097 | 0.215 | 0.792 | 75.1 | 0 (0) | or_trap_dn · trend=up → down over 15m, excess +0.093% (t 3.9, 1.064/wk, years 3/3) *(below recurrence bar)* | +0.017 | none validated |
| HEROMOTOCO | AUTOMOBILE | 478/247/250 | 0.025 | 0.095 | 0.177 | 0.983 | 66.6 | 0 (0) | failed_breakout_up · dayvol=expanded → down over 60m, excess +0.175% (t 3.4, 0.737/wk, years 3/3) *(below recurrence bar)* | -0.085 | none validated |
| UPL | CHEMICALS | 478/247/250 | 0.024 | 0.094 | 0.180 | 1.144 | 71.1 | 0 (0) | or_trap_dn · nifty=flat → down over 15m, excess +0.052% (t 3.5, 1.724/wk, years 4/4) *(below recurrence bar)* | -0.014 | none validated |
| POWERGRID | ENERGY | 478/247/250 | 0.024 | 0.094 | 0.162 | 0.955 | 71.6 | 0 (0) | trend_fail_up · vsNifty=inline → up over 15m, excess +0.057% (t 4.2, 1.404/wk, years 4/4) *(below recurrence bar)* | +0.020 | none validated |
| INDHOTEL | OTHER | 478/247/250 | 0.025 | 0.096 | 0.191 | 1.164 | 72.5 | 0 (0) | 5m candles flat-bigUp-flat · 13:00-14:30 → up over 30m, excess +0.131% (t 3.8, 0.417/wk, years 3/3) *(below recurrence bar)* | -0.072 | none validated |
| TIINDIA | AUTOMOBILE | 478/247/250 | 0.030 | 0.100 | 0.210 | 0.972 | 77.8 | 8 (6) | enter 13:15 daily → down over 60m, excess +0.128% (t 6.6, 4.513/wk, years 4/4) | -0.003 | none validated |
| GAIL | ENERGY | 478/247/250 | 0.037 | 0.107 | 0.189 | 1.195 | 73.0 | 0 (0) | enter 10:15 on Mon → up over 15m, excess +0.125% (t 4.0, 0.891/wk, years 3/3) *(below recurrence bar)* | -0.044 | none validated |
| DLF | REALTY | 478/247/250 | 0.025 | 0.095 | 0.204 | 1.609 | 66.8 | 1 (0) | squeeze_break_dn · tod=09:45-11:00 → up over 30m, excess +0.118% (t 4.5, 0.808/wk, years 3/3) | -0.026 | none validated |
| RBLBANK | BANKING | 478/247/250 | 0.033 | 0.103 | 0.258 | 1.588 | 74.3 | 0 (0) | enter 11:15 on Thu → down over 15m, excess +0.107% (t 4.0, 0.91/wk, years 3/3) *(below recurrence bar)* | +0.016 | none validated |
| ENRIN | OTHER | 0/71/250 | 0.028 | 0.098 | 0.225 | 0.717 | 82.6 | 0 (0) | — | — | none validated |
| ABB | CAPITAL_GOODS_ENGINEERING | 478/247/250 | 0.027 | 0.097 | 0.201 | 1.038 | 74.8 | 0 (0) | enter 13:30 on Fri → down over 15m, excess +0.063% (t 3.6, 0.891/wk, years 3/3) *(below recurrence bar)* | +0.002 | none validated |
| LICI | OTHER | 478/247/250 | 0.032 | 0.102 | 0.161 | 0.962 | 64.2 | 0 (0) | or_trap_dn · nifty=flat → down over 15m, excess +0.048% (t 3.8, 1.564/wk, years 3/3) *(below recurrence bar)* | -0.019 | none validated |
