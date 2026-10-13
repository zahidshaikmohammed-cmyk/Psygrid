# Pre-registration — six mechanistic hypotheses (frozen before the confirmation fetch)

- **Code (frozen):** `research/behaviour/prereg.py` (definitions), `execution.py` (fills, stops, costs), `integrity.py` (corporate actions), `confirm.py` (windows, eligibility, decision rules), `power.py`.
- **Lock:** `research/behaviour/prereg.lock` holds the SHA-256 of each frozen file. `tests/test_behaviour_prereg.py::test_definitions_are_frozen` fails if any of them change.
- **Amendments:** any amendment before the confirmation fetch must be its own commit, with the reason stated. **After the fetch, nothing changes.** A genuine bug found afterwards would be fixed in a new commit, and **both** the original and the corrected results would be reported.

## 1. Hypotheses

The exact rules are in `prereg.HYPOTHESES` and the `h1`–`h6` functions. In summary:

| | Mechanism | Signal (5-minute bar close, causal) | Stop | Target | Max hold |
|---|---|---|---|---|---|
| H1 | Liquidity sweep → reclaim → failed continuation | sweep beyond the 12-bar extreme, closed back inside; the next bar does not extend it and closes beyond the sweep bar's close | sweep extreme + 0.1 × typical 5-minute range | 1.5R | 60 min |
| H2 | Trend-day pullback with relative-volume confirmation | trend ≥ 4 typical ranges from the open; EMA9/20 aligned; 30–70% light-volume pullback; close back through EMA9 with relative volume ≥ 1.2 | pullback extreme + pad | 2R | to 15:15 |
| H3 | Failed breakout while weak against the sector | 12-bar breakout that closes back inside within 3 bars while the stock is ≥ 1 typical range weaker than its sector (mirror: failed breakdown while stronger) | extreme since the breakout + pad | 1.5R | 60 min |
| H4 | VWAP rejection in NIFTY's direction on a normal or active day | NIFTY day-so-far and 30-minute momentum both down; 3 closes below VWAP; bar tags VWAP and closes below it, red (mirror for long) | bar extreme + pad | 1.5R | 60 min |
| H5 | Multi-month drift continuation | z of the 60-session return in excess of beta × NIFTY ≥ 1.5 → long, ≤ −1.5 → short, at 09:30 | none | none | to 15:15 |
| H6 | Compression → volume-confirmed expansion break | 6-bar range < 0.6 × its usual size on a quiet day (< 0.8), break of the box with relative volume ≥ 1.5 | opposite side of the box + pad | 2R | 60 min |

**Shared rules**
- **Universe:** equities only.
- **Frequency:** one trade per hypothesis per stock per session (the first signal).
- **No parameter is fitted to any data.**

**Execution (`execution.py`)**
- Entry is one full minute after the signal bar closes, at that minute's open, with adverse slippage. No entries after 15:00.
- Stops and targets are evaluated on the 1-minute path:
  - a stop fills at the stop price, or at the open if the price gaps through it;
  - a target fills only if the price trades through it;
  - if a minute touches both, the stop is assumed.
- Time exits are at the last minute's close, no later than 15:15.

**Costs** (round trip, in % of the underlying; spread is each stock's own 1-minute estimate from the evaluation window, floored at one ₹0.05 tick and capped at 0.20%):

| Scenario | Fees | Slippage per market fill | Spread multiple |
|---|---|---|---|
| zero | 0 | 0 | 0 — *diagnostic only, never presented as executable* |
| low | 0.02 | 0 | 0.5 |
| **base** | **0.05** | **0.02** | **1.0** |
| high | 0.10 | 0.05 | 2.0 |

Every trade records gross, fees, slippage, spread and net separately. **Options P&L is not modelled.** The tests are on the underlying, and an underlying edge does not imply an options edge.

## 2. Why the confirmation year comes before the discovery period, and how it is used

The only historical data no one has inspected is **2021-10-01 .. 2022-10-12**. All of 2022-10-13 .. 2026-10-12 has been looked at. Using an earlier year as the test is a *backward* out-of-sample check. That is legitimate for stable mechanisms, but it brings four risks, and each is handled in advance:

1. **Regime differences.**
   - 2021–22 was a different market: the post-COVID rally, then the 2022 drawdown.
   - **Handling:** a mechanism that only works in one regime is not considered confirmed. The decision requires:
     - a positive base-cost mean in **both halves** of the window;
     - **the same direction** in the already-seen in-sample period (2022-10 .. 2025-09, gross mean > 0);
     - no single month contributing more than 35% of net P&L.
   - Regime-level results are reported descriptively. They do not change the verdict.
2. **Listing eligibility.**
   - Several of today's top-99 did not exist, or traded thinly, in 2021–22 (e.g. ATHERENERG, ENRIN, SWIGGY, WAAREEENER, PREMIERENE, JIOFIN, KAYNES).
   - **Handling:** an instrument with fewer than 150 complete sessions in the window is **NOT_APPLICABLE**, never a failure.
   - Each stock's costs use that window's own spread estimate (stocks that later became liquid pay their 2021–22 spread).
   - A hypothesis with fewer than 100 trades is NOT_APPLICABLE.
3. **Corporate actions.**
   - **Detection:** a session whose overnight move, net of NIFTY's overnight move, is at least 8% is flagged; near-exact split ratios are also labelled.
   - **Exclusion:** signals within ±5 sessions are suppressed, and the day's return is removed from every rolling window (beta and drift).
   - Flags are listed per instrument. Real news gaps of that size are excluded too, which is conservative.
4. **Survivorship.**
   - The universe was chosen in 2026, so in 2021–22 it over-represents stocks that later did well.
   - This biases **H5** (drift) towards passing. **H5's best possible verdict in this window is therefore INCONCLUSIVE.**
   - For H1–H4 and H6 the trades are both long and short and event-driven, so the bias is smaller but not zero. It is stated as a residual risk.

**How the windows are used**

| Window | Dates | Use |
|---|---|---|
| CONFIRMATION | 2021-10-01 .. 2022-10-12 | decisive (the first 20 sessions are warm-up; H5 needs 60) |
| IN-SAMPLE | 2022-10-13 .. 2025-09-30 | direction check only (gross mean > 0); never tuned on |
| CONTAMINATED | 2025-10-01 .. 2026-10-12 | reported, labelled contaminated, never decisive |

**A CONFIRMED verdict is still not a trading approval.** It must then pass forward paper-tracking of at least 3 months, and an options-feasibility check on real option prices. Both need separate approval.

## 3. Statistical power — what six hypotheses can and cannot detect

Six hypotheses do not provide power by themselves. **The power comes from pooling** each hypothesis's trades across all eligible stocks. Formula (`power.py`):
`SE = σ·√(ρ + (1−ρ)/m)/√W`, with MDE = (z₁₋α + z₀.₈₀)·SE at α = 0.05/6 (Holm's first step).

**Assumptions**
- W ≈ 45 tradable weeks.
- σ ≈ 0.42% per 60-minute trade. The median typical 5-minute range is 0.195%, so σ₅ₘ ≈ 0.12% and √12 × 0.12% ≈ 0.42%.
- σ ≈ 0.9% for hold-to-close trades.
- ρ = 0.05 / 0.15 / 0.30, the average same-week correlation between trades: unknown, so given as a range.
- **Trades per week (m)** were estimated from signal rates on synthetic data, scaled to about 90 eligible stocks, and rounded down. They will be re-measured on the seen in-sample period before confirmation.

| | σ % | m / week | MDE, ρ = 0.05 | ρ = 0.15 | ρ = 0.30 |
|---|---|---|---|---|---|
| H1 | 0.42 | 300 | 0.047% | 0.079% | 0.111% |
| H2 | 0.90 | 20 | 0.136% | 0.190% | 0.251% |
| H3 | 0.42 | 120 | 0.049% | 0.080% | 0.112% |
| H4 | 0.42 | 40 | 0.055% | 0.084% | 0.114% |
| H5 | 0.90 | 45 | 0.116% | 0.178% | 0.244% |
| H6 | 0.42 | 8 | 0.083% | 0.103% | 0.126% |

**Interpretation**
- With 80% power, the pooled tests can detect a **net** mean of roughly 0.05–0.11% per trade for the 60-minute hypotheses, and 0.12–0.25% for the hold-to-close ones (H2, H5).
- The earlier search found typical recurring effects of 0.05–0.15% **gross**. A real but small edge could therefore be **missed**. A NOT-confirmed result means "no edge larger than the MDE", not "no edge".
- **Per-stock tests are underpowered** (one stock, about 1.5 trades a week: MDE ≈ 0.17% for 60-minute trades, 0.43% for hold-to-close). Per-stock results are descriptive only and never decisive.

## 4. Multiple testing and the decision rule (fixed in `confirm.py`)

- **Primary metric:** the mean **net** return per trade at **base** cost, pooled across eligible stocks, in the CONFIRMATION window.
- **Test:** one-sided, with standard errors clustered by calendar week. **Holm–Bonferroni** across the six hypotheses at family α = 0.05.

**CONFIRMED** requires every check:
1. ≥ 300 trades, ≥ 30 weeks and ≥ 15 instruments.
2. Holm-adjusted p ≤ 0.05.
3. The lower bound of the 90% moving-block-bootstrap confidence interval of the base-cost net mean is above 0.
4. Mean net at **high** cost is above 0.
5. No instrument contributes more than 20%, and no month more than 35%, of total net P&L.
6. Mean net is above 0 in both halves of the window.
7. In-sample gross mean is above 0 in the same direction.

**Other verdicts**
- **REJECTED:** base-cost net mean ≤ 0.
- **INCONCLUSIVE:** positive but fails any check.
- **NOT_APPLICABLE:** fewer than 100 trades.
- H5 is capped at INCONCLUSIVE (survivorship).

**The confirmation results will not be used to change any definition, parameter, cost or threshold.**
