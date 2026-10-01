# Features and methods

Feature version `1`. The catalogue is also served at `GET /v2/meta`.

| Feature | Unit | Min bars | Purpose |
| --- | --- | --- | --- |
| `ret_1m` | log return | 1 | Size of the latest minute's move; base input for return shocks. |
| `ret_5m` | log return | 6 | Short-horizon move that a single noisy minute cannot fake. |
| `ret_15m` | log return | 16 | Move over a quarter hour; the window used for relative performance. |
| `ret_session` | log return | 1 | Move since today's open: the day's direction and size. |
| `ret_prev_close` | log return | 1 | Move since yesterday's close, including the opening gap. |
| `gap` | log return | 0 | Overnight gap; separates opening repricing from intraday moves. |
| `range_1m` | fraction of price | 1 | Latest bar's high-low range; intrabar volatility even when close-to-close is flat. |
| `rvol_15m` | log return std per minute | 10 | Recent realised volatility; the scale against which a move is judged. |
| `rvol_session` | log return std per minute | 10 | Volatility so far today; detects regime change within the day. |
| `volume_1m` | shares | 1 | Latest minute's traded volume; base input for volume surges. |
| `volume_session` | shares | 1 | Cumulative volume today. |
| `turnover_1m` | INR | 1 | Value traded in the latest minute; comparable across share prices. |
| `activity_30m` | fraction of minutes | 1 | Share of the last 30 minutes with any trade; a direct liquidity measure. |
| `illiquidity_15m` | abs log return per INR crore | 10 | Amihud-style price impact: how far price moves per value traded. |
| `vwap_distance` | log ratio | 1 | Price relative to today's volume-weighted average; where trading has concentrated. |
| `rel_market_15m` | log return | 16 | 15-minute move minus the universe median: idiosyncratic vs market-wide. |
| `rel_market_session` | log return | 1 | Session move minus the universe median. |
| `rel_sector_session` | log return | 1 | Session move minus the sector median; NaN without a known sector. |
| `rel_index_session` | log return | 1 | Session move minus the broad index (NIFTY 500). |
| `rel_sector_index_session` | log return | 1 | Session move minus the sector's own index, where one exists. |

| Market feature | Meaning |
| --- | --- |
| `median_ret_1m` | Typical one-minute move across the universe. |
| `median_ret_session` | Typical move since the open. |
| `dispersion_15m` | Cross-sectional standard deviation of 15-minute returns: how differently stocks are moving. |
| `dispersion_session` | Cross-sectional standard deviation of session returns. |
| `breadth_session` | (advancers - decliners) / (advancers + decliners) on session returns. |
| `active_share_1m` | Share of instruments with a bar in the latest minute. |
| `median_rvol_15m` | Typical recent realised volatility. |
| `turnover_1m` | Total value traded in the latest minute. |

Returns are log returns. A feature that needs more bars than are available is
NaN (served as `null`), never extrapolated. Sector features use the sector
taxonomy; symbols in `OTHER` (about 736 of 989) have no sector, so sector
comparisons cover about a quarter of the universe.

## Baselines (`history.py`)

For each instrument and each minute of the day, the median and scaled MAD
(`1.4826 × MAD`) of log volume, |1m return| and log bar range over the last
20 earlier sessions, pooling ±2 minutes around the minute. At least 5 earlier
sessions are required; otherwise the baseline is unavailable and anomaly
scoring falls back (below). Per-session summaries are cached as
`summaries/<date>.v3.npz`.

## Anomalies (`anomaly.py`)

| Measure | Scored value | z | Fallback without history |
| --- | --- | --- | --- |
| volume | log(1 + volume) | (value − median) / scale | the instrument's earlier minutes today (≥ 20 bars) |
| return | signed 1m log return | r / σ, σ = median(\|r\|) / 0.6745 | the cross-section at this minute |
| range | log((high − low) / close) | (value − median) / scale | none: `INSUFFICIENT_DATA` |

|z| < 3 is `NORMAL`, < 5 `UNUSUAL`, otherwise `EXTREME`. Before scoring:
latest bar missing → `STALE`; latest bar rejected → `INVALID`; no usable
baseline → `INSUFFICIENT_DATA`. Market measures (dispersion, breadth) are
scored against their own per-minute baselines. On synthetic data with no
injected anomalies, 0.6–1.4% of scores are UNUSUAL and ≤ 0.3% EXTREME.

## Relationships (`relationships.py`)

**Stock vs benchmark** (sector = leave-one-out median of the other members;
NIFTY 500; the sector index). Beta, correlation and residual SD are fitted on
the 60 minutes *before* the 15 being judged. The relationship is judged only
if at least 40 estimation pairs, 10 recent pairs and correlation ≥ 0.3;
otherwise `NOT_ESTABLISHED` or `INSUFFICIENT_DATA`. The statistic is the
cumulative residual over the 15 minutes divided by its prediction SD,
`residual_sd × √(n + B² / Sxx)`, where `B` is the benchmark's move over the
15 minutes and `Sxx` its sum of squares in the estimation window (the second
term is the error in the fitted beta).

**Sector vs sector.** The 15-minute spread of two sectors' median returns
against what the 1m spreads earlier today imply: mean × 15, with SD
SD × √15 (at least 60 earlier minutes). Measured null flag rates per check
(|z| ≥ 3, nominal 0.27%): stock divergences 0.3–0.65%, sector pairs 0.41%.

**Price vs volume.** `move_without_volume`: |return z| ≥ 3 while |volume z| <
1; `volume_without_move`: volume z ≥ 3 while |return z| < 1.

**Derivatives** (from the recorded snapshots). Spot-futures basis
`(futures − spot) / spot`, futures top-of-book spread, put-call OI ratio and
IV skew, each as the latest value against the median and scaled MAD of its
earlier values today (at least 20 snapshots).

## Similarity (`similarity.py`)

| Scope | State vector | Outcomes |
| --- | --- | --- |
| Market | index session return, index 15m return, index 15m realised vol, breadth, 15m dispersion | index forward return 15/30/60m, forward 30m realised vol |
| Instrument | 15m return, session return, 15m realised vol, session return vs NIFTY 500, 15m volume vs session average | forward return 15/30/60m, forward 30m return vs NIFTY 500 |

Features are standardised by robust scale over the candidate pool. Each
earlier session (up to 60) offers candidates within ±30 minutes of the same
time of day; only its closest minute counts, so matches are independent
sessions. The closest `k` (default 10) are the matches. Results: quantiles
(p10–p90), mean and share positive of each outcome for the matches *and* for
the base rate (every earlier session at this minute), the separation
`P(matched > base)` (0.5 = no information), match quality (median match
distance / median over all sessions), and flags (`FEW_SESSIONS` under 20).
Fewer than 5 usable sessions gives `INSUFFICIENT_HISTORY` and no
distribution. Instrument states are cached every 5 minutes of the day.
