# PSYGRID Data Dictionary

Field-level reference for every live endpoint. `storage: RAM_ONLY` and `synthetic_data: false` (or `synthetic_candles: false`) apply to every payload in this document unless stated otherwise — nothing is written to disk and nothing is fabricated; a missing value is `null`/absent, never guessed.

---

## 1. Equity candle object (`candles_1m`, `5m`, `15m`, `1h` arrays — equity, index, MIDCPNIFTY underlying)

| Field | Type | Example | Meaning | Source | Required |
|---|---|---|---|---|---|
| `timestamp` | string | `"2026-09-19 09:16:00 IST"` | Candle open time, IST, human-readable | Exchange tick time (WebSocket) or Dhan historical API | Yes |
| `open`/`high`/`low`/`close` | float | `25012.35` | OHLC for the bar | Same | Yes |
| `volume` | int | `128400` | Traded volume in the bar | Same | Yes |

**Validation applied before a candle is ever stored:** `high >= max(open, close)`, `low <= min(open, close)`, `high >= low`, `volume >= 0`, no duplicate timestamp per instrument (duplicate-minute candles are merged, historical-API source wins over WebSocket-derived on conflict), no future timestamp (rejected with a 5s tolerance for clock skew).

---

## 2. Equity endpoints (`/public/live.json`, `/public/live-{shard}.json`, `/public/stock/{symbol}.json`)

| Field | Type | Meaning | Source |
|---|---|---|---|
| `session.status` | string | `LIVE`/`CLOSED`/`AUTHENTICATING`/`AUTH_ERROR` | Session manager |
| `session.current_time_ist` | string | Server clock at request time, IST | Server |
| `stocks.{SYMBOL}.security_id` | string | Dhan's numeric instrument ID | Dhan instrument master |
| `stocks.{SYMBOL}.previous_close`, `today_open` | float | Reference prices | Dhan quote snapshot / WebSocket previous-close packet |
| `stocks.{SYMBOL}.candles_1m` | array | See §1 | WebSocket Full feed, backfilled via Dhan historical API on gaps |

## 3. 16-index endpoints (`/public/{index}.json`)

| Field | Type | Meaning | Source |
|---|---|---|---|
| `ltp`, `ltp_timestamp` | float, string | Last traded price and its exchange timestamp | WebSocket Full feed |
| `feed.status` | string | `CONNECTED`/`CONNECTING`/`RECONNECTING`/`ERROR` | WebSocket connection state |
| `feed.last_tick_received_epoch` | float | Unix epoch of last packet *received by Psygrid* (ingestion time, not exchange time) | Server clock |
| `1m`/`5m`/`15m`/`1h` | array of §1 | Multi-timeframe OHLCV | 1m from WebSocket; higher timeframes from Dhan historical API |
| `candle_source.{tf}` | string | Which upstream produced that timeframe | Static per timeframe |

## 4. Option-chain endpoints (`/public/{nifty,banknifty,midcpnifty}-options.json`)

| Field | Type | Example | Meaning | Source | Units |
|---|---|---|---|---|---|
| `underlying_ltp` | float | `25012.4` | Underlying spot price as reported inside the option-chain response | Dhan Option Chain API | INR |
| `expiry` | string | `"2026-09-25"` | Expiry this snapshot is for | Dhan | date |
| `expiry_list` | array[string] | | All expiries Dhan currently lists | Dhan, refreshed every 1800s | |
| `strikes[].strike` | float | `25000.0` | Strike price | Dhan | INR |
| `strikes[].ce`/`pe` | object or null | | Raw Dhan contract fields, passed through unrenamed: `security_id`, `last_price`, `oi`, `volume`, `top_bid_price`, `top_ask_price`, `top_bid_quantity`, `top_ask_quantity`, `implied_volatility`, `greeks` (delta/gamma/theta/vega), `previous_oi`, `previous_volume` — whatever Dhan returns, unmodified | Dhan | mixed |
| `refresh_seconds` | float | `3.2` | Actual poll interval (matches Dhan's 1-request/3s option-chain rate limit) | Static | seconds |
| `fetch_count` | int | | Number of successful chain refreshes since process start | Server | |

### `analytics` block (computed by Psygrid, not from Dhan)

| Field | Type | Meaning | Formula |
|---|---|---|---|
| `pcr_oi` | float or null | Put-call ratio by open interest | `total_put_oi / total_call_oi` |
| `pcr_volume` | float or null | Put-call ratio by volume | `total_put_volume / total_call_volume` |
| `atm_strike` | float or null | Strike nearest `underlying_ltp` | `min(strikes, key=abs(strike-ltp))` |
| `max_pain_strike` | float or null | Strike minimizing aggregate option-writer payout | Standard max-pain: sum of ITM payouts across all strikes at each candidate settlement price, minimized |
| `support_strikes`/`resistance_strikes` | array[float] | Top-3 strikes by put OI / call OI | Sort by OI descending |
| `avg_call_iv`/`avg_put_iv`/`iv_skew` | float or null | Mean IV per side; `iv_skew = avg_call_iv - avg_put_iv` | Mean of `implied_volatility` across CE/PE |
| `contracts[].moneyness` | string | `ITM`/`ATM`/`OTM` | Strike vs. underlying LTP |
| `contracts[].oi_change_classification` | string | `LONG_BUILDUP`/`SHORT_BUILDUP`/`SHORT_COVERING`/`LONG_UNWINDING`/`INSUFFICIENT_DATA` | Price-direction × OI-direction between this and the previous refresh (raw classification label describing OI/price co-movement — **not a trading signal**) |
| `data_quality.contracts_total` | int | Total CE+PE contracts seen this refresh | |
| `data_quality.contracts_missing_security_id` | int | Contracts Dhan returned without a `security_id` | Validation |
| `data_quality.duplicate_security_ids` | int | Same `security_id` appearing twice in one chain | Validation |
| `data_quality.crossed_markets_detected` | int | Contracts where `top_bid_price > top_ask_price` | Validation |

## 5. Market-depth endpoints (`/public/{nifty,banknifty,midcpnifty}-depth.json`)

| Field | Type | Meaning | Source |
|---|---|---|---|
| `depth_levels` | int | `20` — always 20, never reduced to top-of-book | Static |
| `contracts[].bid`/`ask` | array[20] of `{level, price, quantity, orders}` | Full 20-level order book per side | Dhan 20-level Depth WebSocket (binary packet) |
| `contracts[].crossed_book` | bool | `true` if top bid > top ask on this contract (validation flag, an abnormal/error state) | Computed by Psygrid |
| `contracts[].last_price`, `volume`, `oi`, `buy_quantity`, `sell_quantity`, `ohlc` | mixed | Quote fields refreshed alongside depth | Dhan Market Quote REST API, 1s |
| `connection_count`/`packet_count` | int | WebSocket lifecycle counters | Server |

## 6. Technical-indicator endpoints (equity `/public/indicators*.json`, underlyings `/public/{symbol}-indicators.json`)

37 indicator fields under `result.indicators`, computed by the same `PsygridMasterIndicatorEngine` for both equities and derivatives underlyings:

`sma_20, ema_9, ema_20, vwap, vwma_20, bb_middle_20, bb_upper_20, bb_lower_20, bb_width_20, bb_percent_b_20, true_range, atr_14, natr_14, rsi_14, macd_line, macd_signal, macd_histogram, stoch_raw_k, stoch_k, stoch_d, cci_20, roc_12, momentum_10, williams_r_14, adx_14, plus_di_14, minus_di_14, obv, cmf_20, mfi_14, rvol_20, donchian_upper_20, donchian_lower_20, donchian_middle_20, keltner_middle, keltner_upper, keltner_lower, supertrend, supertrend_direction`

| Field | Type | Meaning |
|---|---|---|
| `result.indicator_status.{name}.ready` | bool | Whether enough history exists for this indicator yet |
| `result.indicator_status.{name}.valid_observations`/`required_observations` | int | Bars available vs. bars needed |
| `result.freshness.status` | string | `FRESH`/`STALE`/`TIME_ERROR`/`UNKNOWN` | Age of the latest candle vs. `stale_after_seconds` (120s) |
| `result.bar_count` | int | Candles used in this computation | |

All 37 fields are raw indicator *values* — none of them are trading decisions. No field here is ever `BUY`/`SELL`/a confidence score.

## 7. Futures endpoints (`/public/{nifty,banknifty}-futures.json`)

| Field | Type | Example | Meaning | Source |
|---|---|---|---|---|
| `security_id` | string | `"49081"` | Dhan's ID for the resolved front-month contract | Dhan instrument master |
| `trading_symbol` | string | `"NIFTY-Oct2026-FUT"` | Exchange trading symbol | Dhan instrument master |
| `expiry` | string | `"2026-10-30"` | Contract expiry (nearest unexpired FUTIDX row) | Dhan instrument master |
| `lot_size`, `tick_size` | int, float | `75`, `0.05` | Contract specs | Dhan instrument master (`SEM_LOT_UNITS`, `SEM_TICK_SIZE`) |
| `last_price`, `ohlc`, `volume`, `oi` | mixed | Current quote | Dhan Market Quote API |
| `oi_change` | float or null | OI delta since the previous poll of *this* contract (null on the first observation or right after a contract roll) | Computed by Psygrid, RAM-only |
| `top_bid_price`/`top_ask_price` | float or null | Best bid/ask if Dhan's quote packet includes depth | Dhan Market Quote API |
| `raw_quote` | object | The full, unmodified Dhan quote response for this contract | Dhan — preserved per the "no information loss" principle |
| `refresh_seconds` | float | `2.0` | Quote poll interval; contract identity re-resolved every 1800s | Static |

## 8. Market breadth (`/public/market-breadth.json`)

| Field | Type | Meaning | Formula/Source |
|---|---|---|---|
| `advancing`/`declining`/`unchanged`/`unknown` | int | Counts across the live 990-equity universe | `change_pct` sign vs. previous close; `unknown` = no LTP or no previous close yet |
| `advance_decline_ratio` | float or null | `advancing / declining` | null if `declining == 0` |
| `new_session_highs`/`new_session_lows` | int | Constituents whose LTP is at/above (at/below) their **own intraday session** high/low so far | **Intraday only — not 52-week**, explicitly labeled as such in the payload |
| `coverage_count` | int | Constituents with usable data this refresh | |
| `constituents[]` | array | Per-stock: `symbol, security_id, sector, ltp, previous_close, today_open, day_high, day_low, change_pct, is_new_session_high, is_new_session_low` | Computed from Psygrid's own live RAM state |

No `BULLISH`/`BEARISH`/`STRONG`/`WEAK` label anywhere in this payload — verified by test.

## 9. Sector data (`/public/sectors.json`)

| Field | Type | Meaning |
|---|---|---|
| `sectors[].sector` | string | One of 18 broad NSE sector buckets, or `OTHER` |
| `sectors[].constituent_count`/`coverage_count` | int | Stocks mapped to this sector / with usable data |
| `sectors[].median_change_pct` | float or null | Median `change_pct` across sector constituents (raw statistic, not a "regime") |
| `sectors[].advancing`/`declining` | int | Counts within the sector |
| `sectors[].constituents[]` | array | `symbol, security_id, ltp, change_pct` per stock |

## 10. Health (`/public/health.json`)

| Field | Type | Meaning |
|---|---|---|
| `overall_status` | string | `HEALTHY`/`WARNING`/`DEGRADED`/`DOWN` — rolled up from every component |
| `components.{name}.status` | string | `FRESH`/`WARNING`/`STALE`/`ERROR` for that one feed |
| `components.{name}.age_seconds` | float or null | Time since that feed's own last update | 
| `components.{name}.expected_refresh_seconds` | float | What that feed's own refresh cadence should be (used to derive the status thresholds: `>2x` → WARNING, `>5x` → STALE) |
| `components.{name}.last_error` | string | The feed's own last recorded error, if any |
| `components.{name}.record_count`/`expected_record_count`/`record_count_match` | int/int/bool | Present only for feeds with a known expected size (currently `equity_990`, expected 990) |

`/public/health.json` never calls out to any upstream API itself — it only reads timestamps/status fields each manager already tracks, which is what keeps it lightweight.

## 11. Global context — Tier 2 (`/public/global-context.json`)

| Field | Type | Meaning | Source |
|---|---|---|---|
| `market_data_status` | string | **Always `"DELAYED"`** | Structural — FRED never publishes live ticks |
| `series.{name}.value` | float | Latest official value | FRED |
| `series.{name}.source_date` | string | The date FRED's own release covers (not "now") | FRED |
| `series.{name}.source` | string | `"FRED_FEDERAL_RESERVE_BANK_OF_ST_LOUIS"` | |
| `not_available` | array[string] | Explicit list of categories this endpoint deliberately does not cover | Static |

Series covered: `sp500` (S&P 500 close, `SP500`), `vix` (`VIXCLS`), `us_10y_yield` (`DGS10`), `wti_crude_oil` (`DCOILWTICO`), `usd_inr` (`DEXINUS`, official Fed H.10 rate).

## 12. RBI news — Tier 2 (`/public/rbi-news.json`)

| Field | Type | Meaning | Source |
|---|---|---|---|
| `market_data_status` | string | `"NEAR_LIVE"` — polled every 300s from RBI's own feed, not delayed by policy, just by poll interval | |
| `items[].id` | string | RSS `guid` (or link/title fallback) | RBI RSS |
| `items[].headline`, `summary`, `url` | string | Raw RSS `title`, `description`, `link` | RBI RSS |
| `items[].category` | string | `press_releases`/`notifications`/`speeches` — which RBI feed it came from | Static per feed |
| `items[].published_at` | string or null | Parsed from RSS `pubDate`; null if RBI's feed omits or malforms it (never guessed) | RBI RSS |
| `items[].country` | string | Always `"IN"` | Static |

No sentiment/classification field exists on any item.
