# PSYGRID Endpoint Map

Live market-data acquisition and distribution layer for the Indian market session. Every endpoint below is RAM-only, non-synthetic, and served fresh on every request from in-process state built by an independent background manager. This document lists what exists, where it comes from, and how often it changes. Field-level detail is in `DATA_DICTIONARY.md`.

Base URL (production): `http://140.245.226.102:10000`

## Service / meta

| Endpoint | Purpose | Refresh | Source | Dependencies |
|---|---|---|---|---|
| `/` | Service identity, full endpoint index | On request | — | none |
| `/health` | Liveness probe | On request | — | none |
| `/ready` | Readiness gate (full 989-equity universe live) | On request | Equity feed state | equity feed |
| `/public/health.json` | Aggregated freshness/status of every live feed | On request (reads cached state only) | All managers | all managers below |

## Equity (989 stocks) — sealed

| Endpoint | Purpose | Refresh | Data source | Expected records |
|---|---|---|---|---|
| `/public/live.json` | Full 989-stock 1m OHLCV | Tick-driven (WebSocket) | Dhan WebSocket Full feed | 989 |
| `/public/live-{a..v}.json` | 45-stock shards of the same universe | Tick-driven | same | 22 shards: 45 each, last shard 44 |
| `/public/stock/{symbol}.json` | Single stock | Tick-driven | same | 1 |
| `/public/indicators.json`, `/public/indicators/{symbol}.json`, `/public/indicators-{shard}.json` | Technical-indicator suite (37 fields) per stock | 1s | Derived from `/public/live.json` | 989 |

## 16-index layer — sealed

| Endpoint | Purpose | Refresh | Data source |
|---|---|---|---|
| `/public/{nifty,banknifty,sensex,nifty500,niftymidcap100,niftysmallcap100,finnifty,indiavix,niftyit,niftyauto,niftypharma,niftymetal,niftyfmcg,niftyrealty,niftyenergy,niftyinfra}.json` | 1m/5m/15m/1h OHLCV per index | Tick-driven (1m), on-demand historical (5m/15m/1h) | Dhan WebSocket Full feed + Dhan historical API |

## NIFTY / BANKNIFTY / MIDCPNIFTY / SENSEX derivatives

| Endpoint | Purpose | Refresh | Data source |
|---|---|---|---|
| `/public/{nifty,banknifty,midcpnifty,sensex}-options.json` | Option chain + chain analytics | 3.2s | Dhan Option Chain REST API |
| `/public/{nifty,banknifty,midcpnifty,sensex}-depth.json` | 20-level market depth, nearest 25 strikes × CE/PE | WebSocket push (depth), 1s (quotes) | Dhan 20-level Depth WebSocket + Market Quote REST |
| `/public/{nifty,banknifty,midcpnifty,sensex}-indicators.json` | Technical-indicator suite on the underlying's own 1m candles | 5s | NIFTY/BANKNIFTY/SENSEX: sealed index layer's candles. MIDCPNIFTY: Dhan historical intraday API (new poller, no WS feed exists for this symbol) |
| `/public/{nifty,banknifty,sensex}-futures.json` | Front-month index-futures quote | 2s quote / 30min contract resolution | Dhan instrument master (contract identity) + Dhan Market Quote API |

**SENSEX trades on BSE, not NSE** — its option-chain underlying identity (`security_id=51`, `IDX_I`) matches the sealed index layer, but its actual option/futures contracts trade on Dhan's `BSE_FNO` segment (vs. `NSE_FNO` for NIFTY/BANKNIFTY). `futures_layer.py`/`derivatives_instruments.py` take an `exchange` parameter for this; MIDCPNIFTY has no futures endpoint (no listed MIDCPNIFTY futures contract exists).

## NIFTY 50 stock options

| Endpoint | Purpose | Refresh | Data source |
|---|---|---|---|
| `/public/stock-options.json` | Listing of all 50 NIFTY 50 constituents' option status/rotation metadata | On request (reads cached state) | — |
| `/public/stock-options/{SYMBOL}.json` | One stock's option chain + chain analytics | Round-robin, ~3.2s per symbol | Dhan Option Chain REST API |

Covers the 50 NIFTY 50 index constituents (a fixed, periodically-reconstituted list — see `stock_options.py`'s `NIFTY50_SYMBOLS`, last updated 2026-09-28). Each symbol's Dhan security ID is resolved **independently**, directly against Dhan's live NSE equity instrument master — deliberately not reused from the unrelated 989-equity universe, since two current NIFTY 50 constituents (SBILIFE, SHRIRAMFIN) aren't part of that list. `StockOptionsManager` is a single round-robin poller sharing the **same** rate-limited Dhan option-chain REST queue as the NIFTY/BANKNIFTY/MIDCPNIFTY/SENSEX option-chain pollers above, one symbol at a time — a full rotation across all resolved symbols takes roughly `resolved_count × 3.2s` (~2.7 minutes for all 50), which is by design, not a bug: polling all NSE F&O stocks (~180-220) at this same rate would take closer to 10 minutes per cycle, which is why this is scoped to NIFTY 50 rather than the full F&O universe. If a symbol never resolves against Dhan's instrument master, its own state reports `"UNRESOLVED"` — never fabricated.

## NIFTY 50 stock option depth

| Endpoint | Purpose | Refresh | Data source |
|---|---|---|---|
| `/public/stock-depth.json` | Listing of all 50 stocks' depth rotation status | On request | — |
| `/public/stock-depth/{SYMBOL}.json` | One stock's 20-level depth, nearest 5 strikes × CE/PE | WebSocket push (depth), 1s (quotes), rotated | Dhan 20-level Depth WebSocket + Market Quote REST |

Dhan's 20-level depth WebSocket allows **at most 50 subscribed instruments per connection** — the same cap `index_depth.py` uses for the NIFTY/BANKNIFTY/MIDCPNIFTY/SENSEX depth feeds. Real-time depth for all 50 NIFTY 50 stocks at 5 strikes × CE/PE each (10 contracts/stock) would need 10 separate connections — instead `StockDepthManager` shares **one** connection and rotates its 50-instrument subscription through the stock universe in batches of 5 stocks every 30s, the same spirit as the option-chain poller's own rotation, rather than opening 10 more WebSocket connections on top of the 6 already running (equity feed, the shared 16-index feed, and the 4 existing per-underlying depth feeds). Each stock's `rotation_status` is `"ACTIVE"` only while its batch is the WebSocket's current subscription; `"IDLE"` the rest of the time, showing its last known depth from its previous turn rather than fabricating anything for the gap. A full rotation across all resolved stocks takes roughly `ceil(resolved_count / 5) × 30s` (~5 minutes for all 50).

## Market-wide aggregates (Tier 1, new)

| Endpoint | Purpose | Refresh | Data source |
|---|---|---|---|
| `/public/market-breadth.json` | Raw advance/decline/new-high/new-low counts + full constituent list | On request (computed from live RAM state) | Psygrid's own 989-equity RAM state — no new source |
| `/public/sectors.json` | Raw per-sector median return/breadth, constituent list | On request | same |

## Tier 2 — delayed/context data (new, clearly marked)

| Endpoint | Purpose | Refresh | Data source | Status label |
|---|---|---|---|---|
| `/public/global-context.json` | S&P 500, VIX, US 10Y yield, WTI crude, USD/INR — official reference values | 3600s (FRED updates at most daily) | FRED (Federal Reserve Bank of St. Louis) | `market_data_status: "DELAYED"` always |
| `/public/rbi-news.json` | RBI's own press releases, notifications, speeches | 300s | RBI official RSS feeds (`rbi.org.in`) | `market_data_status: "NEAR_LIVE"` |

**Requires `FRED_API_KEY` environment variable** (free, instant, at fredaccount.stlouisfed.org) — without it, `/public/global-context.json` reports a clear error rather than fabricating values.

## Explicitly NOT_AVAILABLE (by design, not oversight)

GIFT NIFTY, NASDAQ, Dow Jones, Nikkei, Hang Seng, Shanghai, KOSPI, DXY, gold, general global/financial market news, a full economic calendar with forecast/actual/importance fields. No free, ToS-clean, reliable official source exists for these — see the final report for the full source evaluation. Never approximated or substituted.

## Payload size / weight notes

- `/public/live.json` and `/public/market-breadth.json`/`/public/sectors.json` (full constituent detail) are the largest payloads (~989 rows); all are served behind GZip.
- `/public/{symbol}-depth.json` carries up to 50 contracts × 20 levels × 2 sides — moderate size, refreshed by push not poll.
- `/public/health.json` is intentionally the lightest endpoint: it reads cached status fields off every manager, never recomputes or refetches anything.
