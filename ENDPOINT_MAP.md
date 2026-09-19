# PSYGRID Endpoint Map

Live market-data acquisition and distribution layer for the Indian market session. Every endpoint below is RAM-only, non-synthetic, and served fresh on every request from in-process state built by an independent background manager. This document lists what exists, where it comes from, and how often it changes. Field-level detail is in `DATA_DICTIONARY.md`.

Base URL (production): `http://140.245.226.102:10000`

## Service / meta

| Endpoint | Purpose | Refresh | Source | Dependencies |
|---|---|---|---|---|
| `/` | Service identity, full endpoint index | On request | — | none |
| `/health` | Liveness probe | On request | — | none |
| `/ready` | Readiness gate (990-equity fully live) | On request | Equity feed state | equity feed |
| `/public/health.json` | Aggregated freshness/status of every live feed | On request (reads cached state only) | All managers | all managers below |

## Equity (990 stocks) — sealed

| Endpoint | Purpose | Refresh | Data source | Expected records |
|---|---|---|---|---|
| `/public/live.json` | Full 990-stock 1m OHLCV | Tick-driven (WebSocket) | Dhan WebSocket Full feed | 990 |
| `/public/live-{a..v}.json` | 45-stock shards of the same universe | Tick-driven | same | 45 each, 22 shards |
| `/public/stock/{symbol}.json` | Single stock | Tick-driven | same | 1 |
| `/public/indicators.json`, `/public/indicators/{symbol}.json`, `/public/indicators-{shard}.json` | Technical-indicator suite (37 fields) per stock | 1s | Derived from `/public/live.json` | 990 |

## 16-index layer — sealed

| Endpoint | Purpose | Refresh | Data source |
|---|---|---|---|
| `/public/{nifty,banknifty,sensex,nifty500,niftymidcap100,niftysmallcap100,finnifty,indiavix,niftyit,niftyauto,niftypharma,niftymetal,niftyfmcg,niftyrealty,niftyenergy,niftyinfra}.json` | 1m/5m/15m/1h OHLCV per index | Tick-driven (1m), on-demand historical (5m/15m/1h) | Dhan WebSocket Full feed + Dhan historical API |

## NIFTY / BANKNIFTY / MIDCPNIFTY derivatives

| Endpoint | Purpose | Refresh | Data source |
|---|---|---|---|
| `/public/{nifty,banknifty,midcpnifty}-options.json` | Option chain + chain analytics | 3.2s | Dhan Option Chain REST API |
| `/public/{nifty,banknifty,midcpnifty}-depth.json` | 20-level market depth, nearest 25 strikes × CE/PE | WebSocket push (depth), 1s (quotes) | Dhan 20-level Depth WebSocket + Market Quote REST |
| `/public/{nifty,banknifty,midcpnifty}-indicators.json` | Technical-indicator suite on the underlying's own 1m candles | 5s | NIFTY/BANKNIFTY: sealed index layer's candles. MIDCPNIFTY: Dhan historical intraday API (new poller, no WS feed exists for this symbol) |
| `/public/{nifty,banknifty}-futures.json` | Front-month index-futures quote | 2s quote / 30min contract resolution | Dhan instrument master (contract identity) + Dhan Market Quote API |

## Market-wide aggregates (Tier 1, new)

| Endpoint | Purpose | Refresh | Data source |
|---|---|---|---|
| `/public/market-breadth.json` | Raw advance/decline/new-high/new-low counts + full constituent list | On request (computed from live RAM state) | Psygrid's own 990-equity RAM state — no new source |
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

- `/public/live.json` and `/public/market-breadth.json`/`/public/sectors.json` (full constituent detail) are the largest payloads (~990 rows); all are served behind GZip.
- `/public/{symbol}-depth.json` carries up to 50 contracts × 20 levels × 2 sides — moderate size, refreshed by push not poll.
- `/public/health.json` is intentionally the lightest endpoint: it reads cached status fields off every manager, never recomputes or refetches anything.
