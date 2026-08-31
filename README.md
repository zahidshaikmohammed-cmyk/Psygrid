# Psygrid

Live, machine-readable Dhan Data API service for 90 NSE equities.

## Locked architecture

- DhanHQ v2 Data APIs + DhanHQ v2 Quote WebSocket.
- Free Render web service.
- GitHub repository: Psygrid.
- RAM only. No database, Redis, Postgres, SQLite, files, or persistent market-data storage.
- NSE session: **09:15 to 15:15 Asia/Kolkata**.
- Live acquisition starts at 09:15 and stops at 15:15 exactly.
- After 15:15, all session candles, indicators and instrument state are wiped from RAM.
- Before 09:15 there is no live market acquisition.
- **Synthetic candles are forbidden.** Psygrid never interpolates, fabricates, gap-fills, reconstructs or invents missing OHLCV candles.
- Live 1-minute candles are formed only from genuine Dhan Quote WebSocket events and cumulative-volume deltas.
- Historical 5m, 15m and 1h candles come directly from Dhan's native intraday historical endpoint.
- Historical daily candles come directly from Dhan's native daily historical endpoint. Psygrid loads indicator warmup history internally but returns exactly the previous 7 completed daily candles.
- Dhan's documented v2 equity historical endpoints expose daily and minute intervals (1, 5, 15, 25, 60), not a native weekly equity candle. Therefore Psygrid **does not synthesize weekly candles**; the 1w field explicitly reports native-weekly unavailability.
- Dhan's User Profile endpoint is checked at session start so the service can verify that the paid Data API plan is active.

## 90-stock universe

`stocks.json` contains the 90-symbol test universe. Psygrid resolves the current Dhan security IDs at runtime from Dhan's official instrument master; IDs are never hard-coded or persisted.

## JSON endpoints

### All 90 stocks

`/public/live.json`

This is the primary machine/AI endpoint. During the live session it exposes all 90 configured stocks, the live 1m candle history from 09:15 onward, and native historical 5m, 15m, 1h and previous-7-day daily context. Each stock also has a `current` object with the latest LTP/candle state.

### One stock

`/public/stock/RELIANCE.json`

### One timeframe

`/public/stock/RELIANCE/1m.json`
`/public/stock/RELIANCE/5m.json`
`/public/stock/RELIANCE/15m.json`
`/public/stock/RELIANCE/1h.json`
`/public/stock/RELIANCE/1d.json`
`/public/stock/RELIANCE/1w.json`

All public JSON responses use `Cache-Control: no-store` so a machine does not receive stale cached market data.

## Candle and indicator fields

Every available candle contains:

- timestamp
- open
- high
- low
- close
- volume
- VWAP
- MA9
- EMA20
- RSI14
- complete
- source

The current 1m candle has `complete=false`; completed candles have `complete=true`.

VWAP is candle-typical-price weighted with a daily/session reset. Historical VWAP is calculated from the genuine Dhan OHLCV candles; no OHLCV values are altered.

## Environment

Required:

- `DHAN_CLIENT_ID`
- `DHAN_ACCESS_TOKEN`, or the actual daily-token variable name through `DHAN_TOKEN_VAR`

The 90-stock universe is read from `stocks.json`, not from a giant Render environment variable.

Recommended:

- `TIMEZONE=Asia/Kolkata`
- `MARKET_START=09:15`
- `MARKET_END=15:15`
- `INTRADAY_HISTORY_DAYS=5`
- `DAILY_LOOKBACK=7`
- `DAILY_INDICATOR_WARMUP=30`
- `WEEKLY_LOOKBACK=7`
- `MA_PERIOD=9`
- `EMA_PERIOD=20`
- `RSI_PERIOD=14`
- `MAX_INSTRUMENTS=500`

The Dhan access token must never be committed to GitHub.

## Render

Build command:

`pip install -r requirements.txt`

Start command:

`python app.py`

Render environment variables supply the Dhan client ID and the daily-rotated access token. No market-data persistence service is required.
