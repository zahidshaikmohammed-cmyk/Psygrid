# Psygrid

Live, machine-readable Dhan market-data service.

## Locked architecture

- DhanHQ v2 historical APIs + DhanHQ v2 Quote WebSocket.
- Free Render web service.
- GitHub repository: Psygrid.
- RAM only. No database, Redis, Postgres, files, or persistent market-data storage.
- NSE session: 09:15 to 15:15 Asia/Kolkata.
- Live acquisition starts only during the market session.
- Session state is wiped after 15:15.
- **Synthetic candles are forbidden.** No interpolation, reconstruction, aggregation, gap-filling, or fabricated OHLCV candles.
- Live 1-minute candles are formed only from genuine Dhan Quote packets.
- Historical 5m, 15m and 1h candles come directly from Dhan's native intraday historical API.
- Historical daily candles come directly from Dhan's native daily historical API.
- Weekly candles are intentionally not generated because Dhan's documented equity historical endpoints do not expose a native weekly candle. Psygrid therefore returns an explicit unavailable status instead of synthesizing a weekly candle.

## Output

`/public/live.txt` — latest live 1m state for all configured stocks.

`/public/stock/SYMBOL.txt` — complete machine-readable package for one stock.

`/public/stock/SYMBOL/1m.txt`
`/public/stock/SYMBOL/5m.txt`
`/public/stock/SYMBOL/15m.txt`
`/public/stock/SYMBOL/1h.txt`
`/public/stock/SYMBOL/1d.txt`
`/public/stock/SYMBOL/1w.txt`

## Live fields

OHLCV, Dhan day VWAP/average-price field, MA9, EMA20 and RSI14.

## Environment

Required:

- `DHAN_CLIENT_ID`
- `DHAN_ACCESS_TOKEN` or another environment variable named by `DHAN_TOKEN_VAR`
- `PSYGRID_INSTRUMENTS` — JSON array of instruments

Example:

```json
[
  {"symbol":"TCS","security_id":"11536","exchange_segment":"NSE_EQ","instrument":"EQUITY"}
]
```

Recommended defaults:

- `TIMEZONE=Asia/Kolkata`
- `MARKET_START=09:15`
- `MARKET_END=15:15`
- `INTRADAY_HISTORY_DAYS=7`
- `DAILY_LOOKBACK=7`
- `WEEKLY_LOOKBACK=7`
- `MA_PERIOD=9`
- `EMA_PERIOD=20`
- `RSI_PERIOD=14`
- `MAX_INSTRUMENTS=500`

The Dhan access token must never be committed to GitHub.
