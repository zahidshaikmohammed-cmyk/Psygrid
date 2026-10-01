# Psygrid

Live, machine-readable Indian market data built on the DhanHQ v2 Data APIs.

Psygrid streams a 989-stock NSE equity universe, 16 NSE/BSE indices, index and
NIFTY 50 stock option chains with 20-level depth, and index futures. It serves
everything as JSON over HTTP from process memory, with no database and no
fabricated data. Each day's completed 1-minute candles are also archived to
disk, building a history for backtesting.

## Design principles

- **Genuine data only.** Psygrid never interpolates, gap-fills or invents candles, quotes or option rows. A missing value is reported as missing.
- **RAM-only serving.** Endpoints are served from memory, with no database. Session state is wiped after the 15:15 IST close, once the day has been archived (see [Daily archive](#daily-archive)).
- **Native candles.** Live 1-minute candles are built from Dhan WebSocket ticks and cumulative-volume deltas. Higher timeframes come from Dhan's own historical endpoints.
- **Isolated domains.** The equity feed, the index layer and each derivatives feed run in independent managers. A failure in one never takes down another.
- **Fixed configuration.** Session times, universe size and indicator periods are constants in `config.py`. Environment variables cannot change market-data behaviour.

## Data coverage

| Domain | Endpoints | Source |
|---|---|---|
| Equities (989 stocks) | `/public/live.json`, shards `/public/live-{a..v}.json`, `/public/stock/{SYMBOL}.json` | Dhan Full WebSocket |
| Equity indicators | `/public/indicators.json`, `/public/indicators-{a..v}.json`, `/public/indicators/{SYMBOL}.json` | Derived from Psygrid's own 1m candles |
| 16 indices | `/public/{nifty,banknifty,sensex,finnifty,indiavix,…}.json` | Dhan WebSocket + historical API |
| Index options | `/public/{nifty,banknifty,midcpnifty,sensex}-options.json` | Dhan option-chain API |
| Index option depth | `/public/{nifty,banknifty,midcpnifty,sensex}-depth.json` | Dhan 20-level depth WebSocket |
| Underlying indicators | `/public/{nifty,banknifty,midcpnifty,sensex}-indicators.json` | Index 1m candles |
| Index futures | `/public/{nifty,banknifty,sensex}-futures.json` | Dhan market-quote API |
| NIFTY 50 stock options | `/public/stock-options.json`, `/public/stock-options/{SYMBOL}.json` | Dhan option-chain API |
| NIFTY 50 stock depth | `/public/stock-depth.json`, `/public/stock-depth/{SYMBOL}.json` | Dhan 20-level depth WebSocket |
| Breadth & sectors | `/public/market-breadth.json`, `/public/sectors.json` | Computed from the equity feed |
| Context (delayed) | `/public/global-context.json`, `/public/rbi-news.json` | FRED, RBI RSS |
| Service | `/`, `/health`, `/ready`, `/public/health.json` | — |

See [`ENDPOINT_MAP.md`](ENDPOINT_MAP.md) for refresh rates and dependencies, and
[`DATA_DICTIONARY.md`](DATA_DICTIONARY.md) for every field. All JSON responses
are sent with `Cache-Control: no-store`.

## Architecture

```
app.py                      FastAPI service; starts every manager, serves the endpoints
├── session.py              equity session lifecycle (09:15 start, 15:15 wipe)
│   ├── feed_runtime.py     equity WebSocket feed with stale-instrument recovery (feed.py)
│   ├── state_runtime.py    RAM state + freshness tracking (state.py)
│   └── backfill.py         rate-limited historical gap backfill
├── indicator_runtime.py    equity indicator suite (psygrid_master_indicator.py)
├── index_layer.py          16 indices on one shared WebSocket
├── index_options.py        NIFTY/BANKNIFTY/MIDCPNIFTY/SENSEX option chains
├── index_depth.py          20-level depth for those chains
├── daily_archive.py        on-disk copy of each day's 1m candles
├── futures_layer.py        front-month index futures (derivatives_instruments.py)
├── stock_options.py        NIFTY 50 stock option chains, round-robin
├── stock_depth.py          NIFTY 50 stock depth, one rotating WebSocket
├── underlying_indicators.py / midcpnifty_underlying.py
├── market_breadth.py       breadth and sector aggregates (sector_taxonomy.py)
├── global_context.py / rbi_news.py
└── health_monitor.py       aggregated freshness across all feeds

dhan_api.py                 rate-limited Dhan REST client
dhan_auth.py / auth_retry.py  token handling and auth-failure retry
config.py                   fixed configuration and the stocks.json universe
```

## Configuration

| Variable | Required | Purpose |
|---|---|---|
| `DHAN_CLIENT_ID` | yes | Dhan client id |
| `DHAN_ACCESS_TOKEN` | one of the two auth methods | Explicit access token. Use `DHAN_TOKEN_VAR` to read it from a differently named variable. |
| `DHAN_PIN`, `DHAN_TOTP_SECRET` | one of the two auth methods | Automatic daily token generation via PIN + TOTP |
| `FRED_API_KEY` | no | Enables `/public/global-context.json`. Without it, the endpoint reports an error. |
| `PORT` | no | HTTP port (default `10000`) |
| `PSYGRID_STOCKS_FILE` | no | Path to the universe file (default `stocks.json`) |
| `PSYGRID_ARCHIVE_DIR` | no | Where daily archives are written (default `~/psygrid-data`) |

Never commit credentials. The equity universe lives in `stocks.json`. Each
symbol's Dhan security id is resolved at startup from Dhan's instrument
master, never hard-coded.

## Daily archive

Every trading day, Psygrid saves its completed 1-minute candles to
`$PSYGRID_ARCHIVE_DIR/YYYY-MM-DD/` (default `~/psygrid-data`):

| File | Columns |
|---|---|
| `equity_1m.csv.gz` | `symbol, security_id, timestamp, open, high, low, close, volume` |
| `equity_reference.csv.gz` | `symbol, security_id, previous_close, today_open` |
| `index_1m.csv.gz` | `index, symbol, timestamp, open, high, low, close, volume` |
| `manifest.json` | row count and write time per file |

Values are exactly what `/public/live.json` and the index endpoints serve: IST
timestamps and completed candles only, never synthetic. The archive is saved
every 5 minutes during the session, at the 15:15 close (after the final candle
closes and before memory is wiped), and when the service stops. A full day is
about 6 MB.

Archiving is best-effort and isolated: a failure is reported under `archive`
in `/public/health.json` and never affects the live feeds. A save never
replaces an existing file that holds more rows, so a restart part-way through
the day cannot overwrite a fuller earlier save.

```python
import pandas as pd
day = pd.read_csv("~/psygrid-data/2026-10-01/equity_1m.csv.gz")
```

## Development

Requires Python 3.12.

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

ruff check .            # lint
ruff format .           # format
python -m pytest -q     # full test suite
python app.py           # run locally (needs Dhan credentials)
```

`tests/test_dhan_990_resolution.py` downloads Dhan's live instrument master and
needs network access to `images.dhan.co`.

## Deployment

Production runs on an Oracle Cloud VM as the `psygrid` systemd service from
`/home/ubuntu/Psygrid`, using the `.venv` virtualenv and listening on port 10000.
Daily archives live outside the repository (`~/psygrid-data` by default), so
deploys never touch them.

Every push to `main` triggers `.github/workflows/deploy-oracle.yml`:

1. Lint and run the full test suite.
2. Pull `main` on the VM, install requirements and restart the service.
3. Wait for `/health`, then check all 16 index endpoints and the full equity
   universe (`tools/check_live_universe.py`).

`.github/workflows/ci.yml` runs the same lint and test checks on every pull
request.
