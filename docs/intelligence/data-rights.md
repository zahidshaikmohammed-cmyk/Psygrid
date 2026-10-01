# Data-rights register

Every dataset PSYGRID ingests, stores or derives, with what is known about the
right to use it. **No row has a verified right to redistribute.** Until a row's
status says otherwise, its data is for the account holder's own use only, and no
PSYGRID output (raw or derived) may be served to a third party.

Status values:

- `UNVERIFIED`: terms not yet read or not yet confirmed in writing.
- `OWN USE`: confirmed usable for the subscriber's own analysis and trading.
- `REDISTRIBUTABLE`: confirmed in writing that it may be served to others, with the conditions recorded.

## Datasets

| Dataset | Source | How PSYGRID gets it | Stored | Served at | Status |
| --- | --- | --- | --- | --- | --- |
| Equity ticks → 1m candles (989 stocks) | NSE via Dhan | Dhan Full-mode market-feed WebSocket | RAM; daily archive on disk | `/public/live*.json`, `/public/stock/*` | UNVERIFIED |
| Previous close, today's open | NSE via Dhan | Dhan market-quote REST | RAM; daily archive | same | UNVERIFIED |
| Equity 1m history (bootstrap, gap backfill) | NSE via Dhan | Dhan intraday historical REST | RAM; daily archive | same | UNVERIFIED |
| Archive history bootstrap (equity and index 1m, daily open/close) | NSE, BSE via Dhan | Dhan intraday and daily historical REST, run on demand by `bootstrap-history` | Daily archive on disk (`source: DHAN_HISTORICAL_API`) | Only through Intelligence outputs | UNVERIFIED (own use) |
| Index ticks and candles (16 indices) | NSE, BSE via Dhan | Dhan WebSocket + historical REST | RAM; daily archive | `/public/{index}.json` | UNVERIFIED |
| Index and stock option chains | NSE, BSE via Dhan | Dhan option-chain REST | RAM | `*-options.json`, `/public/stock-options/*` | UNVERIFIED |
| 20-level market depth | NSE, BSE via Dhan | Dhan 20-depth WebSocket | RAM | `*-depth.json`, `/public/stock-depth/*` | UNVERIFIED |
| Index futures quotes | NSE, BSE via Dhan | Dhan market-quote REST | RAM | `*-futures.json` | UNVERIFIED |
| Instrument master (security ids, lot sizes) | Dhan | `images.dhan.co/api-data/api-scrip-master.csv` | Not stored | Not served directly | UNVERIFIED |
| Indicators, option analytics, breadth, sector aggregates | Derived from the rows above | Computed by PSYGRID | RAM | `/public/indicators*`, `*-indicators.json`, `market-breadth`, `sectors` | UNVERIFIED (derived data; see question 3) |
| Global context: SP500, VIXCLS, DGS10, DCOILWTICO, DEXINUS | FRED (St. Louis Fed) | FRED API with `FRED_API_KEY` | RAM | `/public/global-context.json` | UNVERIFIED |
| RBI press releases, notifications, speeches | Reserve Bank of India | RBI RSS feeds on `www.rbi.org.in` | RAM | `/public/rbi-news.json` | UNVERIFIED |
| Sector taxonomy, NIFTY 50 constituent list | Maintained by hand in `sector_taxonomy.py`, `stock_options.py` | Repository | Repository | Inside breadth and stock-option payloads | OWN USE (own work; constituent membership is public information) |
| Intelligence outputs (features, anomalies, relationships, events, similarity) | Derived from the archived Dhan data above | Computed by the separate `psygrid-intelligence` service | Event store, baselines and similarity caches under `~/psygrid-intelligence` | `/v2` on port 18101, localhost by default, API key required | UNVERIFIED (derived data; see question 3). Keys are for the account holder's own engines only until question 3 is answered |

## Open questions

Each must be answered in writing, with the answer and its source added to this
file, before the rows it affects are served to anyone else.

1. Do Dhan's API terms allow commercial use of Data API output, or only use for the subscriber's own trading? Ask: Dhan (help@dhan.co) and the DhanHQ API terms.
2. Do Dhan's terms allow showing raw prices, candles or depth to anyone other than the subscriber?
3. Is derived data (indicators, anomaly scores, events, similarity results) treated as redistribution under NSE and BSE market-data policy, and at what delay or level of aggregation does it stop being so? Ask: NSE Data & Analytics, BSE.
4. Which NSE licence fits a product like this (real-time, delayed, end-of-day, snapshot, non-display), and what does it cost?
5. May history fetched through Dhan be stored long term and used in a commercial product?
6. What do the FRED and RBI terms require for the context endpoints (attribution, commercial use)?

## Pages to read

These could not be opened from the environment where this file was written;
they are where the answers start.

- Dhan Data API subscription: https://dhan.co/support/platforms/dhanhq-api/how-does-the-dhanhq-data-api-subscription-work/
- DhanHQ v2 documentation: https://dhanhq.co/docs/v2/
- NSE paid real-time data: https://www.nseindia.com/static/market-data/real-time-data-subscription
- FRED API terms of use: https://fred.stlouisfed.org/docs/api/terms_of_use.html
