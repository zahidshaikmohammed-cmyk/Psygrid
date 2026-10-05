# PSYGRID Live Core

A separate, minimal runtime for the 989-stock equity live feed, sized for two Oracle Always Free
**E2.1.Micro** VMs (1/8 OCPU, 1 GB RAM each). It is **not** a replacement for the full PSYGRID:
`python app.py` is still the full application with its index, options, futures, depth,
indicator, archive and intelligence layers. The Live Core starts with `python -m live_core` and
runs only

```
Dhan WebSocket -> live 1-minute equity OHLCV -> RAM -> HTTP JSON
```

for one deterministic half of the canonical universe in `stocks.json`.

```
Psygrid repository
|-- full PSYGRID (unchanged)        python app.py           deploy-oracle.yml   psygrid.service
|-- Live Core                        python -m live_core     deploy-live-core.yml
      |-- node 0  (129.225.112.47)   stocks [0, 495)   shards live-a .. live-k   psygrid-live-core-node0.service
      |-- node 1  (second E2 Micro)  stocks [495, 989) shards live-l .. live-v   psygrid-live-core-node1.service
```

## What is reused, what is untouched, what is new

**Reused unchanged** (imported, not copied):

| Module | Used for |
| --- | --- |
| `feed.LiveFeed` | The whole Dhan WebSocket lifecycle: Full-mode subscription, packet parsing, LTT normalisation, reconnect back-off, 300 s cool-down on Dhan connection limits, no-quote watchdog, and `close_market_feed` on every connection cycle |
| `runtime_guard` | `close_market_feed` (closes dhanhq's private asyncio loop), `is_trading_day`, fd/RSS stats, `raise_nofile_limit`, `ServiceWatchdog` + `sd_notify` |
| `config` | `stocks.json` validation (`_load_symbol_universe`), `load_settings`, `load_instruments` (Dhan instrument master, 989/989 resolution), `refresh_access_token`, market hours, `MAX_LIVE_AGE_SECONDS` |
| `dhan_auth`, `auth_retry` | Explicit token or PIN + TOTP generation, rate-limit handling, one forced refresh on auth failure |
| `dhan_api.DhanAPI` | Profile/data-plan check, one REST quote snapshot, today's 1m bars (`load_today_completed_intraday`) |
| `output` | `_ist_timestamp`, `_price`: identical timestamp and rounding rules |

**Untouched:** every existing file. No existing module, test, workflow, systemd file, `stocks.json`
or `requirements.txt` was edited. `deploy-oracle.yml` does not know the Live Core exists.

**Not reused, deliberately:**

* `state.PsygridState`: one Python dict per candle (~1 KB). The Live Core keeps the same candle
  rules but stores candles column-wise in `array` buffers (48 bytes per candle).
* `session.SessionManager` / `backfill.HistoricalBackfill`: an 8-thread bootstrap pool and a 4-thread
  backfill pool coupled to `PsygridState`. The Live Core uses one rate-limited history thread.
* `app.py`: it starts every manager. Nothing in `live_core` imports it, pandas, numpy, the archive or
  any derivatives/indicator/intelligence module (enforced by a test).

**New files:**

```
live_core/            __init__ __main__ config partition state feed history render gzipjoin redact aggregate api runtime
deploy/live-core/     MANIFEST requirements.txt psygrid-live-core.service.in install.sh check_health.py
.github/workflows/deploy-live-core.yml
tests/live_core/      170 tests
docs/live-core/README.md
```

## Partition

Node `i` of `K` owns the contiguous canonical-order block `[ceil(i*N/K), ceil((i+1)*N/K))` of
`stocks.json`, computed at startup from `LIVE_CORE_NODE_ID` / `LIVE_CORE_NODE_COUNT`. There is no
second stock list. For N = 989, K = 2:

| node | indices | stocks | first | last | shards |
| --- | --- | --- | --- | --- | --- |
| 0 | 0 - 494 | 495 | RELIANCE | JAINREC | live-a .. live-k |
| 1 | 495 - 988 | 494 | RELAXO | MEESHO | live-l .. live-v |

Because 495 = 11 x 45, every existing 45-stock shard lies on exactly one node. Before starting, each
node validates `stocks.json` (989 unique NSE equities, the full app's rule), proves the partitions
are disjoint, complete and in canonical order, and at 09:15 resolves **all 989** symbols against
Dhan's instrument master. It refuses to start if the master does not resolve the whole universe in
the same order. Each node publishes a universe fingerprint and a partition fingerprint. A node refuses
to merge a peer whose fingerprints differ, for example two VMs deployed from different commits.

## Endpoints and the aggregation choice

Both nodes serve the same routes:

| Route | Served from |
| --- | --- |
| `/public/live.json` | local partition + peer partition, sorted by symbol (as the full app) |
| `/public/live-a.json` .. `live-v.json` | the owning node; canonical order (as the full app) |
| `/public/stock/{SYMBOL}.json` | the owning node |
| `/health` | this node + every peer's `/health/node` (cluster view, with `dimensions`) |
| `/public/health.json` | the full PSYGRID's health schema (`equity_990` component etc.); the cluster view rides along under `live_core` |
| `/health/node` | this node only (used by the systemd watchdog and by peers) |
| `/ready` | the full PSYGRID's `/ready` keys; 200 only when this node is LIVE, CONNECTED, fully subscribed and every stock fresh |
| `/internal/live-core/fragments` | this node's current RAM state, for peers |

The payload contract is the full PSYGRID's schema 4.0, including key order. A test feeds identical
ticks to `PsygridState` + `output.market_live_json` and to the Live Core and requires equal stock
objects. The repo's own `tools/check_live_universe.py` passes against the two-node cluster, live and
off-market. The only addition is a top-level `coverage` object.

Architectures considered:

1. **Node 0 aggregates over HTTP (chosen, made symmetric).** Consumers keep a single base URL. The
   cost is kept small by:
   * **No parsing.** Each stock's JSON object is cached once per stock and extended in place as
     candles complete. A peer sends `<index>\t<SYMBOL>\t<json>` lines and node 0 splices those
     bytes into its response.
   * **Conditional polling.** Node 0 polls with `if_version`. An unchanged peer answers with a
     header-only `not_modified` (a few hundred bytes). Content changes only when minutes close, so
     the full transfer happens about once a minute, over the private VCN, where traffic is free.
   * **Render caching.** A whole body is reused for `LIVE_CORE_RENDER_CACHE_SECONDS` (1 s). After
     that only the small head (status, session, `current_time_ist`, coverage) is rebuilt; the
     stocks part and its raw-deflate stream are reused while the content versions are unchanged,
     and the gzip response is the new head's deflate joined to the cached tail (CRC combined with
     `crc32_combine`). A response is therefore never older than 1 s, yet steady-state serving does
     not re-concatenate or re-compress 40 MB. Cached tails idle for 120 s are dropped.
   * **Coherent snapshots.** Each stock's encoding is refreshed first (lock per stock), then the
     version, session state and every fragment are captured under one lock, so a response never
     mixes stocks from different moments. A stock whose encoding raises is served from its last
     good encoding and counted (`render_errors`); one bad stock never fails a response.
   * **Peer circuit breaker.** A failing peer is not contacted again for 1 s doubling to 15 s, and
     requests never queue behind a request already talking to the peer; they get its last answer.
     A peer that accepts connections but never answers delays at most one request per back-off.
   * **Only current state is exchanged.** Only the peer's current session crosses the wire; there
     is no history to transfer.
   * **Symmetric routes.** Node 1 serves the same URLs, so it can stand in for node 0.
2. **Separate shard endpoints per node.** Zero aggregation cost, but every consumer would need two
   base URLs and `/public/live.json` would disappear. That breaks existing consumers.
3. **Push or replication** (node 1 streams its state to node 0, a shared store, Redis). More moving
   parts, a second copy of half the universe on node 0 at all times, and Redis or a database is out
   of scope.

**Failure behaviour.** A peer that is down never takes the aggregating node down:

* The aggregating node keeps serving its own half.
* A recent peer snapshot (up to 15 s old) is reused through a brief blip and marked `stale`.
* After that, `/public/live.json` returns `status: "PARTIAL"` with only the reachable stocks and a
  `coverage` block naming the missing node. A LIVE peer that holds fewer stocks than its partition
  also makes the response `PARTIAL` (`missing_stock_count`).
* The peer's shards and stocks return HTTP 503 `NODE_UNAVAILABLE`.

Stocks are never invented or silently dropped under an `OK`.

## Session and data rules

* **Session window.** Asia/Kolkata, trading days per `runtime_guard.is_trading_day` (weekends,
  `PSYGRID_MARKET_HOLIDAYS`, `PSYGRID_SPECIAL_SESSIONS`), 09:15-15:15. No Dhan feed outside it.
* **Before the open.** 09:00-09:15: security ids are resolved. 09:15: authenticate (one forced
  token refresh on 401/807-809, Dhan's token rate limit respected), one REST quote snapshot for
  reference prices and the volume baseline, then the WebSocket.
* **Candle rules (as the full app).** The minute comes from the exchange LTT. Volume is the delta
  of Dhan's cumulative volume. A repeated trade is ignored. A Dhan historical bar beats a
  WebSocket-built one. Only completed candles are published.
* **Publication timing.** A minute is published once it has ended plus 3 s, rather than waiting
  for the stock's next trade. Once published, a candle is immutable: a trade that arrives later
  for an already published minute is dropped (`late_minute`), its cumulative volume is carried
  forward into the next candle, and only an authoritative Dhan historical bar may replace a
  published candle. A minute without trades has no candle.
* **Freshness (locked rule).** A stock is stale only when its last accepted packet is more than
  120 s old (`age_seconds > 120`); at exactly 120 s it is fresh. Only accepted packets refresh a
  stock. Stale stocks are listed (`stale_symbols_sample`, up to 25) and never stop the node or the
  cluster from serving.
* **Data-level isolation (fail soft).** A packet is rejected, counted under `rejected_packets`
  and otherwise ignored when it is malformed, has a non-finite or non-positive price, exceeds
  sane bounds, carries an LTT outside today's session day (e.g. yesterday's last trade), belongs to
  an already published minute, or reports a cumulative volume lower than the baseline. A
  non-positive or zero day volume (no trade yet today) refreshes the stock but never creates a
  candle (`no_trade_today_packets`). Repeated trades are counted (`duplicate_trades`). Historical
  bars are validated the same way (session date, finite values, OHLC geometry) and are never merged
  after 15:15.
* **Health dimensions.** `/health` and `/health/node` report `dimensions` separately for `http`,
  `feed` (CONNECTED, RECONNECTING, ...), `data` (FRESH, FRESH_WITH_STALE_STOCKS, STALE, NO_DATA,
  CLOSED) and `coverage` (COMPLETE, INCOMPLETE, IDLE). A reconnecting feed with fresh data is not a
  data failure, and a few stale stocks are `FRESH_WITH_STALE_STOCKS`, not a node failure.
* **After a restart or a feed reconnect.** Today's genuine Dhan 1m bars are fetched by one thread at
  2 requests/s at most, 90 s after the reconnect, so the gap is filled from Dhan, not guessed.
* **At 15:15.** The feed is stopped and its loop closed, the history thread stopped, all market state,
  cached bodies and peer snapshots dropped, then `gc` + `malloc_trim`. Nothing is archived. The
  unit runs with `ProtectSystem=strict`, `ProtectHome=read-only` and no writable path. A test also
  records every Python file-write audit event across a whole session and requires none.

## Feed resilience

Every connection cycle goes through `feed.LiveFeed._run`, whose `finally` calls
`runtime_guard.close_market_feed`: `close_connection()`, cancel the loop's pending tasks, shut down
async generators, close `feed.loop`. `LiveCoreFeed` counts cycles and checks after each one that the
loop is really closed (`event_loops_leaked` in `/health`).

It also replaces `stop()`. The inherited `LiveFeed.stop()` calls `MarketFeed.close_connection()` from
the stopping thread; if that lands after a connection is built but before its loop runs, dhanhq runs
the feed's loop on the stopping thread. That can collide with the feed thread's own `run()`, make
`close_market_feed` skip the close because the loop is "running", or leave a connection that started
afterwards open. A test reproduces this: the inherited stop waits 8 s and leaves the feed running.
`LiveCoreFeed.stop()` only signals thread-safely and lets the feed thread clean up itself. The full
app's `feed.py` has the same latent race at its 15:15 stop and shutdown. It is not changed here.

Further protections in `live_core/feed.py`:

* **Packet isolation.** An exception while handling one packet is caught and counted
  (`packet_errors`, logged only for the first 3 and every 1000th). Without this, dhanhq's loop
  treats it as a connection error and sleeps 1 s, dropping every stock's packets in that second.
* **Silence watchdog.** With no message for 45 s during the session, the connection is closed and a
  new cycle started (`silence_reconnects`).
* **Stale resubscription.** Stocks stale for more than 120 s are re-subscribed on the live
  connection in batches of 100 (at most 5 batches per pass, each stock at most every 300 s). A
  failed resubscribe is recorded but does not flip the feed status.
* **Internal reconnects.** dhanhq reconnects inside one `MarketFeed` without the Live Core seeing a
  new cycle. Each such reconnect is counted (`internal_reconnects`) and schedules the same 90 s
  history refill as an outer reconnect, so the gap is filled.
* **Status recovery.** A feed in ERROR returns to CONNECTED as soon as quote packets flow again.
* **Credential redaction.** Every stored or logged error passes through `live_core/redact.py`
  (the Dhan secret values, runtime tokens, URL query strings, JWTs, `token=`/`pin=`-style pairs and
  long opaque strings) and a logging filter redacts every log record. dhanhq's WebSocket URL
  carries the token, so this matters for connection errors. Tests scan every endpoint for the
  configured secrets.

Tests drive dhanhq's real `MarketFeed` (only the network connect is replaced) through hundreds of
failed connections. Every loop closes, and under uvloop, as in production, descriptor growth over
300 reconnects is at most 5. With the guard replaced by a bare `close_connection()`, 5 of the 6 lifecycle
tests fail. HTTP handlers only read RAM: a test keeps the feed failing and reconnecting while every
endpoint answers 200 in well under a second.

## Resources (measured, two real processes, real dhanhq over a local WebSocket, full 09:15-15:15 session)

| | node 0 (aggregator) | node 1 |
| --- | --- | --- |
| idle, holding a full day (~178k candles per node) | ~65 MB | ~65 MB |
| 3 clients polling full `live.json` + shards every second for 7 min (with `malloc_trim` every 60 s) | 113-135 MB, no upward trend | 84-103 MB |
| `live.json` (989 stocks x 360 candles) | 38 MB raw / 3.3 MB gzip | |
| `live.json` steady state (content unchanged) | ~2 ms in-process; 9 ms p50 / 19 ms p95 over HTTP | |
| `live.json` first request after a minute completes | ~0.35 s over HTTP (up to ~1.4 s in-process under load) | |
| mixed-endpoint stress (134,087 requests) | 0 errors, p50 7.7 ms, p99 39.6 ms, fds stable at 22-32 | |
| 60 forced WebSocket drops | fds unchanged, 0 event loops leaked, data FRESH afterwards | |

Polling overhead is about 3% of one core. Real Dhan packet rates and the E2.1.Micro's 1/8 OCPU
baseline were not available for measurement; see the open risks.

The systemd unit sets `MemoryHigh=650M`, `MemoryMax=800M`, `LimitNOFILE=8192`, `TasksMax=128`,
`MALLOC_ARENA_MAX=2`. The watchdog stops feeding systemd above 600 MB RSS or 85% of the fd limit.
`install.sh` adds a 1 GB swap file if the VM has none. Threads: HTTP pool (8), session, feed and
feed watchdog, history.

## Configuration

| Variable | Where | Meaning |
| --- | --- | --- |
| `LIVE_CORE_NODE_ID`, `LIVE_CORE_NODE_COUNT` | systemd unit | identity (0/1 of 2) |
| `LIVE_CORE_PEERS` | `/etc/psygrid-live-core-node.env` (written by deploy) | e.g. node 0: `1=http://10.0.0.12:10000` |
| `LIVE_CORE_PORT` | same | default 10000 |
| `DHAN_CLIENT_ID` + `DHAN_ACCESS_TOKEN` or `DHAN_PIN` + `DHAN_TOTP_SECRET` | `/etc/psygrid-live-core.env` (mode 600, on the VM only) | same account and data plan as the full PSYGRID |
| `PSYGRID_MARKET_HOLIDAYS`, `PSYGRID_SPECIAL_SESSIONS` | same | trading-day overrides |
| `LIVE_CORE_HISTORY_BOOTSTRAP` (1), `LIVE_CORE_HISTORY_INTERVAL_SECONDS` (0.5) | optional | Dhan 1m bar refill |
| `LIVE_CORE_FINALIZE_GRACE_SECONDS` (3) | optional | publish delay after a minute ends |
| `LIVE_CORE_RENDER_CACHE_SECONDS` (1) | optional | whole-response reuse window (the stocks part is reused while unchanged) |
| `LIVE_CORE_PEER_TIMEOUT_SECONDS` (4), `LIVE_CORE_PEER_CACHE_SECONDS` (1), `LIVE_CORE_PEER_STALE_SECONDS` (15) | optional | peer exchange |
| `LIVE_CORE_MAX_RSS_MB` (600), `LIVE_CORE_HTTP_THREADS` (8) | optional | budgets |

`PSYGRID_ARCHIVE` is ignored (a warning is logged).

## Deployment

The `PSYGRID Live Core Deploy (E2 Micro nodes)` workflow (`deploy-live-core.yml`) is manual only and
refuses to run during 09:00-15:20 IST on weekdays unless `allow_market_hours` is ticked, because a
restart drops that node's RAM session. Its `action` input is `status` (read-only: OS, Python,
memory, the names but never the values of the variables in `/etc/psygrid-live-core.env`, unit state,
listeners, local and public health, cluster view and `/public/live.json` summary) or `deploy`, which:

1. Runs the Live Core tests plus the shared feed/runtime-guard/contract tests.
2. Builds a bundle of only the files in `deploy/live-core/MANIFEST`. `app.py` and the full app's
   managers never reach the micro VMs.
3. On each VM, extracts it to `~/psygrid-live-core/releases/<sha>` and runs `install.sh`, which:
   * creates the venv and installs `deploy/live-core/requirements.txt`;
   * validates the partition before switching the `current` symlink;
   * writes the topology env and renders `psygrid-live-core-node<N>.service`;
   * opens the port in iptables, enables and restarts the unit;
   * keeps the last 3 releases.
4. Checks `/health/node` on each VM. Every selected node is attempted even if an earlier one fails.
5. From the internet: port 10000 on both VMs, the cluster view from node 0 (for `both`, both nodes
   must be reachable with matching partitions) and a summary of node 0's `/public/live.json`.

One-time setup:

1. **Repository settings.** Secrets `LIVE_CORE_NODE0_SSH_KEY` and `LIVE_CORE_NODE1_SSH_KEY`: the
   private keys of the two VMs, which were created with different key pairs. Node 0 is deployed
   only with the node 0 key and node 1 only with the node 1 key; a target of `node0` needs only the
   first, `node1` only the second. Keys are never stored in the repository. The hosts default to
   129.225.112.47 (node 0) and 140.245.228.101 (node 1); `LIVE_CORE_NODE0_HOST` /
   `LIVE_CORE_NODE1_HOST` override them. Optional `LIVE_CORE_NODE0_PRIVATE_URL` /
   `LIVE_CORE_NODE1_PRIVATE_URL` keep peer traffic inside the VCN; without them the nodes use each
   other's public IP. `LIVE_CORE_SSH_USER` is optional: `ubuntu`, then `opc`, is tried.
2. **VMs.** Ubuntu 24.04 (Python 3.12). On each VM, create `/etc/psygrid-live-core.env` with the
   Dhan credentials (the first deploy creates a template and reports `CONFIG_ERROR` until it is
   filled).
3. **Network.** Allow TCP 10000 in the VCN security list: from the internet for node 0, and at least
   from node 0's private IP for node 1.

Either node can be deployed first. While one is missing, the other serves its half and reports
`PARTIAL` / `LIVE_INCOMPLETE`.

## Intentional differences from the full PSYGRID

The full PSYGRID's endpoints are the specification; the Live Core matches their shapes and values
(golden-shape and value-parity tests). The deliberate differences:

* **Publication timing.** A minute is published 3 s after it ends, not at the stock's next trade.
* **Published candles are immutable.** A late trade for a published minute is dropped (its volume
  carries into the next candle) instead of rewriting the minute.
* **Volume regression.** A cumulative volume lower than the baseline is rejected rather than
  resetting the baseline.
* **No trade today.** A zero day-volume packet (e.g. yesterday's last trade echoed at the open)
  never produces a candle.
* **Additions only.** `/public/live.json` adds `coverage`; health payloads add Live Core blocks
  (`dimensions`, `data_quality`, `live_core`). `index_layer.feed_status` reflects the equity feed,
  as the Live Core has no index layer. No full-PSYGRID key is removed or renamed.

## Operational notes and open risks

* **Dhan WebSocket connections.** The two nodes use 2 of the account's market-feed WebSocket
  connections (Dhan allows up to 5 per account, 5,000 instruments each). If the full PSYGRID runs at
  the same time on the same account, its equity, index and depth feeds count too. A node that hits
  the limit reports it and retries every 300 s.
* **Token generation.** With PIN + TOTP, each node and the full PSYGRID generate their own daily
  token. Whether a newly generated Dhan token invalidates earlier ones has not been verified here.
  If it does, the processes would knock each other's tokens out. Each one regenerates on 401/807
  and Dhan's 2-minute token rate limit is respected, but the safe setup is one daily
  `DHAN_ACCESS_TOKEN` shared by all processes, or confirming Dhan's behaviour before relying on TOTP
  on several hosts.
* **Data API rate.** The history refill is at most 2 requests/s per node; at 0.5 s per request a
  node's 495-stock refill takes about 4 minutes.
* **Restarts.** A restart mid-session loses that node's RAM candles until the refill completes;
  the refill restores them from Dhan.
* **Dhan single live token.** Dhan issues one live access token per client. Giving the nodes their
  own credentials while the full PSYGRID runs on the same client would invalidate one of them. The
  nodes stay in `CONFIG_ERROR` until this is decided.
* **CPU on E2.1.Micro.** The VMs have a 1/8 OCPU baseline. Frequent polling of the full
  `/public/live.json` (38 MB raw) is the most expensive request; prefer shards or gzip.
* **dhanhq binary parse errors.** A malformed binary frame raised inside dhanhq itself (before the
  Live Core's handler) still triggers dhanhq's internal 1 s sleep; this cannot be fixed without
  patching dhanhq.
