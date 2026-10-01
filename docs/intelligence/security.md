# Security review

Scope: the `psygrid-intelligence` service (`intelligence/`, the unit, the
deploy step). PSYGRID's existing `/public` API is out of scope and unchanged.

## Assets and trust boundaries

| Asset | Where | Protection |
| --- | --- | --- |
| The production feed and PSYGRID process | `psygrid` unit | Separate process and unit; intelligence only reads files and five read-only localhost endpoints; capped CPU, I/O, memory; higher OOM score; never imports `app` (tested) |
| Derived market data | `/v2`, `events.db` | API key on every data route; localhost bind by default; data-rights register |
| API keys | `api_keys.json` | Only SHA-256 hashes stored; file mode 0600; key shown once |
| Dhan credentials | PSYGRID's environment | Not used, read or needed by the intelligence service |

## Controls

- **Authentication.** Keys are `psg_<8 hex id>_<256-bit random secret>`. Only
  SHA-256 of the full key is stored. A slow hash is unnecessary because the
  secret is random, not a password. Comparison uses `hmac.compare_digest`.
  Revocation takes effect on the next request: the file is re-read when its
  modification time changes.
- **Authorisation.** One level: a valid key reads everything under `/v2`.
  `/v2/health`, `/v2/ready`, `/v2/docs` and `/v2/openapi.json` are public and
  carry no market data.
- **Rate limiting.** Per-key token bucket (429 with `Retry-After`). Server-wide
  `limit_concurrency=200`. At most 2 concurrent similarity searches, with
  results cached. Stream limits: 50 in total, 5 per key. WebSocket messages
  are capped at 64 KB.
- **Input validation.** Every path and query parameter is validated: patterns
  for symbols, dates and event ids, bounded limits, enumerated values, and
  unknown feature names rejected. SQL is parameterised; the only interpolated
  SQL fragments are fixed column names chosen in code.
- **Logging.** The access log never records query strings (a stream key can be
  in one) or headers. It records the key *id* only.
- **Transport.** HTTP on localhost by default. Exposing the port requires an
  explicit setting, a firewall change and a TLS proxy (see
  [deployment.md](deployment.md)).
- **Process hardening.** systemd `NoNewPrivileges`, `PrivateTmp`,
  `ProtectSystem=full`, `ProtectHome=read-only` with one writable directory,
  `TasksMax`, memory ceiling.
- **Response headers.** `X-Content-Type-Options: nosniff`,
  `Cache-Control: no-store`. No CORS headers, so browsers on other origins
  cannot read responses.
- **Availability isolation.** Each tick catches its own failures; a failing
  PSYGRID endpoint or a missing archive cannot stop the loop or affect PSYGRID.

## Findings and residual risks

| # | Finding | Severity | Status |
| --- | --- | --- | --- |
| 1 | A key passed as `?api_key=` (needed by browsers for WebSockets) can appear in proxy or browser history logs | Low | Documented; headers are preferred; the service's own log omits query strings |
| 2 | Rate-limit state is in memory and resets on restart | Low | Accepted: restarts are rare and keys are few |
| 3 | `/v2/health` shows the last error message, which may name file paths | Low | Accepted while the service binds to localhost; revisit before exposing it |
| 4 | Failed authentications are not rate-limited per client | Low | Accepted: guessing a 256-bit secret is infeasible; a TLS proxy can add connection limits |
| 5 | Derived data may be subject to exchange redistribution rules | Policy | Open: keys only for the account holder's own engines until [data-rights.md](data-rights.md) question 3 is answered |
| 6 | Dependencies are pinned but not scanned automatically | Low | Recommended: enable GitHub Dependabot alerts (free) |
| 7 | No TLS in the service itself | Medium if exposed | Mitigated by the localhost default; TLS proxy required before exposure |

No critical or high findings are open.
