"""Bootstrap the PSYGRID archive with genuine Dhan 1m history, so Intelligence has baselines from day one.

    python -m intelligence bootstrap-history plan                 # estimate requests, storage, API budget; no API calls
    python -m intelligence bootstrap-history run                  # download (resumable), assemble, verify
    python -m intelligence bootstrap-history status               # progress of the current batch and the archive
    python -m intelligence bootstrap-history verify               # integrity check of every archived day
    python -m intelligence bootstrap-history run --sessions 60    # explicit, resumable expansion to deeper history

What it downloads. For the last ``--sessions`` completed trading sessions
(default 20: the Intelligence baseline window), each universe equity's and
each index's 1m candles from Dhan's intraday-history API (one request per
instrument per span of up to 90 calendar days), and each equity's daily
candles from the historical API (one request per equity) for the genuine
previous close and day open. The trading calendar comes from Dhan's own
NIFTY daily candles, so holidays are never guessed.

What it writes. One directory per session in the archive, in exactly the
format ``daily_archive.py`` writes and ``intelligence/archive.py`` reads, with
a manifest recording the source, per-file row counts and SHA-256, and the
integrity report. Values are Dhan's, unchanged. Rows that fail validation
(non-numeric, broken OHLC geometry, outside the 09:15-15:30 session,
duplicate minute) are left out and counted in the manifest: nothing is
repaired, interpolated or invented. A day directory that already exists and
was not written by the bootstrap (PSYGRID's live archive) is never touched,
and today is never written, so the live archive keeps appending sessions.

How it stays safe. It runs as its own process, outside market hours only
(08:45-15:45 IST on weekdays is refused), at a bounded request rate (2 per
second by default; Dhan's Data API limit is 5 per second per account, shared
with the live feed) with bounded concurrency, and it never generates a Dhan
access token unless explicitly told to. Each instrument's download is staged
to its own file the moment it arrives (written atomically, with a digest), so
memory stays small and an interrupted run resumes where it stopped.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import math
import os
import shutil
import threading
import time
from collections import Counter
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

from daily_archive import (
    EQUITY_COLUMNS,
    EQUITY_FILE,
    EQUITY_REFERENCE_COLUMNS,
    EQUITY_REFERENCE_FILE,
    INDEX_COLUMNS,
    INDEX_FILE,
    MANIFEST_FILE,
)
from intelligence.archive import IST, available_days
from intelligence.archive_integrity import digests_match, sha256_file, verify_day
from output import _ist_timestamp, _price

TOOL_VERSION = "1"
SOURCE = "DHAN_HISTORICAL_API"
BASE_URL = "https://api.dhan.co/v2"
MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
DEFAULT_SESSIONS = 20  # history.build_baselines' window: full baselines and similarity without FEW_SESSIONS
MAX_SPAN_DAYS = 90  # Dhan intraday history: at most 90 calendar days per request
DEFAULT_RATE = 2.0  # requests per second; Dhan Data APIs allow 5/s per account, shared with the live feed
MAX_RATE = 4.0
DEFAULT_WORKERS = 2
MAX_WORKERS = 4
DATA_API_DAILY_LIMIT = 100_000  # Dhan's documented Data API requests per day
GUARD_START, GUARD_END = "08:45", "15:45"
SESSION_OPEN, SESSION_CLOSE = (9, 15), (15, 30)
CALENDAR_PROBE_DAYS = 400  # calendar days of NIFTY daily candles fetched to find trading sessions
REFERENCE_PAD_DAYS = 15  # extra calendar days of daily candles before the first session, for its previous close
BYTES_PER_SESSION = 6_200_000  # measured: one 989-stock session in the archive format
MAX_ATTEMPTS = 6
STAGING_DIR = ".bootstrap"
NIFTY = ("nifty", "13", "IDX_I", "INDEX")

AUTH_CODES = {"DH-901", "DH-902", "DH-903"}  # invalid token, no Data API access, account issue: stop the run
RATE_CODES = {"DH-904"}
NO_DATA_CODES = {"DH-907"}
RETRY_CODES = {"DH-908", "DH-909", "DH-910"}


class BootstrapBlocked(RuntimeError):
    """A condition the bootstrap must not work around (credentials, entitlement, market hours)."""


class RequestFailed(RuntimeError):
    pass


@dataclass(frozen=True)
class Instrument:
    kind: str  # "equity" or "index"
    key: str  # symbol, or the index route key
    symbol: str  # as archived: the equity symbol, or the index's PSYGRID symbol
    security_id: str
    segment: str
    instrument: str

    @property
    def stage_name(self) -> str:
        safe = "".join(ch if ch.isalnum() or ch in "-_" else f"%{ord(ch):02X}" for ch in self.key)
        return f"{self.kind}-{safe}"


# --- credentials ----------------------------------------------------------------------------------


def read_env_file(path: Path) -> dict[str, str]:
    """KEY=VALUE lines (systemd EnvironmentFile syntax, optional quotes); values are never logged."""
    values = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip().removeprefix("export ").strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        values[key] = value
    return values


def credentials(env: dict[str, str], allow_generate: bool = False) -> tuple[str, str, str]:
    """(client id, access token, how it was obtained). Never generates a token unless ``allow_generate``.

    Generating a token from PIN + TOTP is not done by default because the
    production service may hold a token generated the same way; whether Dhan
    expires an older token when a new one is generated is not something this
    tool should find out during a run.
    """
    client_id = env.get("DHAN_CLIENT_ID", "").strip()
    if not client_id:
        raise BootstrapBlocked("DHAN_CLIENT_ID is not set (pass --env-file pointing at PSYGRID's environment file)")
    token_var = env.get("DHAN_TOKEN_VAR", "").strip() or "DHAN_ACCESS_TOKEN"
    token = env.get(token_var, "").strip()
    if token:
        return client_id, token, f"environment:{token_var}"
    if env.get("DHAN_PIN") and env.get("DHAN_TOTP_SECRET"):
        if not allow_generate:
            raise BootstrapBlocked(
                "No Dhan access token in the environment; only PIN + TOTP. Generating a token could expire the token "
                "PSYGRID is using. Provide DHAN_ACCESS_TOKEN (for example a token from web.dhan.co), or rerun with "
                "--generate-token outside market hours to accept that risk."
            )
        from dhan_auth import generate_access_token

        token, _ = generate_access_token(client_id, env["DHAN_PIN"], env["DHAN_TOTP_SECRET"])
        return client_id, token, "generated:totp"
    raise BootstrapBlocked(f"No Dhan credentials: set {token_var}, or DHAN_PIN and DHAN_TOTP_SECRET")


# --- client -------------------------------------------------------------------------------------


class RateLimiter:
    """At most ``rate`` requests per second across all workers, plus a shared cooldown after a 429."""

    def __init__(self, rate: float, clock=time.monotonic, sleep=time.sleep):
        self.interval = 1.0 / max(0.1, min(MAX_RATE, rate))
        self._clock, self._sleep = clock, sleep
        self._next = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = self._clock()
            wait = self._next - now
            self._next = max(now, self._next) + self.interval
        if wait > 0:
            self._sleep(wait)

    def cool_down(self, seconds: float) -> None:
        with self._lock:
            self._next = max(self._next, self._clock() + seconds)


def _http_post(url: str, headers: dict, payload: dict, timeout: float = 30.0):
    response = requests.post(url, headers=headers, json=payload, timeout=timeout)
    try:
        body = response.json()
    except ValueError:
        body = {"errorMessage": response.text[:300]}
    return response.status_code, dict(response.headers), body


class HistoryClient:
    def __init__(self, client_id: str, token: str, rate: float = DEFAULT_RATE, post=_http_post, sleep=time.sleep):
        self.headers = {"Accept": "application/json", "Content-Type": "application/json",
                        "access-token": token, "client-id": client_id}  # fmt: skip
        self.limiter = RateLimiter(rate, sleep=sleep)
        self._post, self._sleep = post, sleep
        self.requests = 0
        self._count_lock = threading.Lock()

    def post(self, path: str, payload: dict) -> dict | None:
        """The response body, or None when Dhan reports no data. Raises BootstrapBlocked or RequestFailed."""
        last = ""
        for attempt in range(MAX_ATTEMPTS):
            self.limiter.acquire()
            with self._count_lock:
                self.requests += 1
            try:
                status, headers, body = self._post(BASE_URL + path, self.headers, payload)
            except (requests.Timeout, requests.ConnectionError) as exc:
                last = f"network: {exc}"
                self._sleep(min(30.0, 2.0**attempt))
                continue
            code = str((body or {}).get("errorCode", "")) if isinstance(body, dict) else ""
            if status == 401 or code in AUTH_CODES:
                raise BootstrapBlocked(f"Dhan rejected the credentials or Data API access ({status} {code}): "
                                       f"{(body or {}).get('errorMessage', '')}")  # fmt: skip
            if status == 429 or code in RATE_CODES:
                retry = _retry_after(headers, attempt)
                self.limiter.cool_down(retry)
                last = f"rate limited ({status} {code})"
                continue
            if code in NO_DATA_CODES:
                return None
            if status >= 500 or code in RETRY_CODES:
                last = f"HTTP {status} {code}"
                self._sleep(min(30.0, 2.0**attempt))
                continue
            if status >= 400 or not isinstance(body, dict):
                raise RequestFailed(f"HTTP {status} {code}: {(body or {}).get('errorMessage', body)}"[:300])
            return body
        raise RequestFailed(f"gave up after {MAX_ATTEMPTS} attempts: {last}")

    def intraday(self, inst: Instrument, start: datetime, end: datetime) -> dict | None:
        return self.post("/charts/intraday", {
            "securityId": inst.security_id, "exchangeSegment": inst.segment, "instrument": inst.instrument,
            "interval": "1", "oi": False,
            "fromDate": start.strftime("%Y-%m-%d %H:%M:%S"), "toDate": end.strftime("%Y-%m-%d %H:%M:%S"),
        })  # fmt: skip

    def daily(self, inst: Instrument, start: date, end: date) -> dict | None:
        return self.post("/charts/historical", {
            "securityId": inst.security_id, "exchangeSegment": inst.segment, "instrument": inst.instrument,
            "expiryCode": 0, "oi": False, "fromDate": start.isoformat(), "toDate": end.isoformat(),
        })  # fmt: skip


def _retry_after(headers: dict, attempt: int) -> float:
    try:
        return max(1.0, min(60.0, float(headers.get("Retry-After") or headers.get("retry-after"))))
    except (TypeError, ValueError):
        return min(60.0, 2.0 * 2**attempt)


# --- parsing --------------------------------------------------------------------------------------


def parse_candles(body: dict | None) -> tuple[list[tuple], Counter]:
    """Dhan's column arrays to rows (epoch, open, high, low, close, volume), exactly as sent.

    Rows that cannot be used are counted by reason and left out; nothing is changed.
    """
    rejected: Counter = Counter()
    if not body:
        return [], rejected
    names = ("timestamp", "open", "high", "low", "close", "volume")
    arrays = [body.get(n) for n in names]
    if not all(isinstance(a, list) for a in arrays):
        rejected["malformed_response"] += 1
        return [], rejected
    if len({len(a) for a in arrays}) != 1:
        raise RequestFailed("Dhan returned arrays of different lengths")
    rows, seen = [], set()
    for values in zip(*arrays, strict=True):
        try:
            epoch = int(values[0])
            o, h, low, c = (float(v) for v in values[1:5])
            volume = int(values[5])
        except (TypeError, ValueError):
            rejected["non_numeric"] += 1
            continue
        if not all(math.isfinite(x) for x in (o, h, low, c)):
            rejected["non_numeric"] += 1
            continue
        if epoch % 60:
            rejected["unaligned_timestamp"] += 1
            continue
        moment = datetime.fromtimestamp(epoch, IST)
        if not SESSION_OPEN <= (moment.hour, moment.minute) < SESSION_CLOSE:
            rejected["outside_session"] += 1
            continue
        if h < max(o, c) or low > min(o, c) or low <= 0 or volume < 0:
            rejected["invalid_ohlc"] += 1
            continue
        if epoch in seen:
            rejected["duplicate_minute"] += 1
            continue
        seen.add(epoch)
        rows.append((epoch, o, h, low, c, volume))
    rows.sort()
    return rows, rejected


def daily_reference(body: dict | None) -> dict[str, dict]:
    """date -> {"open", "close"} from Dhan daily candles (only rows with valid numbers)."""
    out = {}
    if not body:
        return out
    stamps, opens, closes = body.get("timestamp") or [], body.get("open") or [], body.get("close") or []
    for stamp, o, c in zip(stamps, opens, closes, strict=False):
        try:
            day = datetime.fromtimestamp(int(stamp), IST).strftime("%Y-%m-%d")
            o, c = float(o), float(c)
        except (TypeError, ValueError):
            continue
        if math.isfinite(o) and math.isfinite(c) and o > 0 and c > 0:
            out[day] = {"open": o, "close": c}
    return out


# --- instruments ----------------------------------------------------------------------------------


def universe_symbols(path: Path | None = None) -> list[str]:
    path = Path(path or os.getenv("PSYGRID_STOCKS_FILE", "stocks.json"))
    return [str(s).strip().upper() for s in json.loads(path.read_text())["symbols"] if str(s).strip()]


def resolve_instruments(symbols: list[str], lines=None) -> tuple[list[Instrument], list[str]]:
    """Equities and the 16 PSYGRID indices from Dhan's instrument master, streamed (never held whole).

    Returns (instruments, unresolved keys). ``lines`` replaces the download in tests.
    """
    from index_layer import INDEX_FALLBACK_IDS, INDEX_SPECS

    def norm(value) -> str:
        return "".join(ch for ch in str(value or "").upper() if ch.isalnum())

    wanted = set(symbols)
    equities: dict[str, str] = {}
    aliases = {key: {norm(a) for a in spec[1]} for key, spec in INDEX_SPECS.items()}
    index_matches: dict[str, set[str]] = {key: set() for key in INDEX_SPECS if key not in INDEX_FALLBACK_IDS}
    response = None
    if lines is None:
        response = requests.get(MASTER_URL, timeout=60, stream=True)
        response.raise_for_status()
        lines = response.iter_lines(decode_unicode=True)
    try:
        for row in csv.DictReader(line for line in lines if line):
            name = str(row.get("SEM_INSTRUMENT_NAME", "")).strip().upper()
            security_id = str(row.get("SEM_SMST_SECURITY_ID", "")).strip()
            if not security_id:
                continue
            if name == "EQUITY" and str(row.get("SEM_EXM_EXCH_ID", "")).strip().upper() == "NSE":
                symbol = str(row.get("SEM_TRADING_SYMBOL", "")).strip().upper()
                if symbol in wanted:
                    equities.setdefault(symbol, security_id)
            elif name == "INDEX" and str(row.get("SEM_SEGMENT", "")).strip().upper() in {"I", "IDX_I", "INDEX"}:
                names = {norm(row.get(f)) for f in ("SEM_TRADING_SYMBOL", "SEM_CUSTOM_SYMBOL", "SM_SYMBOL_NAME")}
                for key in index_matches:
                    if names & aliases[key]:
                        index_matches[key].add(security_id)
    finally:
        if response is not None:
            response.close()
    instruments = [Instrument("equity", s, s, equities[s], "NSE_EQ", "EQUITY") for s in sorted(equities)]
    unresolved = sorted(wanted - set(equities))
    for key in sorted(INDEX_SPECS):
        security_id = INDEX_FALLBACK_IDS.get(key)
        if security_id is None:
            matches = index_matches[key]
            if len(matches) != 1:
                unresolved.append(f"index:{key}")
                continue
            security_id = next(iter(matches))
        instruments.append(Instrument("index", key, INDEX_SPECS[key][0], security_id, "IDX_I", "INDEX"))
    return instruments, unresolved


# --- planning -------------------------------------------------------------------------------------


def market_hours_guard(now: datetime) -> str | None:
    """A reason to refuse to run now, or None."""
    if now.weekday() < 5:
        start = now.replace(hour=int(GUARD_START[:2]), minute=int(GUARD_START[3:]), second=0, microsecond=0)
        end = now.replace(hour=int(GUARD_END[:2]), minute=int(GUARD_END[3:]), second=0, microsecond=0)
        if start <= now <= end:
            return f"refusing to download between {GUARD_START} and {GUARD_END} IST on a weekday (market hours)"
    return None


def _spans(first: str, last: str) -> list[tuple[date, date]]:
    """Calendar spans of at most MAX_SPAN_DAYS covering first..last."""
    start, end = date.fromisoformat(first), date.fromisoformat(last)
    out = []
    while start <= end:
        stop = min(end, start + timedelta(days=MAX_SPAN_DAYS - 1))
        out.append((start, stop))
        start = stop + timedelta(days=1)
    return out


def estimate(sessions: list[str], equities: int, indices: int, rate: float, existing_bytes: int = 0) -> dict:
    """Requests, time, storage and share of Dhan's daily Data API budget for downloading ``sessions``."""
    spans = len(_spans(sessions[0], sessions[-1])) if sessions else 0
    intraday = (equities + indices) * spans
    daily = equities if sessions else 0
    total = intraday + daily + 1  # + the calendar probe
    seconds = total / max(0.1, min(MAX_RATE, rate))
    archive_bytes = len(sessions) * BYTES_PER_SESSION
    return {
        "sessions": len(sessions),
        "first_session": sessions[0] if sessions else None,
        "last_session": sessions[-1] if sessions else None,
        "requests": {"intraday": intraday, "daily": daily, "calendar_probe": 1, "total": total},
        "rate_per_second": rate,
        "estimated_minutes": round(seconds / 60, 1),
        "dhan_daily_budget_share": round(total / DATA_API_DAILY_LIMIT, 4),
        "storage_bytes": {"archive": archive_bytes, "staging_peak": archive_bytes, "existing_archive": existing_bytes},
    }


def weekday_sessions(today: date, count: int) -> list[str]:
    """The last ``count`` weekdays before today: an offline stand-in for the calendar (holidays not known)."""
    out, day = [], today
    while len(out) < count:
        day -= timedelta(days=1)
        if day.weekday() < 5:
            out.append(day.isoformat())
    return sorted(out)


# --- the batch ------------------------------------------------------------------------------------


@dataclass
class BatchPaths:
    root: Path

    @property
    def plan(self) -> Path:
        return self.root / "plan.json"

    @property
    def progress(self) -> Path:
        return self.root / "progress.json"

    def stage(self, inst: Instrument) -> Path:
        return self.root / "intraday" / f"{inst.stage_name}.csv.gz"

    def daily(self, inst: Instrument) -> Path:
        return self.root / "daily" / f"{inst.stage_name}.json"


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    with open(tmp, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def _write_stage(path: Path, rows: list[tuple], rejected: Counter, requests_made: int) -> None:
    """One instrument's rows, staged atomically with a sidecar recording the count and digest."""
    text = "".join(f"{e},{o!r},{h!r},{low!r},{c!r},{v}\n" for e, o, h, low, c, v in rows)
    data = gzip.compress(text.encode(), compresslevel=6, mtime=0)
    _atomic_write_bytes(path, data)
    meta = {"rows": len(rows), "sha256": hashlib.sha256(data).hexdigest(), "rejected": dict(rejected),
            "requests": requests_made}  # fmt: skip
    _atomic_write_bytes(path.with_suffix(".json"), json.dumps(meta).encode())


def stage_complete(path: Path) -> bool:
    """A staged file is complete when its sidecar exists and its digest matches (else it is downloaded again)."""
    meta_path = path.with_suffix(".json")
    if not (path.exists() and meta_path.exists()):
        return False
    try:
        meta = json.loads(meta_path.read_text())
        return hashlib.sha256(path.read_bytes()).hexdigest() == meta["sha256"]
    except (OSError, ValueError, KeyError):
        return False


def _read_stage(path: Path):
    with gzip.open(path, "rt") as handle:
        for line in handle:
            e, o, h, low, c, v = line.rstrip("\n").split(",")
            yield int(e), float(o), float(h), float(low), float(c), int(v)


class Bootstrap:
    def __init__(self, archive_root: Path, client: HistoryClient | None = None, *, sessions: int = DEFAULT_SESSIONS,
                 workers: int = DEFAULT_WORKERS, clock: Callable[[], datetime] | None = None,
                 log: Callable[[str], None] = print, master_lines=None, symbols: list[str] | None = None):  # fmt: skip
        self.archive = Path(archive_root)
        self.client = client
        self.sessions = max(1, int(sessions))
        self.workers = max(1, min(MAX_WORKERS, int(workers)))
        self.clock = clock or (lambda: datetime.now(IST))
        self.log = log
        self.master_lines = master_lines
        self.symbols = symbols
        self.staging = self.archive / STAGING_DIR
        self._stop = threading.Event()

    # --- archive state ---

    def archived(self) -> dict[str, str]:
        """date -> "bootstrap" or "live" for every archived day."""
        out = {}
        for day in available_days(self.archive):
            try:
                manifest = json.loads((self.archive / day / MANIFEST_FILE).read_text())
            except (OSError, ValueError):
                manifest = {}
            out[day] = "bootstrap" if manifest.get("source") == SOURCE else "live"
        return out

    def calendar(self) -> list[str]:
        """Completed trading sessions before today, from Dhan's NIFTY daily candles."""
        today = self.clock().date()
        nifty = Instrument("index", *NIFTY[:1], "NIFTY", *NIFTY[1:])
        body = self.client.daily(nifty, today - timedelta(days=CALENDAR_PROBE_DAYS), today)
        return sorted(d for d in daily_reference(body) if d < today.isoformat())

    def targets(self, calendar: list[str]) -> tuple[list[str], list[str]]:
        """(the wanted sessions, those of them not archived yet)."""
        wanted = calendar[-self.sessions :]
        archived = self.archived()
        # A day the bootstrap wrote whose files no longer match their digests (a crash, a bad disk) is rebuilt.
        missing = [d for d in wanted if d not in archived
                   or (archived[d] == "bootstrap" and not digests_match(self.archive / d))]  # fmt: skip
        return wanted, missing

    # --- planning without API calls ---

    def plan(self, rate: float = DEFAULT_RATE) -> dict:
        today = self.clock().date()
        wanted = weekday_sessions(today, self.sessions)
        archived = self.archived()
        missing = [d for d in wanted if d not in archived]
        equities = len(self.symbols or universe_symbols())
        from index_layer import INDEX_SPECS

        existing = sum(f.stat().st_size for d in archived for f in (self.archive / d).glob("*") if f.is_file())
        return {
            "note": "Offline estimate: the run takes the real calendar (holidays) from Dhan, so it may target "
            "slightly earlier dates; the request count is an upper bound.",
            "archive": str(self.archive),
            "already_archived": len(archived),
            "target_sessions": self.sessions,
            "to_download": estimate(missing, equities, len(INDEX_SPECS), rate, existing),
            "market_hours_guard": market_hours_guard(self.clock()),
        }

    # --- running ---

    def _resumable(self, missing: list[str]) -> list[str] | None:
        """The sessions of an interrupted batch that are all still missing: resume it before starting another."""
        if not self.staging.exists():
            return None
        for folder in sorted(self.staging.iterdir()):
            plan_path = BatchPaths(folder).plan
            if plan_path.exists():
                sessions = json.loads(plan_path.read_text()).get("sessions", [])
                if sessions and set(sessions) <= set(missing):
                    return sessions
        return None

    def _batch(self, missing: list[str]) -> BatchPaths:
        return BatchPaths(self.staging / f"{missing[0]}_{missing[-1]}")

    def _load_or_create_plan(self, paths: BatchPaths, missing: list[str]) -> tuple[list[Instrument], dict]:
        if paths.plan.exists():
            plan = json.loads(paths.plan.read_text())
            if plan["sessions"] == missing:
                return [Instrument(**i) for i in plan["instruments"]], plan
        instruments, unresolved = resolve_instruments(self.symbols or universe_symbols(), self.master_lines)
        if unresolved:
            self.log(f"warning: {len(unresolved)} instruments not in Dhan's master and skipped: {unresolved[:10]}")
        plan = {"tool_version": TOOL_VERSION, "sessions": missing, "created_at": self.clock().isoformat(),
                "instruments": [asdict(i) for i in instruments], "unresolved": unresolved}  # fmt: skip
        _atomic_write_bytes(paths.plan, json.dumps(plan, indent=1).encode())
        return instruments, plan

    def _download(self, inst: Instrument, paths: BatchPaths, missing: list[str]) -> str:
        if self._stop.is_set():
            return "stopped"
        stage = paths.stage(inst)
        if not stage_complete(stage):
            rows, rejected, made = [], Counter(), 0
            first, last = missing[0], missing[-1]
            for start, stop in _spans(first, last):
                body = self.client.intraday(inst, datetime(start.year, start.month, start.day, 9, 15, tzinfo=IST),
                                            datetime(stop.year, stop.month, stop.day, 15, 30, tzinfo=IST))  # fmt: skip
                made += 1
                part, why = parse_candles(body)
                rows += part
                rejected += why
            wanted = set(missing)
            kept = [r for r in rows if datetime.fromtimestamp(r[0], IST).strftime("%Y-%m-%d") in wanted]
            rejected["other_session"] += len(rows) - len(kept)
            _write_stage(stage, kept, +rejected, made)
        if inst.kind == "equity" and not paths.daily(inst).exists():
            start = date.fromisoformat(missing[0]) - timedelta(days=REFERENCE_PAD_DAYS)
            reference = daily_reference(self.client.daily(inst, start, date.fromisoformat(missing[-1])))
            _atomic_write_bytes(paths.daily(inst), json.dumps(reference, sort_keys=True).encode())
        return "done"

    def run(self, allow_market_hours: bool = False, keep_staging: bool = False) -> dict:
        reason = market_hours_guard(self.clock())
        if reason and not allow_market_hours:
            raise BootstrapBlocked(reason)
        calendar = self.calendar()
        wanted, missing = self.targets(calendar)
        if not missing:
            self.log(f"nothing to download: the last {len(wanted)} sessions are archived")
            return {"downloaded_sessions": [], "wanted": wanted, "verified": self.verify(wanted)}
        missing = self._resumable(missing) or missing
        paths = self._batch(missing)
        instruments, plan = self._load_or_create_plan(paths, missing)
        todo = [i for i in instruments if not (stage_complete(paths.stage(i))
                                                and (i.kind == "index" or paths.daily(i).exists()))]  # fmt: skip
        self.log(f"batch {paths.root.name}: {len(missing)} sessions, {len(instruments)} instruments, "
                 f"{len(instruments) - len(todo)} already staged, {len(todo)} to download")  # fmt: skip
        failures: dict[str, str] = {}
        done = processed = 0
        with ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix="history") as pool:
            futures = {pool.submit(self._download, inst, paths, missing): inst for inst in todo}
            for future in as_completed(futures):
                inst = futures[future]
                try:
                    future.result()
                    done += 1
                except BootstrapBlocked:
                    self._stop.set()
                    for f in futures:
                        f.cancel()
                    self._write_progress(paths, failures, blocked=True)
                    raise
                except Exception as exc:  # one instrument's failure never stops the others
                    failures[inst.key] = f"{type(exc).__name__}: {exc}"[:300]
                processed += 1
                if processed % 50 == 0:
                    self.log(f"  {done}/{len(todo)} staged, {len(failures)} failed, "
                             f"{self.client.requests} requests")  # fmt: skip
                    self._write_progress(paths, failures)
                    reason = market_hours_guard(self.clock())
                    if reason and not allow_market_hours:
                        self._stop.set()
        self._write_progress(paths, failures)
        if self._stop.is_set():
            raise BootstrapBlocked("stopped before the market opened; rerun later to resume")
        if failures:
            self.log(f"{len(failures)} instruments failed; rerun to retry them. Sessions are assembled only "
                     "when every resolved instrument is staged.")  # fmt: skip
            return {"downloaded_sessions": [], "failures": failures, "batch": paths.root.name}
        written = self.assemble(paths, instruments, missing, plan)
        report = self.verify(written)
        if all(r["ok"] for r in report.values()) and not keep_staging:
            shutil.rmtree(paths.root, ignore_errors=True)
        return {"downloaded_sessions": written, "verified": report, "requests": self.client.requests}

    def _write_progress(self, paths: BatchPaths, failures: dict, blocked: bool = False) -> None:
        _atomic_write_bytes(paths.progress, json.dumps({
            "updated_at": self.clock().isoformat(), "requests": self.client.requests if self.client else 0,
            "failures": failures, "blocked": blocked,
        }, indent=1).encode())  # fmt: skip

    # --- assembling day files ---

    def assemble(self, paths: BatchPaths, instruments: list[Instrument], missing: list[str], plan: dict) -> list[str]:
        """Write each missing session's directory from the staged files, one instrument at a time."""
        tmp_root = paths.root / "assembling"
        shutil.rmtree(tmp_root, ignore_errors=True)
        writers, counts, rejected = {}, Counter(), Counter()
        present: dict[str, set] = {d: set() for d in missing}
        stack = ExitStack()

        def writer(day: str, name: str, columns):
            key = (day, name)
            if key not in writers:
                path = tmp_root / day / name
                path.parent.mkdir(parents=True, exist_ok=True)
                text = stack.enter_context(gzip.open(path, "wt", compresslevel=6, encoding="utf-8", newline=""))  # noqa: SIM115 - closed by the ExitStack
                w = csv.writer(text, lineterminator="\n")
                w.writerow(columns)
                writers[key] = w
            return writers[key]

        with stack:
            for inst in sorted(instruments, key=lambda i: (i.kind != "equity", i.key)):
                meta = json.loads(paths.stage(inst).with_suffix(".json").read_text())
                rejected.update(meta.get("rejected", {}))
                name, columns = (EQUITY_FILE, EQUITY_COLUMNS) if inst.kind == "equity" else (INDEX_FILE, INDEX_COLUMNS)
                first_column = inst.symbol if inst.kind == "equity" else inst.key
                second_column = inst.security_id if inst.kind == "equity" else inst.symbol
                for e, o, h, low, c, v in _read_stage(paths.stage(inst)):
                    day = datetime.fromtimestamp(e, IST).strftime("%Y-%m-%d")
                    values = (_price(o), _price(h), _price(low), _price(c), v)
                    writer(day, name, columns).writerow((first_column, second_column, _ist_timestamp(e), *values))
                    counts[(day, name)] += 1
                    if inst.kind == "equity":
                        present[day].add(inst.symbol)
        references = self._references(paths, instruments, missing, present)
        for path in tmp_root.rglob("*.csv.gz"):  # durable before the directory is moved into the archive
            with open(path, "rb") as handle:
                os.fsync(handle.fileno())
        equities = sorted(i.symbol for i in instruments if i.kind == "equity")
        written = []
        for day in missing:
            folder = tmp_root / day
            if not counts[(day, EQUITY_FILE)]:
                self.log(f"  {day}: no equity rows returned; not archived")
                continue
            _atomic_write_bytes(folder / EQUITY_REFERENCE_FILE, references[day])
            files = {}
            for name in (EQUITY_FILE, INDEX_FILE, EQUITY_REFERENCE_FILE):
                if (folder / name).exists():
                    rows = counts[(day, name)] if name != EQUITY_REFERENCE_FILE else len(present[day])
                    files[name] = {"rows": rows, "sha256": sha256_file(folder / name),
                                   "written_at": self.clock().isoformat()}  # fmt: skip
            manifest = {
                "session_date": day, "timezone": "Asia/Kolkata", "synthetic_candles": False, "source": SOURCE,
                "files": files,
                "bootstrap": {"tool_version": TOOL_VERSION, "batch": paths.root.name, "created_at": plan["created_at"],
                              "expected_equities": len(equities), "equities_with_data": len(present[day]),
                              "missing_equities": sorted(set(equities) - present[day]),
                              "unresolved_instruments": plan.get("unresolved", []),
                              "excluded_rows_in_batch": dict(rejected)},
            }  # fmt: skip
            _atomic_write_bytes(folder / MANIFEST_FILE, json.dumps(manifest, indent=2, sort_keys=True).encode())
            report = verify_day(folder, equities)
            if not report["ok"]:
                self.log(f"  {day}: failed verification, not archived: {report['problems']}")
                continue
            target = self.archive / day
            if target.exists():
                if self.archived().get(day) != "bootstrap":
                    self.log(f"  {day}: the live archive wrote this day meanwhile; keeping it")
                    continue
                shutil.rmtree(target)
            os.replace(folder, target)
            written.append(day)
            self.log(f"  {day}: archived {counts[(day, EQUITY_FILE)]} equity rows, "
                     f"{len(present[day])}/{len(equities)} equities")  # fmt: skip
        shutil.rmtree(tmp_root, ignore_errors=True)
        return written

    def _references(self, paths, instruments, missing, present) -> dict[str, bytes]:
        """Per session: each equity's previous close (prior session's daily close) and open, from Dhan's daily candles."""
        rows: dict[str, list[tuple]] = {d: [] for d in missing}
        for inst in sorted((i for i in instruments if i.kind == "equity"), key=lambda i: i.symbol):
            daily = json.loads(paths.daily(inst).read_text()) if paths.daily(inst).exists() else {}
            dates = sorted(daily)
            for day in missing:
                if inst.symbol not in present[day]:
                    continue
                earlier = [d for d in dates if d < day]
                previous_close = daily[earlier[-1]]["close"] if earlier else None
                today_open = daily.get(day, {}).get("open")
                rows[day].append((inst.symbol, inst.security_id, _price(previous_close), _price(today_open)))
        out = {}
        for day, items in rows.items():
            text = csv_text(EQUITY_REFERENCE_COLUMNS, items)
            out[day] = gzip.compress(text.encode(), compresslevel=6, mtime=0)
        return out

    # --- status and verification ---

    def verify(self, days: list[str] | None = None) -> dict[str, dict]:
        symbols = self.symbols or universe_symbols()
        days = days if days is not None else available_days(self.archive)
        return {d: verify_day(self.archive / d, symbols) for d in days if (self.archive / d).exists()}

    def status(self) -> dict:
        archived = self.archived()
        batches = []
        if self.staging.exists():
            for folder in sorted(p for p in self.staging.iterdir() if p.is_dir()):
                paths = BatchPaths(folder)
                plan = json.loads(paths.plan.read_text()) if paths.plan.exists() else {}
                instruments = [Instrument(**i) for i in plan.get("instruments", [])]
                staged = sum(stage_complete(paths.stage(i)) for i in instruments)
                progress = json.loads(paths.progress.read_text()) if paths.progress.exists() else {}
                batches.append({"batch": folder.name, "sessions": len(plan.get("sessions", [])),
                                "instruments": len(instruments), "staged": staged,
                                "failures": len(progress.get("failures", {})), "blocked": progress.get("blocked"),
                                "updated_at": progress.get("updated_at")})  # fmt: skip
        return {
            "archive": str(self.archive),
            "sessions_archived": len(archived),
            "bootstrap_sessions": sum(v == "bootstrap" for v in archived.values()),
            "live_sessions": sum(v == "live" for v in archived.values()),
            "first_session": min(archived) if archived else None,
            "last_session": max(archived) if archived else None,
            "pending_batches": batches,
        }


def csv_text(columns, rows) -> str:
    import io

    buffer = io.StringIO()
    w = csv.writer(buffer, lineterminator="\n")
    w.writerow(columns)
    w.writerows(rows)
    return buffer.getvalue()
