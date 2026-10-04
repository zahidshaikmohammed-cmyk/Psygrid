"""Process-level guards that keep the live PSYGRID service healthy.

* ``close_market_feed`` releases everything a dhanhq ``MarketFeed`` owns. Its constructor creates a
  private asyncio event loop that ``close_connection()`` never closes; under uvloop each abandoned loop
  keeps its epoll/eventfd/pipe descriptors until the process exits, so every reconnect leaked a handful
  of file descriptors until the 1024 soft limit was reached and the HTTP server could no longer accept.
* ``is_trading_day`` keeps the session loops from connecting to Dhan on weekends and listed holidays.
* ``raise_nofile_limit`` lifts the soft descriptor limit to the hard limit at startup.
* ``ServiceWatchdog`` pings the systemd watchdog only while the process can still answer HTTP on its
  own port and is within its descriptor and memory budget. It never restarts anything because data is
  stale or Dhan is down: that is reported by ``/health`` and the feed reconnects by itself.
"""

from __future__ import annotations

import asyncio
import contextlib
import http.client
import os
import socket
import threading
import time
from datetime import date

try:
    import resource
except ImportError:  # pragma: no cover - non-POSIX
    resource = None


def close_market_feed(feed) -> None:
    """Disconnect a dhanhq MarketFeed and close the event loop it created. Never raises."""
    if feed is None:
        return
    with contextlib.suppress(Exception):
        feed.close_connection()
    loop = getattr(feed, "loop", None)
    if loop is None:
        return
    try:
        if loop.is_running() or loop.is_closed():
            return
        pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.run_until_complete(loop.shutdown_asyncgens())
    except Exception:
        pass
    finally:
        with contextlib.suppress(Exception):
            if not loop.is_running():
                loop.close()
        with contextlib.suppress(Exception):
            asyncio.set_event_loop(None)


def _date_set(raw: str) -> frozenset[str]:
    return frozenset(item.strip() for item in raw.replace(";", ",").split(",") if item.strip())


def is_trading_day(day: date, environ=None) -> bool:
    """Monday to Friday, minus ``PSYGRID_MARKET_HOLIDAYS``, plus ``PSYGRID_SPECIAL_SESSIONS`` (ISO dates)."""
    environ = os.environ if environ is None else environ
    iso = day.isoformat()
    if iso in _date_set(environ.get("PSYGRID_SPECIAL_SESSIONS", "")):
        return True
    if iso in _date_set(environ.get("PSYGRID_MARKET_HOLIDAYS", "")):
        return False
    return day.weekday() < 5


def raise_nofile_limit(target: int = 65536) -> tuple[int, int] | None:
    """Raise the soft RLIMIT_NOFILE towards ``target`` (bounded by the hard limit). Returns (soft, hard)."""
    if resource is None:
        return None
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        wanted = target if hard == resource.RLIM_INFINITY else min(target, hard)
        if soft != resource.RLIM_INFINITY and soft < wanted:
            resource.setrlimit(resource.RLIMIT_NOFILE, (wanted, hard))
        return resource.getrlimit(resource.RLIMIT_NOFILE)
    except (OSError, ValueError):
        return None


def nofile_limit() -> int | None:
    if resource is None:
        return None
    try:
        soft = resource.getrlimit(resource.RLIMIT_NOFILE)[0]
    except (OSError, ValueError):
        return None
    return None if soft == resource.RLIM_INFINITY else int(soft)


def open_fd_count() -> int | None:
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        return None


def rss_bytes() -> int | None:
    try:
        with open("/proc/self/statm", encoding="ascii") as handle:
            pages = int(handle.read().split()[1])
        return pages * os.sysconf("SC_PAGE_SIZE")
    except (OSError, ValueError, IndexError):
        return None


def process_stats() -> dict:
    fds = open_fd_count()
    limit = nofile_limit()
    rss = rss_bytes()
    return {
        "pid": os.getpid(),
        "open_fds": fds,
        "fd_limit": limit,
        "fd_usage_ratio": round(fds / limit, 4) if fds is not None and limit else None,
        "threads": threading.active_count(),
        "rss_mb": round(rss / 1048576, 1) if rss is not None else None,
    }


def sd_notify(message: str, environ=None) -> bool:
    """Send one sd_notify datagram. A no-op (False) when not started by systemd with NOTIFY_SOCKET."""
    environ = os.environ if environ is None else environ
    address = environ.get("NOTIFY_SOCKET", "")
    if not address:
        return False
    if address.startswith("@"):
        address = "\0" + address[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as sock:
            sock.connect(address)
            sock.sendall(message.encode("utf-8"))
        return True
    except OSError:
        return False


def http_probe(port: int, path: str = "/health", timeout: float = 5.0) -> bool:
    """True when the local HTTP server answers ``path`` with a 2xx response in time."""
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        connection.request("GET", path, headers={"User-Agent": "psygrid-watchdog"})
        response = connection.getresponse()
        response.read()
        return 200 <= response.status < 300
    except (OSError, http.client.HTTPException):
        return False
    finally:
        connection.close()


class ServiceWatchdog:
    """Keep the systemd watchdog fed while the process is genuinely serving.

    Every ``interval`` seconds it checks that the local HTTP server answers, that open descriptors stay
    below ``max_fd_ratio`` of the limit and that RSS stays below ``max_rss_mb``. Only a passing check
    sends ``WATCHDOG=1``; when checks keep failing for ``WatchdogSec`` systemd restarts the service.
    During the startup grace period it pings unconditionally so a slow start is never killed.
    """

    def __init__(
        self,
        port: int,
        *,
        interval: float = 15.0,
        startup_grace: float = 180.0,
        max_fd_ratio: float = 0.85,
        max_rss_mb: float = 2048.0,
        probe=None,
        notify=None,
        stats=None,
    ) -> None:
        self.port = port
        self.interval = interval
        self.startup_grace = startup_grace
        self.max_fd_ratio = max_fd_ratio
        self.max_rss_mb = max_rss_mb
        self._probe = probe or (lambda: http_probe(self.port))
        self._notify = notify or sd_notify
        self._stats = stats or process_stats
        self._started = time.monotonic()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.last_check: dict = {"ok": None, "reasons": [], "at_monotonic": None}
        self.consecutive_failures = 0

    @classmethod
    def from_environment(cls, port: int) -> ServiceWatchdog:
        def number(name: str, default: float) -> float:
            try:
                return float(os.getenv(name, "") or default)
            except ValueError:
                return default

        return cls(
            port,
            interval=number("PSYGRID_WATCHDOG_INTERVAL_SECONDS", 15.0),
            startup_grace=number("PSYGRID_WATCHDOG_STARTUP_GRACE_SECONDS", 180.0),
            max_fd_ratio=number("PSYGRID_WATCHDOG_MAX_FD_RATIO", 0.85),
            max_rss_mb=number("PSYGRID_WATCHDOG_MAX_RSS_MB", 2048.0),
        )

    def check(self) -> tuple[bool, list[str]]:
        reasons: list[str] = []
        stats = self._stats()
        ratio = stats.get("fd_usage_ratio")
        if ratio is not None and ratio >= self.max_fd_ratio:
            reasons.append(f"open_fds {stats.get('open_fds')} of {stats.get('fd_limit')}")
        rss = stats.get("rss_mb")
        if rss is not None and rss >= self.max_rss_mb:
            reasons.append(f"rss {rss}MB >= {self.max_rss_mb}MB")
        if not self._probe():
            reasons.append(f"http 127.0.0.1:{self.port}/health not answering")
        return not reasons, reasons

    def tick(self) -> bool:
        """Run one check and ping systemd if it passes (or while in the startup grace period)."""
        ok, reasons = self.check()
        self.consecutive_failures = 0 if ok else self.consecutive_failures + 1
        self.last_check = {"ok": ok, "reasons": reasons, "at_monotonic": round(time.monotonic(), 3)}
        in_grace = time.monotonic() - self._started < self.startup_grace
        if ok or in_grace:
            self._notify("WATCHDOG=1")
        return ok

    def _run(self) -> None:
        while not self._stop.wait(self.interval):
            with contextlib.suppress(Exception):
                self.tick()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._started = time.monotonic()
        self._notify("WATCHDOG=1")
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="psygrid-service-watchdog")
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread is not threading.current_thread():
            self._thread.join(timeout=2)
        self._thread = None
