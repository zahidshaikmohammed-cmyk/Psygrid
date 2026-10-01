"""Profile the machine and the running Psygrid service, read-only.

Run on the production VM, ideally during a market session:

    python tools/profile_host.py --seconds 120

It reports the host's shape (CPUs, memory, disk), the psygrid process's CPU and
memory use, how long /public/live.json takes to serve, and how often the
equity indicator runtime completes a sync (and how far it lags). It only reads
/proc and makes a few GET requests, so it is safe beside the live feeds.
Standard library only.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
CLOCK_TICKS = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100


def parse_meminfo(text: str) -> dict[str, int]:
    """Return /proc/meminfo values in bytes, keyed by field name."""
    values = {}
    for line in text.splitlines():
        name, _, rest = line.partition(":")
        parts = rest.split()
        if parts and parts[0].isdigit():
            values[name.strip()] = int(parts[0]) * (1024 if parts[1:2] == ["kB"] else 1)
    return values


def ist_lag_seconds(stamp: str | None, now: datetime) -> float | None:
    """Seconds between an 'YYYY-MM-DD HH:MM:SS IST' stamp and now; None if absent or malformed."""
    if not stamp:
        return None
    try:
        parsed = datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S IST").replace(tzinfo=IST)
    except ValueError:
        return None
    return round((now - parsed).total_seconds(), 1)


def host_shape(archive_dir: Path) -> dict:
    meminfo = parse_meminfo(Path("/proc/meminfo").read_text()) if Path("/proc/meminfo").exists() else {}
    disk = shutil.disk_usage(archive_dir if archive_dir.exists() else Path.home())
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "load_average": os.getloadavg() if hasattr(os, "getloadavg") else None,
        "memory_total_gb": round(meminfo.get("MemTotal", 0) / 1e9, 2),
        "memory_available_gb": round(meminfo.get("MemAvailable", 0) / 1e9, 2),
        "disk_total_gb": round(disk.total / 1e9, 1),
        "disk_free_gb": round(disk.free / 1e9, 1),
        "archive_dir": str(archive_dir),
        "archive_days": len([p for p in archive_dir.iterdir() if p.is_dir()]) if archive_dir.exists() else 0,
    }


def psygrid_pid() -> int | None:
    try:
        out = subprocess.run(
            ["systemctl", "show", "-p", "MainPID", "--value", "psygrid"], capture_output=True, text=True, timeout=5
        )
        pid = int(out.stdout.strip() or 0)
        return pid or None
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


def process_sample(pid: int) -> tuple[float, dict]:
    """CPU seconds used so far, plus memory and thread count, for one process."""
    fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
    cpu_seconds = (int(fields[11]) + int(fields[12])) / CLOCK_TICKS  # utime + stime
    status = parse_meminfo(Path(f"/proc/{pid}/status").read_text())
    return cpu_seconds, {"rss_mb": round(status.get("VmRSS", 0) / 1e6, 1), "threads": status.get("Threads")}


def timed_get(url: str) -> tuple[float, int, dict]:
    started = time.perf_counter()
    with urlopen(Request(url, headers={"Cache-Control": "no-cache"}), timeout=60) as response:
        body = response.read()
    return time.perf_counter() - started, len(body), json.loads(body)


def profile(base_url: str, seconds: int, interval: float) -> dict:
    pid = psygrid_pid()
    cpu_start = process_sample(pid)[0] if pid else None
    started = time.monotonic()
    live_times, live_bytes, lags, syncs = [], 0, [], []
    while time.monotonic() - started < seconds:
        elapsed, size, _ = timed_get(f"{base_url}/public/live.json")
        live_times.append(elapsed)
        live_bytes = size
        _, _, indicators = timed_get(f"{base_url}/public/indicators.json")
        syncs.append((time.monotonic(), indicators.get("sync_count")))
        lags.append(
            ist_lag_seconds((indicators.get("source") or {}).get("endpoint_current_time_ist"), datetime.now(IST))
        )
        time.sleep(interval)
    window = time.monotonic() - started
    measured = [lag for lag in lags if lag is not None]
    result = {
        "window_seconds": round(window, 1),
        "live_json_seconds": {"min": round(min(live_times), 3), "max": round(max(live_times), 3)},
        "live_json_megabytes": round(live_bytes / 1e6, 2),
        "indicator_lag_seconds": {"min": min(measured, default=None), "max": max(measured, default=None)},
    }
    counts = [count for _, count in syncs if isinstance(count, int)]
    if len(counts) >= 2:
        result["indicator_syncs_per_minute"] = round((counts[-1] - counts[0]) / (syncs[-1][0] - syncs[0][0]) * 60, 1)
    if pid:
        cpu_end, memory = process_sample(pid)
        result["process"] = {
            "pid": pid,
            "cpu_percent_of_one_core": round((cpu_end - cpu_start) / window * 100, 1),
            **memory,
        }
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base-url", default=os.getenv("PSYGRID_BASE_URL", "http://127.0.0.1:10000"))
    parser.add_argument("--seconds", type=int, default=120, help="how long to sample the service")
    parser.add_argument("--interval", type=float, default=10.0, help="seconds between samples")
    parser.add_argument(
        "--archive-dir", type=Path, default=Path(os.getenv("PSYGRID_ARCHIVE_DIR", Path.home() / "psygrid-data"))
    )
    args = parser.parse_args()
    report = {
        "measured_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
        "host": host_shape(args.archive_dir),
        "service": profile(args.base_url.rstrip("/"), args.seconds, args.interval),
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
