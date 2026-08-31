from __future__ import annotations

import csv
import io
from typing import Dict, Iterable

import requests


DHAN_INSTRUMENT_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"


def fetch_nse_equity_security_ids(symbols: Iterable[str], timeout: int = 30) -> Dict[str, str]:
    """Resolve NSE equity symbols against Dhan's current official instrument master.

    The master is fetched into RAM only. Nothing is written to disk or persisted.
    """
    wanted = {str(s).strip().upper() for s in symbols if str(s).strip()}
    if not wanted:
        return {}

    response = requests.get(DHAN_INSTRUMENT_MASTER_URL, timeout=timeout)
    response.raise_for_status()
    response.encoding = response.encoding or "utf-8"

    reader = csv.DictReader(io.StringIO(response.text))
    result: Dict[str, str] = {}

    for row in reader:
        if row.get("SEM_EXM_EXCH_ID", "").strip().upper() != "NSE":
            continue
        if row.get("SEM_INSTRUMENT_NAME", "").strip().upper() != "EQUITY":
            continue

        symbol = row.get("SEM_TRADING_SYMBOL", "").strip().upper()
        security_id = row.get("SEM_SMST_SECURITY_ID", "").strip()
        if symbol in wanted and security_id:
            result[symbol] = security_id

    missing = sorted(wanted - set(result))
    if missing:
        raise RuntimeError(
            "Dhan instrument master could not resolve these NSE_EQ symbols: "
            + ", ".join(missing)
        )

    if len(result) != len(wanted):
        raise RuntimeError("Dhan security-ID resolution did not produce a one-to-one universe")

    return result
