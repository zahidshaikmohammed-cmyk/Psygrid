from __future__ import annotations

import csv
import io
import threading
import time
from dataclasses import dataclass
from datetime import datetime, time as dt_time
from typing import Optional
from zoneinfo import ZoneInfo

import requests
from dhanhq import DhanContext, MarketFeed

from output import _clean_candle, _ist_timestamp, _price

MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
INDEX_SEGMENT = "IDX_I"
INDEX_INSTRUMENT = "INDEX"
INDEX_FALLBACK_IDS = {
    "nifty": "13",
    "banknifty": "25",
    "sensex": "51",
    "nifty500": "17",
    "finnifty": "27",
    "indiavix": "26",
    "niftyit": "29",
}

INDEX_SPECS = {
    "nifty": ("NIFTY", ("NIFTY", "NIFTY 50")),
    "banknifty": ("BANKNIFTY", ("BANKNIFTY", "NIFTY BANK", "NIFTY BANK 50")),
    "sensex": ("SENSEX", ("SENSEX", "S&P BSE SENSEX")),
    "nifty500": ("NIFTY_500", ("NIFTY 500", "NIFTY500", "NIFTY_500")),
    "niftymidcap100": ("NIFTY_MIDCAP_100", ("NIFTY MIDCAP 100", "NIFTY_MIDCAP_100", "NIFTYMIDCAP100")),
    "niftysmallcap100": ("NIFTY_SMALLCAP_100", ("NIFTY SMALLCAP 100", "NIFTY_SMALLCAP_100", "NIFTYSMALLCAP100")),
    "finnifty": ("NIFTY_FIN_SERVICE", ("NIFTY FIN SERVICE", "NIFTY FINANCIAL SERVICES", "NIFTY_FIN_SERVICE", "NIFTYFINSERVICE")),
    "indiavix": ("INDIA VIX", ("INDIA VIX", "INDIAVIX")),
    "niftyit": ("NIFTY_IT", ("NIFTY IT", "NIFTY_IT", "NIFTYIT")),
    "niftyauto": ("NIFTY_AUTO", ("NIFTY AUTO", "NIFTY_AUTO", "NIFTYAUTO")),
    "niftypharma": ("NIFTY_PHARMA", ("NIFTY PHARMA", "NIFTY_PHARMA", "NIFTYPHARMA")),
    "niftymetal": ("NIFTY_METAL", ("NIFTY METAL", "NIFTY_METAL", "NIFTYMETAL")),
    "niftyfmcg": ("NIFTY_FMCG", ("NIFTY FMCG", "NIFTY_FMCG", "NIFTYFMCG")),
    "niftyrealty": ("NIFTY_REALTY", ("NIFTY REALTY", "NIFTY_REALTY", "NIFTYREALTY")),
    "niftyenergy": ("NIFTY_ENERGY", ("NIFTY ENERGY", "NIFTY_ENERGY", "NIFTYENERGY")),
    "niftyinfra": ("NIFTY_INFRA", ("NIFTY INFRA", "NIFTY_INFRA", "NIFTYINFRA")),
}

def _norm(value: object) -> str:
    return "".join(ch for ch in str(value or "").upper() if ch.isalnum())

def _resolve_one(key: str) -> tuple["IndexInstrument | None", str]:
    _symbol, aliases = INDEX_SPECS[key]
    fallback = INDEX_FALLBACK_IDS.get(key)
    if fallback:
        return IndexInstrument(fallback, "IDX_I", INDEX_INSTRUMENT), ""

    try:
        response = requests.get(MASTER_URL, timeout=30)
        response.raise_for_status()
        rows = csv.DictReader(io.StringIO(response.text))
        wanted = {_norm(v) for v in aliases}
        matches: dict[tuple[str, str, str], IndexInstrument] = {}
        for row in rows:
            if str(row.get("SEM_SEGMENT", "")).strip().upper() != "I":
                continue
            if str(row.get("SEM_INSTRUMENT_NAME", "")).strip().upper() != INDEX_INSTRUMENT:
                continue
            security_id = str(row.get("SEM_SMST_SECURITY_ID", "")).strip()
            if not security_id:
                continue
            values = (
                row.get("SEM_TRADING_SYMBOL", ""),
                row.get("SEM_CUSTOM_SYMBOL", ""),
                row.get("SM_SYMBOL_NAME", ""),
            )
            if {_norm(v) for v in values} & wanted:
                exchange = str(row.get("SEM_EXM_EXCH_ID", "")).strip().upper()
                matches[(security_id, exchange, INDEX_INSTRUMENT)] = IndexInstrument(
                    security_id, exchange or "NSE", INDEX_INSTRUMENT
                )
        if len(matches) == 1:
            return next(iter(matches.values())), ""
        if not matches:
            return None, "not found in Dhan instrument master"
        return None, f"ambiguous instrument master matches: {sorted(matches)}"
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _resolve_all() -> tuple[dict[str, "IndexInstrument"], dict[str, str]]:
    resolved: dict[str, IndexInstrument] = {}
    errors: dict[str, str] = {}
    for key in INDEX_SPECS:
        instrument, error = _resolve_one(key)
        if instrument is not None:
            resolved[key] = instrument
        else:
            errors[key] = error
    return resolved, errors

