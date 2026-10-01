"""An offline stand-in for Dhan's historical APIs and instrument master, so CI never calls Dhan.

Deterministic: the same security and minute always give the same candle. The
calendar has weekend gaps and two holidays, and the fake injects the faults the
bootstrap must handle: a 429, a 500, a "no data" instrument, an invalid row, a
duplicate minute, a pre-open bar and a non-numeric value.
"""

import zlib
from datetime import date, datetime, timedelta

import numpy as np

from intelligence.archive import IST

HOLIDAYS = {"2026-09-15", "2026-10-02"}
SYMBOLS = {"RELIANCE": "2885", "TCS": "11536", "INFY": "1594", "HDFCBANK": "1333", "SUNPHARMA": "3351",
           "DELISTED": "99999"}  # fmt: skip
NO_DATA = {"99999"}
FAULTY = "2885"  # RELIANCE: an invalid row, a duplicate minute and a non-numeric value on its first day


def master_lines(extra_index_rows=True):
    header = "SEM_EXM_EXCH_ID,SEM_SEGMENT,SEM_SMST_SECURITY_ID,SEM_INSTRUMENT_NAME,SEM_TRADING_SYMBOL,SEM_CUSTOM_SYMBOL,SM_SYMBOL_NAME"
    lines = [header]
    for symbol, sid in SYMBOLS.items():
        lines.append(f"NSE,E,{sid},EQUITY,{symbol},{symbol},{symbol}")
    lines.append("BSE,E,500325,EQUITY,RELIANCE,RELIANCE,RELIANCE")  # another exchange: ignored
    if extra_index_rows:
        for i, name in enumerate(("NIFTY MIDCAP 100", "NIFTY SMALLCAP 100", "NIFTY AUTO", "NIFTY PHARMA",
                                  "NIFTY METAL", "NIFTY FMCG", "NIFTY REALTY", "NIFTY ENERGY", "NIFTY INFRA")):  # fmt: skip
            lines.append(f"NSE,I,{500 + i},INDEX,{name},{name},{name}")
    return lines


def trading_days(start: date, end: date):
    day = start
    while day <= end:
        if day.weekday() < 5 and day.isoformat() not in HOLIDAYS:
            yield day
        day += timedelta(days=1)


def candles(security_id: str, day: date, with_faults: bool):
    rng = np.random.default_rng(zlib.crc32(f"{security_id}:{day}".encode()))
    base = 100 + zlib.crc32(security_id.encode()) % 2000
    rows = []
    open_ = datetime(day.year, day.month, day.day, 9, 10, tzinfo=IST)  # a pre-open bar first
    price = float(base)
    for m in range(376):
        stamp = int((open_ + timedelta(minutes=m if m == 0 else m + 4)).timestamp())
        o = round(price, 2)
        c = round(max(1.0, o * (1 + rng.normal(0, 0.001))), 2)
        h, low = round(max(o, c) * 1.0005, 2), round(min(o, c) * 0.9995, 2)
        rows.append([stamp, o, h, low, c, int(rng.integers(100, 10_000)) if security_id[:1] != "I" else 0])
        price = c
    if with_faults:
        rows[11][2] = rows[11][3] - 1  # high below low
        rows.append(list(rows[21]))  # the same minute twice
        rows[31][5] = "n/a"
    return rows


class FakeDhan:
    def __init__(self, fail_once=("429:1594", "500:1333"), auth_fail=False, stop_after=None, data_plan="Active"):
        self.data_plan = data_plan
        self.calls = []
        self.pending_faults = set(fail_once)
        self.auth_fail = auth_fail
        self.stop_after = stop_after

    def profile(self, url, headers):
        return 200, {"dhanClientId": "1", "dataPlan": self.data_plan, "dataValidity": "2027-01-01 00:00:00.0"}

    def __call__(self, url, headers, payload):
        self.calls.append((url.rsplit("/", 1)[-1], payload.get("securityId")))
        if self.stop_after is not None and len(self.calls) > self.stop_after:
            raise KeyboardInterrupt("simulated crash")
        if self.auth_fail:
            return 401, {}, {"errorCode": "DH-901", "errorMessage": "Invalid token"}
        sid = payload["securityId"]
        for fault in list(self.pending_faults):
            status, fault_sid = fault.split(":")
            if fault_sid == sid:
                self.pending_faults.discard(fault)
                return int(status), {"Retry-After": "0"}, {"errorMessage": "fault"}
        if sid in NO_DATA:
            return 400, {}, {"errorCode": "DH-907", "errorMessage": "No data present"}
        if url.endswith("/charts/historical"):
            start, end = date.fromisoformat(payload["fromDate"]), date.fromisoformat(payload["toDate"])
            days = list(trading_days(start, end - timedelta(days=0)))
            out = {k: [] for k in ("timestamp", "open", "high", "low", "close", "volume")}
            for day in days:
                rows = candles(sid, day, False)
                stamp = int(datetime(day.year, day.month, day.day, tzinfo=IST).timestamp())
                for key, value in zip(out, (stamp, rows[1][1], max(r[2] for r in rows), min(r[3] for r in rows),
                                            rows[-1][4], sum(r[5] for r in rows)), strict=True):  # fmt: skip
                    out[key].append(value)
            return 200, {}, out
        start = datetime.strptime(payload["fromDate"], "%Y-%m-%d %H:%M:%S").date()
        end = datetime.strptime(payload["toDate"], "%Y-%m-%d %H:%M:%S").date()
        out = {k: [] for k in ("timestamp", "open", "high", "low", "close", "volume")}
        days = list(trading_days(start, end))
        for i, day in enumerate(days):
            for row in candles(sid, day, sid == FAULTY and i == 0):
                for key, value in zip(out, row, strict=True):
                    out[key].append(value)
        return 200, {}, out
