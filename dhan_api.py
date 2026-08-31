from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Dict, List
from zoneinfo import ZoneInfo

import requests

BASE_URL = "https://api.dhan.co/v2"


class DhanAPI:
    def __init__(self, settings):
        self.settings = settings
        self.session = requests.Session()
        self.tz = ZoneInfo(settings.timezone)

    @property
    def headers(self) -> dict:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "access-token": self.settings.access_token,
            "client-id": self.settings.client_id,
        }

    def _post(self, path: str, payload: dict, include_client_id: bool = False) -> dict:
        headers = self.headers if include_client_id else {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "access-token": self.settings.access_token,
        }
        last_error = None
        for attempt in range(5):
            try:
                response = self.session.post(BASE_URL + path, headers=headers, json=payload, timeout=30)
                if response.status_code == 429:
                    time.sleep(min(8.0, float(attempt + 1)))
                    continue
                response.raise_for_status()
                data = response.json()
                if isinstance(data, dict) and str(data.get("status", "")).lower() == "failure":
                    raise RuntimeError(str(data))
                return data
            except Exception as exc:
                last_error = exc
                if attempt < 4:
                    time.sleep(min(8.0, float(attempt + 1)))
        raise RuntimeError(f"Dhan API failed: {last_error}")

    def profile(self) -> dict:
        response = self.session.get(
            BASE_URL + "/profile",
            headers={"Accept": "application/json", "access-token": self.settings.access_token},
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, dict):
            raise RuntimeError("Invalid Dhan profile response")
        return data

    def verify_data_access(self) -> dict:
        profile = self.profile()
        if str(profile.get("dataPlan", "")).strip().lower() != "active":
            raise RuntimeError("DHAN_DATA_PLAN_NOT_ACTIVE")
        return profile

    def quote_snapshot(self, instruments) -> Dict[str, dict]:
        grouped: Dict[str, List[int]] = {}
        for item in instruments:
            grouped.setdefault(item.exchange_segment, []).append(int(item.security_id))
        raw = self._post("/marketfeed/quote", grouped, include_client_id=True)
        result: Dict[str, dict] = {}
        for _segment, rows in raw.get("data", {}).items():
            if not isinstance(rows, dict):
                continue
            for security_id, row in rows.items():
                if isinstance(row, dict):
                    result[str(security_id)] = row
        return result

    @staticmethod
    def _candles_from_arrays(data: dict) -> List[dict]:
        if not isinstance(data, dict):
            return []
        keys = ("timestamp", "open", "high", "low", "close", "volume")
        arrays = [data.get(key) for key in keys]
        if not all(isinstance(a, list) for a in arrays):
            return []
        length = len(arrays[0])
        if any(len(a) != length for a in arrays):
            raise RuntimeError("Dhan historical response arrays have inconsistent lengths")
        candles: List[dict] = []
        for i in range(length):
            try:
                timestamp = int(arrays[0][i])
                open_price = float(arrays[1][i])
                high = float(arrays[2][i])
                low = float(arrays[3][i])
                close = float(arrays[4][i])
                volume = int(arrays[5][i])
            except (TypeError, ValueError):
                continue
            if timestamp <= 0 or min(open_price, high, low, close) <= 0 or volume < 0:
                continue
            if high < max(open_price, close) or low > min(open_price, close) or low <= 0 or high <= 0:
                continue
            candles.append({
                "timestamp": timestamp,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "source": "DHAN_HISTORICAL_API",
                "complete": True,
            })
        return candles

    def intraday(self, item, interval: int, from_dt: datetime, to_dt: datetime) -> List[dict]:
        payload = {
            "securityId": item.security_id,
            "exchangeSegment": item.exchange_segment,
            "instrument": item.instrument,
            "interval": str(interval),
            "oi": False,
            "fromDate": from_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "toDate": to_dt.strftime("%Y-%m-%d %H:%M:%S"),
        }
        return self._candles_from_arrays(self._post("/charts/intraday", payload))

    def daily(self, item, from_date: datetime, to_date: datetime) -> List[dict]:
        payload = {
            "securityId": item.security_id,
            "exchangeSegment": item.exchange_segment,
            "instrument": item.instrument,
            "expiryCode": 0,
            "oi": False,
            "fromDate": from_date.strftime("%Y-%m-%d"),
            "toDate": to_date.strftime("%Y-%m-%d"),
        }
        return self._candles_from_arrays(self._post("/charts/historical", payload))

    def load_previous_daily(self, item, lookback: int) -> List[dict]:
        now = datetime.now(self.tz)
        warmup = max(
            self.settings.daily_indicator_warmup,
            self.settings.ma_period,
            self.settings.ema_period,
            self.settings.rsi_period + 1,
        )
        # Request enough calendar time to obtain warm-up trading sessions plus
        # the requested seven completed days. Output layer may retain the full
        # warm-up series for mathematically valid indicator values.
        calendar_days = max((lookback + warmup) * 2, 90)
        start = now - timedelta(days=calendar_days)
        rows = self.daily(item, start, now)
        today = now.date()
        rows = [r for r in rows if datetime.fromtimestamp(r["timestamp"], self.tz).date() < today]
        rows.sort(key=lambda r: r["timestamp"])
        return rows[-(lookback + warmup):]

    def load_previous_intraday(self, item, interval: int, days: int) -> List[dict]:
        now = datetime.now(self.tz)
        start = now - timedelta(days=max(days, 1))
        rows = self.intraday(item, interval, start, now)
        today = now.date()
        rows = [r for r in rows if datetime.fromtimestamp(r["timestamp"], self.tz).date() < today]
        rows.sort(key=lambda r: r["timestamp"])
        return rows

    def load_today_1m(self, item) -> List[dict]:
        now = datetime.now(self.tz)
        start = now.replace(hour=9, minute=15, second=0, microsecond=0)
        if now < start:
            return []
        rows = self.intraday(item, 1, start, now)
        rows.sort(key=lambda r: r["timestamp"])
        return rows
