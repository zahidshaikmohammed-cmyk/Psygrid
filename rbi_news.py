from __future__ import annotations

"""Live RBI (Reserve Bank of India) official press releases, notifications,
and speeches via RBI's own published RSS feeds — a first-party source, no
scraping, no API key, no ToS restriction (RSS is explicitly meant for
automated syndication). Raw feed items only: no bullish/bearish
classification, no market interpretation.
"""

import threading
import xml.etree.ElementTree as ET
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Optional
from zoneinfo import ZoneInfo

import requests

RBI_FEEDS = {
    "press_releases": "https://www.rbi.org.in/pressreleases_rss.xml",
    "notifications": "https://www.rbi.org.in/notifications_rss.xml",
    "speeches": "https://www.rbi.org.in/speeches_rss.xml",
}
RBI_NEWS_REFRESH_SECONDS = 300.0


def _parse_rss(xml_text: str, category: str) -> list[dict]:
    items: list[dict] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return items
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        description = (item.findtext("description") or "").strip()
        pub_date_raw = (item.findtext("pubDate") or "").strip()
        guid = (item.findtext("guid") or link or title).strip()
        pub_date_iso = None
        if pub_date_raw:
            try:
                pub_date_iso = parsedate_to_datetime(pub_date_raw).isoformat()
            except (TypeError, ValueError):
                pub_date_iso = None
        items.append({
            "id": guid,
            "headline": title,
            "source": "RBI_OFFICIAL",
            "category": category,
            "url": link,
            "summary": description,
            "published_at": pub_date_iso,
            "published_at_raw": pub_date_raw or None,
            "country": "IN",
        })
    return items


class RbiNewsState:
    def __init__(self, settings):
        self.tz = ZoneInfo(settings.timezone)
        self.lock = threading.RLock()
        self.status = "STARTING"
        self.last_error = ""
        self.updated_at: Optional[str] = None
        self.items: list[dict] = []
        self.feed_errors: dict[str, str] = {}

    def set_items(self, items: list[dict], feed_errors: dict[str, str]) -> None:
        with self.lock:
            self.items = items
            self.feed_errors = feed_errors
            self.updated_at = datetime.now(self.tz).isoformat()
            self.status = "LIVE" if items else "ERROR"
            self.last_error = "; ".join(f"{k}: {v}" for k, v in feed_errors.items()) if feed_errors else ""

    def set_error(self, error: str) -> None:
        with self.lock:
            self.status = "ERROR"
            self.last_error = error

    def snapshot(self) -> dict:
        with self.lock:
            payload = {
                "service": "PSYGRID",
                "status": self.status,
                "data_source": "RBI_OFFICIAL_RSS",
                "market_data_status": "NEAR_LIVE",
                "note": "RBI's own official RSS feeds only (press releases, notifications, speeches). Not general market news.",
                "item_count": len(self.items),
                "items": list(self.items),
                "feed_errors": dict(self.feed_errors),
                "updated_at": self.updated_at,
                "synthetic_data": False,
                "storage": "RAM_ONLY",
                "refresh_seconds": RBI_NEWS_REFRESH_SECONDS,
            }
            if self.last_error:
                payload["error"] = self.last_error
            return payload


class RbiNewsManager:
    def __init__(self, settings):
        self.settings = settings
        self.state = RbiNewsState(settings)
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.session = requests.Session()

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._loop, daemon=True, name="psygrid-rbi-news")
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=8)
        self.thread = None

    def _fetch_all(self) -> tuple[list[dict], dict[str, str]]:
        items: list[dict] = []
        errors: dict[str, str] = {}
        for category, url in RBI_FEEDS.items():
            try:
                response = self.session.get(url, timeout=15, headers={"User-Agent": "Psygrid/1.0 (+market-data-layer)"})
                response.raise_for_status()
                items.extend(_parse_rss(response.text, category))
            except Exception as exc:
                errors[category] = f"{type(exc).__name__}: {exc}"
        items.sort(key=lambda x: x.get("published_at") or "", reverse=True)
        return items, errors

    def _loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                items, errors = self._fetch_all()
                self.state.set_items(items, errors)
            except Exception as exc:
                self.state.set_error(f"{type(exc).__name__}: {exc}")
            self.stop_event.wait(RBI_NEWS_REFRESH_SECONDS)


def rbi_news_json(state: RbiNewsState) -> dict:
    return state.snapshot()
