from __future__ import annotations

import threading
import time

import requests


class SelfKeepAlive:
    """Small internal HTTP heartbeat for the Render web service."""

    INTERVAL_SECONDS = 300.0
    TIMEOUT_SECONDS = 15.0

    def __init__(self, url: str):
        self.url = url
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.session = requests.Session()

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="psygrid-self-keepalive",
        )
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(timeout=3)
        self.thread = None
        self.session.close()

    def _run(self) -> None:
        # Do not create an immediate request storm at startup. The service is
        # already serving the request that started it; subsequent heartbeats
        # happen every five minutes.
        while not self.stop_event.wait(self.INTERVAL_SECONDS):
            try:
                self.session.get(
                    self.url,
                    timeout=self.TIMEOUT_SECONDS,
                    headers={"Cache-Control": "no-cache"},
                )
            except requests.RequestException:
                # Keepalive failure must never interfere with market acquisition.
                pass
