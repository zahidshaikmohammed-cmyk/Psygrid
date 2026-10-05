"""PSYGRID Live Core: a separate, minimal runtime for the equity 1-minute feed on small VMs.

The Live Core is NOT the full PSYGRID application. ``python app.py`` still means the full
service; the Live Core starts with ``python -m live_core`` and runs only:

    Dhan WebSocket -> live 1-minute equity OHLCV -> RAM -> HTTP JSON

for one deterministic partition of the canonical 989-stock universe (``stocks.json``). Two
nodes (``LIVE_CORE_NODE_ID`` 0 and 1, ``LIVE_CORE_NODE_COUNT=2``) together cover every stock
exactly once. Market data lives only in RAM for the current session and is wiped at 15:15 IST.

The package deliberately imports nothing that pulls in pandas/numpy or any of the full
application's managers (options, futures, depth, indicators, archive, intelligence).
"""

SERVICE_NAME = "PSYGRID_LIVE_CORE"
