"""Archive reader rules."""

import csv
import gzip

import numpy as np


def test_dhan_history_filler_bars_are_no_trade(tmp_path):
    """Dhan's historical API fills minutes without trades with flat zero-volume bars; the live feed has no bar.
    On bootstrap days those bars read as missing; a zero-volume bar that moved, and live days, are untouched."""
    import json as _json

    from daily_archive import EQUITY_COLUMNS, EQUITY_FILE, MANIFEST_FILE
    from intelligence.archive import load_day

    def write(source):
        day = tmp_path / source / "2026-09-01"
        day.mkdir(parents=True)
        rows = [
            ("TCS", "1", "2026-09-01 09:15:00 IST", 10, 11, 9, 10.5, 100),
            ("TCS", "1", "2026-09-01 09:16:00 IST", 10.5, 10.5, 10.5, 10.5, 0),  # filler
            ("TCS", "1", "2026-09-01 09:17:00 IST", 10.5, 10.7, 10.5, 10.6, 0),
        ]  # moved on zero volume
        with gzip.open(day / EQUITY_FILE, "wt", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(EQUITY_COLUMNS)
            writer.writerows(rows)
        (day / MANIFEST_FILE).write_text(_json.dumps({"source": source}))
        return load_day(tmp_path / source, "2026-09-01")

    history = write("DHAN_HISTORICAL_API")
    assert history.equity.no_trade_bars == 1  # the 09:16 filler is gone: no bar, exactly as live records it
    assert history.equity.close.tolist() == [[10.5, 10.6]] and len(history.equity.minutes) == 2
    live = write("PSYGRID_LIVE_ARCHIVE")
    assert live.equity.no_trade_bars == 0 and live.equity.close[0, 1] == 10.5
