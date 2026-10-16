"""Summarise the index node's /health (NIFTY, BANKNIFTY, SENSEX). Exit 1 if it does not answer.

python3 deploy/live-core/check_index.py http://140.245.228.101:10000
"""

import json
import sys
import urllib.request


def main(argv: list[str]) -> int:
    url = argv[1].rstrip("/") + "/health"
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            h = json.loads(r.read().decode("utf-8"))
    except Exception as exc:
        print(f"INDEX NODE: no answer from {url}: {type(exc).__name__}")
        return 1
    print(f"INDEX NODE: status={h.get('status')} market_open={h.get('market_open')} reasons={h.get('reasons')}")
    for sym, c in (h.get("indices") or {}).items():
        print(
            f"  {sym}: candles={c.get('candles')} last={c.get('last_candle')} prev_close={c.get('previous_close')} "
            f"open={c.get('today_open')} options={c.get('options_status')} expiry={c.get('options_expiry')} "
            f"strikes={c.get('options_strikes')} depth={c.get('depth_status')} contracts={c.get('depth_contracts')}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
