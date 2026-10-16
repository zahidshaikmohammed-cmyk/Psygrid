"""Write stocks_fno.json: the NSE stocks that have stock options (OPTSTK) in Dhan's instrument master.

Runs anywhere with internet access (no Dhan token: the instrument master is public). The order is
the canonical stocks.json order first, then any F&O stock missing from it, alphabetically.

    python deploy/live-core/fno_universe.py [OUTPUT]      # default: stocks_fno.json
"""

import csv
import json
import sys

import requests

MASTER = "https://images.dhan.co/api-data/api-scrip-master.csv"


def main(argv: list[str]) -> int:
    out = argv[1] if len(argv) > 1 else "stocks_fno.json"
    equities, fno = set(), set()
    with requests.get(MASTER, timeout=120, stream=True) as resp:
        resp.raise_for_status()
        for row in csv.DictReader(line.decode("utf-8", "replace") for line in resp.iter_lines()):
            if row.get("SEM_EXM_EXCH_ID", "").strip().upper() != "NSE":
                continue
            kind = row.get("SEM_INSTRUMENT_NAME", "").strip().upper()
            sym = row.get("SEM_TRADING_SYMBOL", "").strip().upper()
            if kind == "EQUITY" and sym and row.get("SEM_SMST_SECURITY_ID", "").strip():
                equities.add(sym)
            elif kind == "OPTSTK" and sym:
                fno.add(sym.split("-")[0])
    with open("stocks.json", encoding="utf-8") as fh:
        canonical = [str(s).strip().upper() for s in json.load(fh)["symbols"]]
    selected = {s for s in fno if s in equities}
    symbols = [s for s in canonical if s in selected] + sorted(selected - set(canonical))
    if len(symbols) < 100:
        print(f"only {len(symbols)} F&O stocks resolved; refusing to write {out}")
        return 1
    payload = {"universe": "PSYGRID_FNO", "exchange": "NSE", "instrument": "EQUITY", "symbols": symbols}
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=1)
        fh.write("\n")
    extra = sorted(selected - set(canonical))
    print(f"{len(fno)} OPTSTK underlyings; {len(symbols)} written to {out}; not in stocks.json: {extra}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
