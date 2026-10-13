"""Verify the pre-registered files against prereg.lock.

python -m research.behaviour.lockcheck        # prints one line per file, exits 1 on any mismatch
"""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def verify() -> dict:
    lock = json.loads((HERE / "prereg.lock").read_text())
    files = {}
    for name, want in sorted(lock.items()):
        if name.startswith("_"):
            continue
        got = hashlib.sha256((HERE / name).read_bytes()).hexdigest()
        files[name] = {"locked": want, "actual": got, "match": got == want}
    return {"ok": bool(files) and all(f["match"] for f in files.values()), "files": files}


def main() -> int:
    res = verify()
    for name, f in res["files"].items():
        print(
            f"{'OK      ' if f['match'] else 'MISMATCH'} {name} {f['actual']}"
            + ("" if f["match"] else f" (locked {f['locked']})")
        )
    print("pre-registration intact" if res["ok"] else "PRE-REGISTRATION CHANGED - refusing to run")
    return 0 if res["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
