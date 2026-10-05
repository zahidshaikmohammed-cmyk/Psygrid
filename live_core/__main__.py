"""``python -m live_core``: start one PSYGRID Live Core node (never the full PSYGRID app)."""

import sys

from live_core.runtime import main

if __name__ == "__main__":
    sys.exit(main())
