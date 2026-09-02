from __future__ import annotations

# Psygrid runtime activation shim. Keep app.py stable while ensuring the
# hardened serializer is selected before the application imports it.
try:
    import output_runtime
    import output_runtime_fixed
    output_runtime.market_live_json = output_runtime_fixed.market_live_json
except Exception:
    pass
