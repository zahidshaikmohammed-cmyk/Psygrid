# Indicator Engine Audit — 2026-09-17

This branch is an audit/repair branch. The engine is **not complete**.

Required gates remain: canonical Wilder ADX, CMF edge-case correctness, stale-vs-invalid separation, synchronization semantics, full mathematical audit, no-lookahead validation, float64 intermediate arithmetic, and the real 450-stock PSYGRID fixture.

No trading signals, predictions, scores, or trade authorization are permitted in this layer.
