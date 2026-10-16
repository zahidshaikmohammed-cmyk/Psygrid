"""Minimum detectable effect for the pre-registered pooled tests (FROZEN, see prereg.lock).

Trades of one hypothesis are pooled across instruments and clustered by calendar week. With W weeks,
m trades per week, per-trade standard deviation sigma and average correlation rho between trades in
the same week (shared market moves), the standard error of the mean per trade is approximately

    SE = sigma * sqrt(rho + (1 - rho) / m) / sqrt(W)

and the one-sided minimum detectable mean (net) at significance alpha with the given power is
(z_{1-alpha} + z_{power}) * SE. Holm's first step uses alpha = 0.05 / 6.
"""

from __future__ import annotations

from statistics import NormalDist

Z = NormalDist().inv_cdf


def se(sigma: float, rho: float, per_week: float, weeks: int) -> float:
    return sigma * (rho + (1 - rho) / max(per_week, 1e-9)) ** 0.5 / weeks**0.5


def mde(sigma: float, rho: float, per_week: float, weeks: int, alpha: float = 0.05 / 6, power: float = 0.8) -> float:
    return (Z(1 - alpha) + Z(power)) * se(sigma, rho, per_week, weeks)


def table(scenarios) -> list[dict]:
    """scenarios: iterable of (label, sigma%, trades_per_week, rho, weeks)."""
    return [
        {
            "scenario": lab,
            "sigma_pct": s,
            "trades_per_week": m,
            "rho": rho,
            "weeks": w,
            "se_pct": round(se(s, rho, m, w), 4),
            "mde_pct": round(mde(s, rho, m, w), 4),
        }
        for lab, s, m, rho, w in scenarios
    ]
