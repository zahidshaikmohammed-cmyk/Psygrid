from __future__ import annotations

"""Real options-chain analytics derived from a single Dhan option-chain
snapshot: no synthetic data, no fabricated history. Computed fresh on every
option-chain refresh (the same 3.2s cadence the chain itself refreshes on)
and attached to each *-options.json payload as an "analytics" block.

Deliberately chain-level (PCR, max pain, OI concentration, IV skew, OI
buildup classification) rather than price-series technical indicators
(RSI/MACD/etc.) on option premium: option premium decays with time even
when the underlying is flat, which makes price-based technical indicators
misleading for options. These are the standard derivatives-desk signals.
"""

from typing import Any, Optional


def _num(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _classify_buildup(price_up: Optional[bool], oi_up: Optional[bool]) -> str:
    if price_up is None or oi_up is None:
        return "INSUFFICIENT_DATA"
    if price_up and oi_up:
        return "LONG_BUILDUP"
    if not price_up and oi_up:
        return "SHORT_BUILDUP"
    if price_up and not oi_up:
        return "SHORT_COVERING"
    return "LONG_UNWINDING"


def compute_chain_analytics(
    rows: list[dict],
    underlying_ltp: Optional[float],
    previous: dict[str, dict],
) -> tuple[dict, dict[str, dict]]:
    """Pure function: (rows, underlying_ltp, previous-snapshot state) ->
    (analytics, next-snapshot state). `previous`/next state maps
    security_id -> {"oi": float, "ltp": float} from the prior chain
    refresh, used only to classify OI buildup direction between two
    consecutive real snapshots (ties are treated as "not up").
    """
    total_call_oi = total_put_oi = 0.0
    total_call_volume = total_put_volume = 0.0
    call_ivs: list[float] = []
    put_ivs: list[float] = []
    strike_call_oi: dict[float, float] = {}
    strike_put_oi: dict[float, float] = {}
    contracts: list[dict] = []
    next_previous: dict[str, dict] = {}

    for row in rows:
        strike = row.get("strike")
        if not isinstance(strike, (int, float)):
            continue
        for option_type, key in (("CE", "ce"), ("PE", "pe")):
            contract = row.get(key)
            if not isinstance(contract, dict):
                continue
            security_id = contract.get("security_id")
            oi = _num(contract.get("oi")) or 0.0
            volume = _num(contract.get("volume")) or 0.0
            ltp = _num(contract.get("last_price"))
            iv = _num(contract.get("implied_volatility"))

            if option_type == "CE":
                total_call_oi += oi
                total_call_volume += volume
                strike_call_oi[strike] = strike_call_oi.get(strike, 0.0) + oi
                if iv is not None:
                    call_ivs.append(iv)
            else:
                total_put_oi += oi
                total_put_volume += volume
                strike_put_oi[strike] = strike_put_oi.get(strike, 0.0) + oi
                if iv is not None:
                    put_ivs.append(iv)

            moneyness = None
            if isinstance(underlying_ltp, (int, float)):
                if strike == underlying_ltp:
                    moneyness = "ATM"
                elif option_type == "CE":
                    moneyness = "ITM" if strike < underlying_ltp else "OTM"
                else:
                    moneyness = "ITM" if strike > underlying_ltp else "OTM"

            classification = "INSUFFICIENT_DATA"
            sid = str(security_id) if security_id is not None else None
            if sid is not None:
                prev = previous.get(sid)
                if prev is not None and ltp is not None and prev.get("ltp") is not None and prev.get("oi") is not None:
                    classification = _classify_buildup(ltp > prev["ltp"], oi > prev["oi"])
                if ltp is not None:
                    next_previous[sid] = {"oi": oi, "ltp": ltp}
                elif prev is not None:
                    next_previous[sid] = prev

            contracts.append({
                "security_id": security_id,
                "strike": strike,
                "option_type": option_type,
                "moneyness": moneyness,
                "oi": oi,
                "volume": volume,
                "last_price": ltp,
                "implied_volatility": iv,
                "oi_change_classification": classification,
            })

    pcr_oi = (total_put_oi / total_call_oi) if total_call_oi > 0 else None
    pcr_volume = (total_put_volume / total_call_volume) if total_call_volume > 0 else None

    all_strikes = sorted(strike_call_oi.keys() | strike_put_oi.keys())

    atm_strike = None
    if isinstance(underlying_ltp, (int, float)) and all_strikes:
        atm_strike = min(all_strikes, key=lambda s: abs(s - underlying_ltp))

    max_pain_strike = None
    if all_strikes:
        def _payout(settle: float) -> float:
            total = 0.0
            for k in all_strikes:
                total += strike_call_oi.get(k, 0.0) * max(settle - k, 0.0)
                total += strike_put_oi.get(k, 0.0) * max(k - settle, 0.0)
            return total
        max_pain_strike = min(all_strikes, key=_payout)

    resistance_strikes = [s for s, _ in sorted(strike_call_oi.items(), key=lambda item: item[1], reverse=True)[:3]]
    support_strikes = [s for s, _ in sorted(strike_put_oi.items(), key=lambda item: item[1], reverse=True)[:3]]

    avg_call_iv = (sum(call_ivs) / len(call_ivs)) if call_ivs else None
    avg_put_iv = (sum(put_ivs) / len(put_ivs)) if put_ivs else None
    iv_skew = (avg_call_iv - avg_put_iv) if avg_call_iv is not None and avg_put_iv is not None else None

    analytics = {
        "pcr_oi": pcr_oi,
        "pcr_volume": pcr_volume,
        "atm_strike": atm_strike,
        "max_pain_strike": max_pain_strike,
        "resistance_strikes": resistance_strikes,
        "support_strikes": support_strikes,
        "total_call_oi": total_call_oi,
        "total_put_oi": total_put_oi,
        "total_call_volume": total_call_volume,
        "total_put_volume": total_put_volume,
        "avg_call_iv": avg_call_iv,
        "avg_put_iv": avg_put_iv,
        "iv_skew": iv_skew,
        "contracts": contracts,
    }
    return analytics, next_previous


class ChainAnalyticsTracker:
    """Wraps compute_chain_analytics with the RAM-only previous-snapshot
    state needed for OI-buildup classification across refreshes."""

    def __init__(self) -> None:
        self._previous: dict[str, dict] = {}

    def update(self, rows: list[dict], underlying_ltp: Optional[float]) -> dict:
        analytics, self._previous = compute_chain_analytics(rows, underlying_ltp, self._previous)
        return analytics
