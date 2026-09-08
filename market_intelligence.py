from __future__ import annotations

"""Additive market-regime and sector intelligence for Psygrid public payloads.

This module does not alter Psygrid's feed, candles, indicators, readiness rules,
or execution data. It only derives contextual analytics from the existing live
quotes/candles already held in RAM and attaches them to public JSON payloads.
"""

from collections import defaultdict
from statistics import median
from typing import Iterable

# Broad, stable sector taxonomy for the current PSYGRID_270 universe.
# Symbols not explicitly classified fall back to OTHER rather than being
# silently assigned to a potentially wrong sector.
_SECTOR_RULES: dict[str, set[str]] = {
    "BANKING": {
        "HDFCBANK", "ICICIBANK", "SBIN", "AXISBANK", "KOTAKBANK", "INDUSINDBK",
        "BANKBARODA", "PNB", "CANBK", "BANDHANBNK", "BANKINDIA", "MAHABANK",
        "FEDERALBNK", "IDFCFIRSTB", "KARURVYSYA", "RBLBANK", "AUBANK", "INDIANB",
    },
    "FINANCIAL_SERVICES": {
        "BAJFINANCE", "BAJAJFINSV", "CANFINHOME", "CHOLAFIN", "CAMS", "CRISIL",
        "HDFCAMC", "HDFCLIFE", "ICICIGI", "ICICIPRULI", "LICHSGFIN", "M&MFIN",
        "MANAPPURAM", "MFSL", "MUTHOOTFIN", "POONAWALLA", "PNBHOUSING", "ABCAPITAL",
        "AAVAS", "CREDITACC", "IIFL", "LTF", "BAJAJHLDNG", "GICRE", "LICHSGFIN",
        "POLICYBZR", "BSE", "CDSL", "MCX", "KFINTECH", "LALPATHLAB",
    },
    "INFORMATION_TECHNOLOGY": {
        "TCS", "INFY", "HCLTECH", "WIPRO", "TECHM", "LTM", "MPHASIS", "PERSISTENT",
        "COFORGE", "LTTS", "BSOFT", "CYIENT", "KPITTECH", "OFSS", "TATAELXSI", "ZENSARTECH",
    },
    "TELECOM": {"BHARTIARTL", "INDUSTOWER", "HFCL", "RAILTEL", "TATACOMM"},
    "AUTOMOBILE": {
        "MARUTI", "M&M", "TMPV", "EICHERMOT", "BAJAJ-AUTO", "HEROMOTOCO", "TVSMOTOR",
        "ASHOKLEY", "BOSCHLTD", "BHARATFORG", "MOTHERSON", "BALKRISIND", "CEATLTD",
        "APOLLOTYRE", "ESCORTS", "MRF", "JKTYRE", "SONACOMS", "TIINDIA",
    },
    "PHARMA_HEALTHCARE": {
        "SUNPHARMA", "DRREDDY", "CIPLA", "DIVISLAB", "APOLLOHOSP", "LUPIN", "AUROPHARMA",
        "ZYDUSLIFE", "TORNTPHARM", "MAXHEALTH", "BIOCON", "CAPLIPOINT", "IPCALAB",
        "KIMS", "LAURUSLABS", "AJANTPHARM", "ALKEM", "ABBOTINDIA", "GLAND", "GRANULES",
        "NATCOPHARM", "PPLPHARMA", "FDC", "MEDANTA", "SUPRIYA",
    },
    "METALS_MINING": {
        "TATASTEEL", "JSWSTEEL", "HINDALCO", "VEDL", "COALINDIA", "JINDALSTEL", "NMDC",
        "SAIL", "NATIONALUM", "HINDZINC", "HINDCOPPER", "JINDALSAW", "NAVA", "GRAPHITE",
    },
    "ENERGY": {
        "RELIANCE", "ONGC", "IOC", "BPCL", "GAIL", "HINDPETRO", "OIL", "IGL", "MGL",
        "PETRONET", "ATGL", "ADANIGREEN", "ADANIENSOL", "TATAPOWER", "JSWENERGY",
        "TORNTPOWER", "CESC", "GUJENERGY", "INOXWIND", "NHPC", "NTPC", "POWERGRID",
        "RECLTD", "PFC", "IREDA", "SJVN",
    },
    "DEFENCE_AEROSPACE": {"BEL", "HAL", "BDL", "MAZDOCK", "COCHINSHIP"},
    "CAPITAL_GOODS_ENGINEERING": {
        "LT", "SIEMENS", "ABB", "CUMMINSIND", "AIAENG", "APLAPOLLO", "CGPOWER", "POWERINDIA",
        "KEC", "KEI", "ELECON", "ELGIEQUIP", "HAVELLS", "HBLENGINE", "HONAUT", "ACE",
        "TRITURBINE", "BHEL", "3MINDIA", "APARINDS", "FINCABLES", "FINPIPE", "KAYNES",
        "KPIL", "NCC", "NBCC", "RVNL", "RITES", "IRCON", "OLECTRA",
    },
    "CEMENT": {"ULTRACEMCO", "GRASIM", "SHREECEM", "AMBUJACEM", "BIRLACORPN", "DALBHARAT", "JKCEMENT"},
    "CONSUMER_FMCG": {
        "ITC", "HINDUNILVR", "NESTLEIND", "BRITANNIA", "TATACONSUM", "COLPAL", "DABUR",
        "GODREJCP", "MARICO", "VBL", "AWL", "JUBLFOOD", "EMAMILTD", "PIDILITIND",
        "BATAINDIA", "KALYANKJIL", "DEVYANI", "DMART", "ABFRL", "TRENT",
    },
    "CHEMICALS": {
        "ASIANPAINT", "SRF", "TATACHEM", "PIIND", "UPL", "AARTIIND", "ATUL", "DEEPAKNTR",
        "FLUOROCHEM", "CHAMBLFERT", "COROMANDEL", "EIDPARRY", "DCMSHRIRAM", "JUBLINGREA",
    },
    "REALTY": {"DLF", "LODHA", "OBEROIRLTY", "PRESTIGE", "GODREJPROP", "BRIGADE", "PHOENIXLTD", "ABREL"},
    "LOGISTICS_TRANSPORT": {
        "ADANIPORTS", "CONCOR", "DELHIVERY", "BLUEDART", "GESHIP", "IRCTC", "RVNL", "RAILTEL",
    },
    "MEDIA_INTERNET": {"ETERNAL", "NAUKRI", "INDIAMART", "PAYTM", "ZEEL"},
    "TRAVEL_LEISURE": {"INDIGO", "PVRINOX", "LEMONTREE"},
    "TEXTILES_APPAREL": {"ABFRL", "TRENT"},
}

# Explicit overrides for names whose business spans multiple broad groups.
_OVERRIDES = {
    "ADANIENT": "DIVERSIFIED",
    "ADANIPOWER": "ENERGY",
    "ADANIPORTS": "LOGISTICS_TRANSPORT",
    "M&M": "AUTOMOBILE",
    "TMPV": "AUTOMOBILE",
    "BSE": "FINANCIAL_SERVICES",
    "CDSL": "FINANCIAL_SERVICES",
    "MCX": "FINANCIAL_SERVICES",
    "CRISIL": "FINANCIAL_SERVICES",
    "LALPATHLAB": "PHARMA_HEALTHCARE",
    "PIDILITIND": "CHEMICALS",
    "COLPAL": "CONSUMER_FMCG",
}


def sector_for_symbol(symbol: str) -> str:
    symbol = str(symbol).strip().upper()
    if symbol in _OVERRIDES:
        return _OVERRIDES[symbol]
    for sector, symbols in _SECTOR_RULES.items():
        if symbol in symbols:
            return sector
    return "OTHER"


def _float(value):
    try:
        value = float(value)
        return value if value == value else None
    except (TypeError, ValueError):
        return None


def _market_context(state, security_id: str) -> dict:
    with state.lock:
        return dict(getattr(state, "market_context", {}).get(security_id, {}))


def _stock_metrics(state, security_id: str, stock: dict) -> dict:
    context = _market_context(state, security_id)
    ltp = _float(stock.get("ltp"))
    if ltp is None:
        ltp = _float(context.get("ltp"))
    prev_close = _float(context.get("prev_close"))
    day_open = _float(context.get("day_open"))
    vwap = _float(context.get("dhan_day_vwap"))

    prev_return = ((ltp / prev_close) - 1.0) * 100.0 if ltp and prev_close and prev_close > 0 else None
    open_return = ((ltp / day_open) - 1.0) * 100.0 if ltp and day_open and day_open > 0 else None

    rows_5m = stock.get("5m") or stock.get("timeframes", {}).get("5m") or []
    recent_5m_return = None
    if len(rows_5m) >= 2:
        a = _float(rows_5m[-2].get("close"))
        b = _float(rows_5m[-1].get("close"))
        if a and b and a > 0:
            recent_5m_return = ((b / a) - 1.0) * 100.0

    return {
        "prev_return_pct": prev_return,
        "open_return_pct": open_return,
        "recent_5m_return_pct": recent_5m_return,
        "above_vwap": bool(ltp is not None and vwap is not None and ltp >= vwap),
        "vwap": vwap,
        "ltp": ltp,
    }


def _pct(values: list[bool]) -> float:
    return round((sum(1 for x in values if x) / len(values)) * 100.0, 2) if values else 0.0


def _avg(values: list[float]) -> float | None:
    return round(sum(values) / len(values), 4) if values else None


def _regime_label(breadth: float, above_vwap: float, median_return: float, five_min_median: float) -> str:
    bullish = 0
    bearish = 0
    if breadth >= 60: bullish += 1
    if breadth <= 40: bearish += 1
    if above_vwap >= 60: bullish += 1
    if above_vwap <= 40: bearish += 1
    if median_return >= 0.25: bullish += 1
    if median_return <= -0.25: bearish += 1
    if five_min_median >= 0.10: bullish += 1
    if five_min_median <= -0.10: bearish += 1

    if bullish >= 3 and bullish > bearish:
        return "BULLISH"
    if bearish >= 3 and bearish > bullish:
        return "BEARISH"
    if bullish >= 2 and bullish > bearish:
        return "MILDLY_BULLISH"
    if bearish >= 2 and bearish > bullish:
        return "MILDLY_BEARISH"
    return "NEUTRAL_ROTATIONAL"


def build_market_regime(state, stocks: dict[str, dict]) -> dict:
    """Build a broad regime from the complete 270-stock universe when available."""
    rows = []
    for security_id, meta in getattr(state, "instruments", {}).items():
        symbol = meta.get("symbol", security_id)
        stock = stocks.get(symbol)
        if stock is None:
            # Range endpoints pass only 45/15 stocks. Build a minimal quote-only row
            # from RAM for the missing names so the regime remains universe-wide.
            with state.lock:
                ltp = state.last_ltp_by_security.get(security_id)
            context = _market_context(state, security_id)
            stock = {"ltp": ltp}
            stock["_context_only"] = context
        metrics = _stock_metrics(state, security_id, stock)
        rows.append(metrics)

    prev_returns = [x["prev_return_pct"] for x in rows if x["prev_return_pct"] is not None]
    five_returns = [x["recent_5m_return_pct"] for x in rows if x["recent_5m_return_pct"] is not None]
    breadth = _pct([x["prev_return_pct"] is not None and x["prev_return_pct"] > 0 for x in rows])
    above_vwap = _pct([x["above_vwap"] for x in rows if x["vwap"] is not None])
    median_return = round(median(prev_returns), 4) if prev_returns else 0.0
    five_median = round(median(five_returns), 4) if five_returns else 0.0

    valid = len(prev_returns)
    label = _regime_label(breadth, above_vwap, median_return, five_median)
    score = max(-100.0, min(100.0, round(
        (breadth - 50.0) * 1.2 + (above_vwap - 50.0) * 0.8 + median_return * 20.0 + five_median * 25.0,
        2,
    )))

    return {
        "label": label,
        "score": score,
        "breadth": {"advancing_pct": breadth, "declining_pct": round(100.0 - breadth, 2)},
        "above_vwap_pct": above_vwap,
        "median_return_from_prev_close_pct": median_return,
        "median_recent_5m_return_pct": five_median,
        "coverage": {"stocks_with_prev_close_return": valid, "universe_size": len(rows)},
        "method": "270_STOCK_BREADTH_VWAP_MEDIAN_RETURN_5M_MOMENTUM",
        "source": "PSYGRID_DHAN_LIVE_RAM",
    }


def build_sector_intelligence(state, stocks: dict[str, dict]) -> dict[str, dict]:
    buckets: dict[str, list[dict]] = defaultdict(list)
    for security_id, meta in getattr(state, "instruments", {}).items():
        symbol = meta.get("symbol", security_id)
        stock = stocks.get(symbol, {})
        metrics = _stock_metrics(state, security_id, stock)
        metrics["symbol"] = symbol
        metrics["sector"] = sector_for_symbol(symbol)
        buckets[metrics["sector"]].append(metrics)

    sector_rows = []
    for sector, rows in buckets.items():
        returns = [x["prev_return_pct"] for x in rows if x["prev_return_pct"] is not None]
        five_returns = [x["recent_5m_return_pct"] for x in rows if x["recent_5m_return_pct"] is not None]
        sector_return = median(returns) if returns else 0.0
        sector_5m = median(five_returns) if five_returns else 0.0
        sector_rows.append({
            "sector": sector,
            "stock_count": len(rows),
            "median_return_pct": round(sector_return, 4),
            "median_5m_return_pct": round(sector_5m, 4),
            "breadth_pct": _pct([x["prev_return_pct"] is not None and x["prev_return_pct"] > 0 for x in rows]),
            "above_vwap_pct": _pct([x["above_vwap"] for x in rows if x["vwap"] is not None]),
        })

    sector_rows.sort(key=lambda x: (x["median_return_pct"], x["breadth_pct"]), reverse=True)
    rank_by_sector = {row["sector"]: i + 1 for i, row in enumerate(sector_rows)}
    for row in sector_rows:
        row["strength_rank"] = rank_by_sector[row["sector"]]

    return {row["sector"]: row for row in sector_rows}


def enrich_market_payload(state, payload: dict) -> dict:
    stocks = payload.get("stocks") or {}
    regime = build_market_regime(state, stocks)
    sectors = build_sector_intelligence(state, stocks)

    for symbol, stock in stocks.items():
        sector = sector_for_symbol(symbol)
        stock_sector = dict(sectors.get(sector, {"sector": sector, "stock_count": 0}))
        security_id = str(stock.get("security_id", ""))
        metrics = _stock_metrics(state, security_id, stock)
        stock_return = metrics.get("prev_return_pct")
        sector_return = _float(stock_sector.get("median_return_pct"))
        stock["sector"] = sector
        stock["sector_intelligence"] = {
            **stock_sector,
            "stock_return_from_prev_close_pct": round(stock_return, 4) if stock_return is not None else None,
            "relative_strength_vs_sector_pct": round(stock_return - sector_return, 4)
            if stock_return is not None and sector_return is not None else None,
            "sector_confirmation": (
                "CONFIRMED" if stock_return is not None and sector_return is not None and stock_return >= sector_return
                else "NOT_CONFIRMED" if stock_return is not None and sector_return is not None else "UNAVAILABLE"
            ),
        }
        stock["market_regime"] = {
            "label": regime["label"],
            "score": regime["score"],
        }

    payload["market_regime"] = regime
    payload["sector_intelligence"] = {
        "method": "STATIC_NSE_EQUITY_SECTOR_TAXONOMY_PLUS_LIVE_RELATIVE_STRENGTH",
        "sector_count": len(sectors),
        "sectors": sectors,
    }
    payload.setdefault("intelligence", {})["market_regime"] = "PYTHON_DERIVED"
    payload["intelligence"]["sector_intelligence"] = "PYTHON_DERIVED"
    payload["intelligence"]["source_data"] = "EXISTING_PSYGRID_DHAN_DATA_ONLY"
    return payload


def enrich_stock_payload(state, payload: dict) -> dict:
    """Enrich a single-stock endpoint using the complete live universe for context."""
    symbol = str(payload.get("symbol", "")).upper()
    stocks = {symbol: payload}
    enriched = enrich_market_payload(state, {"stocks": stocks})
    payload.update({k: v for k, v in enriched.items() if k != "stocks"})
    # Replace the stock with its enriched copy.
    payload.update(enriched["stocks"].get(symbol, {}))
    return payload
