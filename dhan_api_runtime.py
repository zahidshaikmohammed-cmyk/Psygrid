from __future__ import annotations

from dhan_api import DhanAPI as BaseDhanAPI


class DhanAPI(BaseDhanAPI):
    """Runtime-safe Dhan API adapter.

    Dhan's Quote API documents depth as {buy:[...], sell:[...]}, while the
    runtime market-context normalizer consumes a flat list of bid/ask levels.
    Normalize it once at the API boundary so REST recovery has the same shape
    as the WebSocket Full feed.
    """

    @staticmethod
    def _normalize_depth(row: dict) -> None:
        depth = row.get("depth")
        if not isinstance(depth, dict):
            return
        buys = depth.get("buy") if isinstance(depth.get("buy"), list) else []
        sells = depth.get("sell") if isinstance(depth.get("sell"), list) else []
        levels = []
        for index in range(5):
            buy = buys[index] if index < len(buys) and isinstance(buys[index], dict) else {}
            sell = sells[index] if index < len(sells) and isinstance(sells[index], dict) else {}
            levels.append({
                "bid_price": buy.get("price", buy.get("bid_price")),
                "ask_price": sell.get("price", sell.get("ask_price")),
                "bid_quantity": buy.get("quantity", buy.get("bid_quantity", 0)),
                "ask_quantity": sell.get("quantity", sell.get("ask_quantity", 0)),
                "bid_orders": buy.get("orders", buy.get("bid_orders", 0)),
                "ask_orders": sell.get("orders", sell.get("ask_orders", 0)),
            })
        row["depth"] = levels

    def quote_snapshot(self, instruments):
        snapshot = super().quote_snapshot(instruments)
        for row in snapshot.values():
            if isinstance(row, dict):
                self._normalize_depth(row)
        return snapshot
