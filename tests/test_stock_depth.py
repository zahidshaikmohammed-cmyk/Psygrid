import struct
import unittest
from types import SimpleNamespace

from stock_depth import (
    STOCK_DEPTH_CONTRACTS_PER_SYMBOL,
    STOCK_DEPTH_MAX_INSTRUMENTS,
    STOCK_DEPTH_STRIKES_PER_SYMBOL,
    STOCK_DEPTH_SYMBOLS_PER_BATCH,
    StockDepthContract,
    StockDepthManager,
    StockDepthState,
    _parse_depth_message,
    _select_contracts_for_symbol,
    stock_depth_json,
    stock_depth_listing_json,
)


def _option_state(strikes, expiry="2026-09-30", underlying_ltp=3000.0):
    return SimpleNamespace(snapshot=lambda: {"expiry": expiry, "underlying_ltp": underlying_ltp, "strikes": strikes})


def _strikes_around(center, count=9, step=50):
    rows = []
    for i in range(-count // 2, count // 2 + 1):
        strike = center + i * step
        rows.append(
            {
                "strike": float(strike),
                "ce": {"security_id": str(100000 + strike)},
                "pe": {"security_id": str(200000 + strike)},
            }
        )
    return rows


class SafetyInvariantTests(unittest.TestCase):
    def test_a_batch_never_exceeds_dhans_50_instrument_cap(self):
        # The one hard rule this whole module exists to respect.
        self.assertLessEqual(
            STOCK_DEPTH_SYMBOLS_PER_BATCH * STOCK_DEPTH_CONTRACTS_PER_SYMBOL, STOCK_DEPTH_MAX_INSTRUMENTS
        )
        self.assertEqual(STOCK_DEPTH_CONTRACTS_PER_SYMBOL, STOCK_DEPTH_STRIKES_PER_SYMBOL * 2)


class DepthMessageParsingTests(unittest.TestCase):
    def test_parses_20_level_bid_packet(self):
        header = struct.pack("<HBBiI", 332, 41, 2, 500001, 1)
        levels = b"".join(struct.pack("<dII", 100.5 + i, 10 + i, 2 + i) for i in range(20))
        parsed = _parse_depth_message(header + levels)
        self.assertEqual(len(parsed), 1)
        security_id, side, rows = parsed[0]
        self.assertEqual(security_id, "500001")
        self.assertEqual(side, "bid")
        self.assertEqual(len(rows), 20)
        self.assertEqual(rows[0], {"level": 1, "price": 100.5, "quantity": 10, "orders": 2})


class ContractSelectionTests(unittest.TestCase):
    def test_selects_nearest_strikes_per_symbol_and_both_sides(self):
        strikes = _strikes_around(3000, count=20, step=50)
        contracts, expiry, ltp = _select_contracts_for_symbol("RELIANCE", _option_state(strikes))
        self.assertEqual(expiry, "2026-09-30")
        self.assertEqual(ltp, 3000.0)
        self.assertEqual(len(contracts), STOCK_DEPTH_CONTRACTS_PER_SYMBOL)
        self.assertEqual(len({c.strike for c in contracts}), STOCK_DEPTH_STRIKES_PER_SYMBOL)
        self.assertEqual({c.option_type for c in contracts}, {"CE", "PE"})
        self.assertTrue(all(c.symbol == "RELIANCE" for c in contracts))

    def test_returns_nothing_before_the_option_chain_has_resolved(self):
        contracts, _expiry, _ltp = _select_contracts_for_symbol(
            "RELIANCE", _option_state([], expiry=None, underlying_ltp=None)
        )
        self.assertEqual(contracts, [])


class StockDepthStateTests(unittest.TestCase):
    def test_ram_only_contract_shape(self):
        settings = SimpleNamespace(timezone="Asia/Kolkata")
        state = StockDepthState("RELIANCE", settings)
        contracts = [StockDepthContract("500001", "RELIANCE", 3000.0, "CE", "2026-09-30")]
        state.set_contracts(contracts, "2026-09-30")
        state.update_depth("500001", "bid", [{"level": 1, "price": 100.0, "quantity": 500, "orders": 3}])
        state.update_quotes({"500001": {"last_price": 101.0, "volume": 1234, "oi": 5678}})

        payload = stock_depth_json(SimpleNamespace(snapshot=lambda symbol: state.snapshot()), "RELIANCE")
        self.assertEqual(payload["symbol"], "RELIANCE")
        self.assertEqual(payload["storage"], "RAM_ONLY")
        self.assertFalse(payload["synthetic_data"])
        self.assertEqual(payload["depth_levels"], 20)
        self.assertEqual(payload["contracts"][0]["bid"][0]["price"], 100.0)
        self.assertEqual(payload["contracts"][0]["volume"], 1234)

    def test_crossed_book_detection(self):
        settings = SimpleNamespace(timezone="Asia/Kolkata")
        state = StockDepthState("RELIANCE", settings)
        state.set_contracts([StockDepthContract("500001", "RELIANCE", 3000.0, "CE", "2026-09-30")], "2026-09-30")
        state.update_depth("500001", "bid", [{"level": 1, "price": 105.0, "quantity": 10, "orders": 1}])
        state.update_depth("500001", "ask", [{"level": 1, "price": 100.0, "quantity": 10, "orders": 1}])
        self.assertTrue(state.snapshot()["contracts"][0]["crossed_book"])


def _manager_with_symbols(symbols, security_ids_start=100):
    settings = SimpleNamespace(timezone="Asia/Kolkata", client_id="x", access_token="y")
    instruments = {s: SimpleNamespace(symbol=s, security_id=str(security_ids_start + i)) for i, s in enumerate(symbols)}
    option_states = {}
    for i, symbol in enumerate(symbols):
        option_states[symbol] = _option_state(_strikes_around(3000 + i, count=20, step=50))
    stock_options_manager = SimpleNamespace(instruments=instruments, states=option_states)
    return StockDepthManager(settings, dhan_api=SimpleNamespace(), stock_options_manager=stock_options_manager)


class StockDepthManagerBatchingTests(unittest.TestCase):
    def test_batches_split_symbols_within_the_instrument_cap(self):
        symbols = [f"SYM{i}" for i in range(12)]
        manager = _manager_with_symbols(symbols)

        batches = manager._batches()

        self.assertEqual(sum(len(b) for b in batches), 12)
        for batch in batches:
            self.assertLessEqual(len(batch), STOCK_DEPTH_SYMBOLS_PER_BATCH)

    def test_refresh_contracts_for_batch_never_exceeds_max_instruments(self):
        symbols = [f"SYM{i}" for i in range(STOCK_DEPTH_SYMBOLS_PER_BATCH)]
        manager = _manager_with_symbols(symbols)

        contracts = manager._refresh_contracts_for_batch(symbols)

        self.assertLessEqual(len(contracts), STOCK_DEPTH_MAX_INSTRUMENTS)
        self.assertEqual(len(contracts), len(symbols) * STOCK_DEPTH_CONTRACTS_PER_SYMBOL)

    def test_rotation_status_reflects_the_active_batch(self):
        symbols = [f"SYM{i}" for i in range(STOCK_DEPTH_SYMBOLS_PER_BATCH + 1)]
        manager = _manager_with_symbols(symbols)
        batches = manager._batches()

        manager._batch_symbols = []
        first_batch = batches[0]
        for symbol in manager._batch_symbols:
            manager.states[symbol].set_rotation_status("IDLE")
        manager._batch_symbols = first_batch
        for symbol in first_batch:
            manager.states[symbol].set_rotation_status("ACTIVE")

        for symbol in first_batch:
            self.assertEqual(manager.states[symbol].rotation_status, "ACTIVE")
        for symbol in symbols:
            if symbol not in first_batch:
                self.assertEqual(manager.states[symbol].rotation_status, "IDLE")

    def test_listing_reports_rotation_metadata(self):
        symbols = [f"SYM{i}" for i in range(7)]
        manager = _manager_with_symbols(symbols)

        listing = stock_depth_listing_json(manager)
        self.assertEqual(listing["universe"], "NIFTY_50_STOCK_DEPTH")
        self.assertEqual(listing["symbol_count"], 7)
        self.assertEqual(listing["resolved_count"], 7)
        self.assertEqual(listing["max_instruments_per_connection"], STOCK_DEPTH_MAX_INSTRUMENTS)
        self.assertFalse(listing["synthetic_data"])
        self.assertEqual(len(listing["stocks"]), 7)

    def test_snapshot_for_unknown_symbol_never_fabricates_data(self):
        manager = _manager_with_symbols(["RELIANCE"])
        snap = manager.snapshot("NOTASYMBOL")
        self.assertEqual(snap["status"], "UNKNOWN_SYMBOL")
        self.assertNotIn("contracts", snap)


if __name__ == "__main__":
    unittest.main()
