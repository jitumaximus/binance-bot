"""POC RETURN must behave exactly as deployed before the 1H RSI books (commit POC_BASELINE, 23 Nov 2026 14:05 IST).
1. AST: every function/method in bot.py is identical to the baseline's, except the hook points listed below; the
   POC functions are listed explicitly and must be identical.
2. Behaviour: with RSI1H_ENABLED=0 the hook points produce identical output (config, start-up message, /status,
   summaries) and a POC paper trade gives identical stops, exits, P&L, events and trades.csv rows.
3. With the 1H RSI books on: they get $100 each (never funded from POC), POC's book is untouched, they show in
   /status and the summaries, and BOOK JUMP stays off even if the old ENABLE_BOOKJUMP=1 is still in .env.
4. POC: one open trade at a time (no POC order while a POC trade is open)."""
import ast
import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
POC_BASELINE = "1536659"
HOOKS = {"make_cfg", "book_for", "Bot.__init__", "Bot.enabled_books", "Bot.check_exit_by_candles",
         "Bot.finish_entry", "Bot.close_trade", "Bot.status_text", "Bot.strategy_desc", "Bot.startup", "Bot.tick",
         "Binance.request",
         # entry execution (25 Nov 2026): try_enter changes only after POC's `if sig.get("limit"): return` (the
         # non-POC paper fill); enter_live only for the non-POC books; IOC reply type; depth limit (POC keeps 100)
         "Bot.try_enter", "Bot.enter_live", "Binance.orderbook", "Binance.place_order",
         # 1H RSI limit exits (26 Nov 2026): 1H RSI paper trades go to manage_rsi1h_paper (POC trades unchanged);
         # no extra slippage on a simulated limit exit; manual / session close sends a pending exit's rest at market
         "Bot.manage_open_trades", "Bot.close_now",
         # parallel 1H RSI trades (26 Nov 2026): open_trades also lists a book's more_trades (POC never has any)
         "Bot.open_trades"}
ADDED = {"Bot.paused_books", "Bot.poc_pause_step",          # POC_ENABLED=0 pause (24 Nov 2026)
         "Bot.capped_entry", "Bot.exit_exec", "BinanceLiveVenue.__init__", "BinanceLiveVenue.book",
         "BinanceLiveVenue.ioc",                            # entry execution (25 Nov 2026)
         "Bot.manage_rsi1h_paper", "Bot.start_limit_exit", "Bot.step_limit_exit",
         "Bot.finish_limit_exit",                           # 1H RSI limit exits (26 Nov 2026)
         "Bot.book_trades", "Bot.free_margin", "Bot.add_trade", "Bot.drop_trade",   # parallel 1H RSI trades
         "Bot.trade_status_line", "Bot.rsi1h_loss_pct",     # /status per trade (26 Nov 2026, sizing v2)
         "usd",                                             # exit slippage vs stop + vs book (26 Nov 2026)
         "Bot.margin_pcts", "pct_text", "stop_text",        # 1H RSI % on margin in /status (27 Nov 2026)
         "Bot.manage_ema20_paper"}                          # EMA20 PULLBACK paper exits (28 Nov 2026)
POC_FUNCS = ("Bot.tick_poc", "Bot.poc_pending_step", "Bot.poc_filled", "Bot.poc_place_limit", "Bot.poc_cancel",
             "Bot.poc_take_at_poc", "Bot.build_pocs", "Bot.poc_tier1", "Bot.manage_poc_live", "Bot.touch_stop_price",
             "Bot.set_touch_stop", "Bot.apply_trail", "Bot.apply_be", "Bot.entry_allowed",
             "Bot.init_books", "Bot.sweep_retired", "Bot.session_tasks", "Bot.report", "Bot.in_us_session",
             "Bot.manage_open_trade", "Bot.run", "poc_price", "size_trade", "size_by_margin")
ENV = {"EXCHANGE": "binance", "DRY_RUN": "1", "ENABLE_POC": "1", "ENABLE_SHORT": "0", "ENABLE_LONG": "0",
       "ENABLE_BREAKOUT": "0", "POC_SKIP_US_SESSION": "1", "START_CAPITAL_INR": "11890", "USD_INR_RATE": "85",
       "RSI1H_ENABLED": "0", "EMA20_ENABLED": "0"}
CLEAR = ("ENABLE_BOOKJUMP", "BJ_ENABLED", "POC_ENABLED")
PROD = {"id": 1, "symbol": "BTCUSDT", "tick": 0.1, "cv": 0.001, "type": "perpetual_futures", "stock_token": False}


def baseline_source():
    try:
        return subprocess.run(["git", "-C", HERE, "show", f"{POC_BASELINE}:bot.py"], capture_output=True, text=True,
                              check=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return None


def functions(src):
    out = {}
    for node in ast.parse(src).body:
        if isinstance(node, ast.FunctionDef):
            out[node.name] = ast.dump(node)
        elif isinstance(node, ast.ClassDef):
            for f in node.body:
                if isinstance(f, ast.FunctionDef):
                    out[f"{node.name}.{f.name}"] = ast.dump(f)
    return out


OLD_SRC = baseline_source()


class FakeData:
    """Candles / tickers for a scripted POC paper trade (no network)."""
    def __init__(self, m1=None):
        self.m1 = m1 or []

    def candles(self, sym, res, count, now=None):
        if res == "1m":
            return [c for c in self.m1 if now is None or c["time"] <= now]
        return []

    def ticker(self, sym):
        return {"mark_price": 100.0}

    def tickers(self, ct):
        return [{"symbol": "BTCUSDT", "mark_price": 100.0}]


@unittest.skipIf(OLD_SRC is None, "git / baseline commit not available")
class PocUnchanged(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.saved_env = {k: os.environ.get(k) for k in list(ENV) + list(CLEAR)}
        os.environ.update(ENV)
        for k in CLEAR:
            os.environ.pop(k, None)
        cls.tmp = tempfile.mkdtemp()
        p = os.path.join(cls.tmp, "bot_base.py")
        with open(p, "w") as fh:
            fh.write(OLD_SRC)
        spec = importlib.util.spec_from_file_location("bot_base", p)
        cls.old = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.old)
        import bot as new
        cls.new = new

    @classmethod
    def tearDownClass(cls):
        for k, v in cls.saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def test_only_hook_points_changed(self):
        with open(os.path.join(HERE, "bot.py")) as fh:
            new_f = functions(fh.read())
        old_f = functions(OLD_SRC)
        self.assertEqual(set(old_f) - set(new_f), set(), "functions removed")
        self.assertEqual(set(new_f) - set(old_f), ADDED, "unexpected new functions")
        changed = {n for n in old_f if old_f[n] != new_f[n]}
        self.assertEqual(changed - HOOKS, set(), f"changed outside the hook points: {changed - HOOKS}")
        for name in POC_FUNCS:
            self.assertEqual(old_f[name], new_f[name], name)

    def make(self, mod, data_dir, data=None):
        cfg = mod.make_cfg()
        cfg["data_dir"] = data_dir
        b = mod.Bot(cfg)
        b.state["exchange"] = "binance"                     # like the live state.json (not a first-ever start)
        sent = []
        b.tg.send = sent.append
        b.get_products = lambda force=False: [dict(PROD)]
        if data is not None:
            b.data = data
        return b, sent

    def bots(self, data=None):
        d1, d2 = tempfile.mkdtemp(dir=self.tmp), tempfile.mkdtemp(dir=self.tmp)
        return self.make(self.old, d1, data), self.make(self.new, d2, data)

    # ---- 2. identical with the 1H RSI books off ----
    def test_config_identical_apart_from_new_switch(self):
        old, new = self.old.make_cfg(), self.new.make_cfg()
        self.assertEqual(new.pop("enable_rsi1h"), False)
        self.assertEqual(new.pop("enable_ema20"), False)            # EMA20 PULLBACK (28 Nov 2026), off here
        self.assertEqual(new.pop("poc_paused"), False)              # POC_ENABLED unset = running
        for k, v in (("entry_slip_cap_pct", 0.001), ("liq_cap_pct", 0.3), ("entry_max_children", 20),
                     ("exit_slip_cap_pct", 0.001), ("exit_guard_r", 0.5), ("exit_max_children", 20)):
            self.assertEqual(new.pop(k), v)                         # entry execution (25 Nov 2026)
        old.pop("data_dir"), new.pop("data_dir")
        self.assertEqual(old, new)

    def test_startup_status_summary_identical(self):
        (bo, so), (bn, sn) = self.bots()
        bo.startup()
        bn.startup()
        self.assertIsNone(bn.bj)
        self.assertIsNone(bn.rsi1h)
        self.assertEqual(so, sn)
        for b in (bo, bn):
            bk = b.state["books"]["POC RETURN"]
            bk["balance"] = 141.25
            bk["day"]["2026-11-20"] = {"trades": 2, "pnl": 1.36, "start_equity": 139.89, "notified": False}
            b.state["poc"] = {"made": 1763672400, "levels": {"ETHUSDT": {"poc": 3001.5, "product_id": 2,
                                                                         "armed_long": True, "armed_short": False}}}
        self.assertEqual(bo.status_text(), bn.status_text())
        self.assertEqual(bo.report(), bn.report())
        self.assertEqual(bo.report("2026-11-20"), bn.report("2026-11-20"))

    def test_poc_paper_trade_identical(self):
        """A POC long: touch-candle stop, breakeven at 1R, trail from 3R, exit on the trail - same in both."""
        t0 = 1_763_700_000 // 900 * 900 - 60          # filled 1 min before a 15m close: the touch candle ends at t0+60
        path = [100.0, 99.9, 99.95, 100.2, 100.6, 101.0, 101.4, 101.6, 101.2, 100.9, 100.5, 100.2, 99.9]
        m1 = [{"time": t0 + 60 * k, "open": px, "high": px + 0.1, "low": px - 0.1, "close": px, "volume": 1}
              for k, px in enumerate(path)]
        results = []
        for mod in (self.old, self.new):
            d = tempfile.mkdtemp(dir=self.tmp)
            data = FakeData(m1)
            b, sent = self.make(mod, d, data)
            b.startup()
            events = []
            b.event = lambda typ, **kw: events.append((typ, kw))
            sig = {"side": "long", "symbol": "BTCUSDT", "p": dict(PROD), "strategy": "poc", "entry": 100.0,
                   "limit": True, "S": {"time": t0, "low": 99.75, "high": 100.25}, "E": {"time": t0 - 900},
                   "period": 900, "extra": {"poc": 100.0, "made": 1, "touch_close": t0 + 60}}
            trade, info = b.try_enter(sig, t0)
            b.state["poc_pending"] = None
            b.poc_filled({"symbol": "BTCUSDT", "side": "long", "trade": trade, "info": info}, 100.0,
                         trade["contracts"], t0)
            t = b.book("POC RETURN")["open_trade"]
            res = None
            for k in range(1, len(m1) + 2):
                res = b.check_exit_by_candles(t, t0 + 60 * k)
                if res:
                    break
            self.assertIsNotNone(res)
            b.close_trade(t, res)
            with open(os.path.join(d, "trades.csv")) as fh:
                rows = fh.read().splitlines()
            results.append((res, round(b.book("POC RETURN")["balance"], 6), events, rows[-1], sent[1:]))
        self.assertEqual(results[0], results[1])
        self.assertIn(results[1][0]["reason"], ("TRAIL_STOP", "BREAKEVEN_STOP", "STOP_LOSS"))

    def test_sweep_and_init_same_as_baseline(self):
        (bo, so), (bn, sn) = self.bots()
        for b in (bo, bn):
            b.state["books"] = {"POC RETURN": {"start": 120.0, "balance": 120.0, "open_trade": None, "day": {},
                                               "month": {}, "trades": 3, "wins": 1},
                                "1H BREAKOUT": {"start": 7.0, "balance": 7.0, "open_trade": None, "day": {},
                                                "month": {}, "trades": 0, "wins": 0}}
            b.state["swept_to_wallet"] = True
            b.sweep_retired()
            b.init_books()
        self.assertEqual(bo.state["books"], bn.state["books"])
        self.assertEqual(so, sn)

    # ---- 3. with the 1H RSI books on ----
    def rsi_bot(self):
        os.environ["RSI1H_ENABLED"] = "1"
        os.environ["ENABLE_BOOKJUMP"] = "1"                  # the old switch, still in the live .env: ignored now
        try:
            d = tempfile.mkdtemp(dir=self.tmp)
            b, sent = self.make(self.new, d)
        finally:
            os.environ["RSI1H_ENABLED"] = "0"
            os.environ.pop("ENABLE_BOOKJUMP")
        b.state["books"] = {"POC RETURN": {"start": 31.42, "balance": 31.42, "open_trade": None, "day": {}, "month": {},
                                           "trades": 9, "wins": 4},
                            "BOOK JUMP": {"strategy": "BOOK JUMP", "balance": 97.55, "start": 100, "open_trade": None,
                                          "day": {}, "month": {}, "trades": 30, "wins": 10, "positions": {}}}
        b.state["books_since"] = "2026-11-01T00:00:00+05:30"
        b.state["swept_to_wallet"] = True
        return b, sent

    def test_rsi_books_funded_separately_and_reported(self):
        b, sent = self.rsi_bot()
        self.assertIsNotNone(b.rsi1h)
        b.startup()
        books = b.state["books"]
        self.assertEqual((books["1H RSI LONG"]["balance"], books["1H RSI SHORT"]["balance"]), (100.0, 100.0))
        self.assertEqual(books["POC RETURN"]["balance"], 31.42)          # never a donor
        self.assertEqual(books["BOOK JUMP"]["balance"], 97.55)           # old book left in state.json, unused
        self.assertIsNone(b.bj)
        self.assertEqual(b.book_names(), ["POC RETURN", "1H RSI LONG", "1H RSI SHORT"])
        start = sent[-1]
        self.assertIn("1H RSI LONG / 1H RSI SHORT [PAPER]: ON", start)
        self.assertNotIn("BOOK JUMP", start)
        st = b.status_text()
        for x in ("1H RSI LONG: $100.00", "1H RSI SHORT: $100.00", "1H RSI: universe 0"):
            self.assertIn(x, st)
        self.assertNotIn("BOOK JUMP", st)
        rep = b.report()
        self.assertIn("1H RSI LONG: 0 trades", rep)
        self.assertIn("ALL 3:", rep)
        self.assertNotIn("BOOK JUMP", rep)
        b.sweep_retired()                                                # 3 books on: nothing is swept
        self.assertEqual(b.state["books"]["1H RSI LONG"]["balance"], 100.0)

    def test_rsi_refused_when_live(self):
        os.environ.update(RSI1H_ENABLED="1", DRY_RUN="0")
        try:
            cfg = self.new.make_cfg()
            cfg["data_dir"] = tempfile.mkdtemp(dir=self.tmp)
            b = self.new.Bot(cfg)
            self.assertIsNone(b.rsi1h)
            self.assertIn("DRY_RUN is off", b.rsi1h_refused)
            self.assertNotIn("1H RSI LONG", b.enabled_books())
        finally:
            os.environ.update(RSI1H_ENABLED="0", DRY_RUN="1")

    def test_bookjump_only_with_bj_enabled(self):
        os.environ["ENABLE_BOOKJUMP"] = "1"
        try:
            self.assertFalse(self.new.make_cfg()["enable_bookjump"])
            os.environ["BJ_ENABLED"] = "1"
            self.assertTrue(self.new.make_cfg()["enable_bookjump"])
        finally:
            os.environ.pop("ENABLE_BOOKJUMP")
            os.environ.pop("BJ_ENABLED", None)

    # ---- 4. POC: one open trade at a time ----
    def test_poc_no_order_while_a_poc_trade_is_open(self):
        import time as _t
        d = tempfile.mkdtemp(dir=self.tmp)
        b, sent = self.make(self.new, d, FakeData())
        b.startup()
        now = _t.time()
        made = b.last_us_close(now)
        b.state["poc"] = {"made": made, "tiered": 2, "levels": {"BTCUSDT": {"poc": 100.0, "product_id": 1,
                                                                            "armed_long": True, "armed_short": False}}}
        b.in_us_session = lambda now: False
        b.data.tickers = lambda ct: [{"symbol": "BTCUSDT", "mark_price": 100.05}]      # within 0.15% of the POC
        b.book("POC RETURN")["open_trade"] = {"symbol": "ETHUSDT", "product_id": 2, "side": "long"}
        events = []
        b.event = lambda typ, **kw: events.append(typ)
        b._poc_poll = 0
        b.tick_poc(now)
        self.assertIsNone(b.state.get("poc_pending"))
        self.assertNotIn("POC_ORDER", events)
        b.book("POC RETURN")["open_trade"] = None                      # free again -> the order is placed
        b._poc_poll = 0
        b.tick_poc(now + 10)
        self.assertEqual((b.state.get("poc_pending") or {}).get("symbol"), "BTCUSDT")
        self.assertIn("POC_ORDER", events)

    # ---- 5. POC_ENABLED=0 pauses the book ----
    def paused_bot(self):
        os.environ["POC_ENABLED"] = "0"
        try:
            b, sent = self.make(self.new, tempfile.mkdtemp(dir=self.tmp), FakeData())
        finally:
            os.environ.pop("POC_ENABLED", None)
        return b, sent

    def test_pause_flag_values(self):
        for v, paused in (("0", True), ("1", False), ("", False), (None, False)):
            if v is None:
                os.environ.pop("POC_ENABLED", None)
            else:
                os.environ["POC_ENABLED"] = v
            try:
                self.assertEqual(self.new.make_cfg()["poc_paused"], paused, repr(v))
            finally:
                os.environ.pop("POC_ENABLED", None)

    def test_paused_no_orders_shown_paused_book_kept(self):
        import time as _t
        b, sent = self.paused_bot()
        b.startup()
        self.assertEqual(b.state["paused_books"], ["POC RETURN"])
        with open(os.path.join(b.cfg["data_dir"], "state.json")) as fh:
            self.assertIn('"paused_books"', fh.read())
        start = next(m for m in sent if m.startswith("Bot started"))
        self.assertRegex(start, r"POC RETURN: \$[0-9.]+ \(paused\)")
        self.assertIn("PAUSED (POC_ENABLED=0)", start)
        self.assertIn("POC RETURN", b.enabled_books())                 # still a book (balance kept), not "off"
        bal = b.book("POC RETURN")["balance"]
        now = _t.time()
        b.state["poc"] = {"made": b.last_us_close(now), "tiered": 2, "levels": {"BTCUSDT": {
            "poc": 100.0, "product_id": 1, "armed_long": True, "armed_short": False}}}
        b.in_us_session = lambda now: False
        b.data.tickers = lambda ct: [{"symbol": "BTCUSDT", "mark_price": 100.05}]      # would place an order
        events = []
        b.event = lambda typ, **kw: events.append(typ)
        b.manage_open_trades = lambda now: None
        b.session_tasks = lambda now: None
        b.handle_telegram = lambda: None
        b._poc_poll = 0
        for k in range(5):
            b.tick(now + 10 * k)
        self.assertIsNone(b.state.get("poc_pending"))
        self.assertEqual([e for e in events if e.startswith("POC")], [])
        self.assertIsNone(b.book("POC RETURN")["open_trade"])
        self.assertEqual(b.book("POC RETURN")["balance"], bal)
        st = b.status_text()
        self.assertIn("POC RETURN (paused): $", st)
        self.assertIn("POC RETURN: paused (POC_ENABLED=0)", st)
        self.assertNotIn("POC levels:", st)

    def test_paused_cancels_a_resting_order(self):
        import time as _t
        b, _ = self.paused_bot()
        b.startup()
        events = []
        b.event = lambda typ, **kw: events.append((typ, kw))
        b.state["poc_pending"] = {"symbol": "BTCUSDT", "side": "long", "product_id": 1, "price": 100.0,
                                  "placed": int(_t.time()), "trade": {}}
        b.poc_pause_step(_t.time())
        self.assertIsNone(b.state["poc_pending"])
        self.assertEqual(events[0][0], "POC_ORDER_CANCELLED")
        self.assertIn("paused", events[0][1]["reason"])

    def test_running_state_says_nothing_paused(self):
        (_, _), (bn, _) = self.bots()
        bn.startup()
        self.assertEqual(bn.state["paused_books"], [])


if __name__ == "__main__":
    unittest.main()
