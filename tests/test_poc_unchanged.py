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
         "Binance.request"}
ADDED = set()
POC_FUNCS = ("Bot.tick_poc", "Bot.poc_pending_step", "Bot.poc_filled", "Bot.poc_place_limit", "Bot.poc_cancel",
             "Bot.poc_take_at_poc", "Bot.build_pocs", "Bot.poc_tier1", "Bot.manage_poc_live", "Bot.touch_stop_price",
             "Bot.set_touch_stop", "Bot.apply_trail", "Bot.apply_be", "Bot.try_enter", "Bot.entry_allowed",
             "Bot.init_books", "Bot.sweep_retired", "Bot.session_tasks", "Bot.report", "Bot.in_us_session",
             "Bot.manage_open_trade", "Bot.run", "poc_price", "size_trade", "size_by_margin")
ENV = {"EXCHANGE": "binance", "DRY_RUN": "1", "ENABLE_POC": "1", "ENABLE_SHORT": "0", "ENABLE_LONG": "0",
       "ENABLE_BREAKOUT": "0", "POC_SKIP_US_SESSION": "1", "START_CAPITAL_INR": "11890", "USD_INR_RATE": "85",
       "RSI1H_ENABLED": "0"}
CLEAR = ("ENABLE_BOOKJUMP", "BJ_ENABLED")
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


if __name__ == "__main__":
    unittest.main()
