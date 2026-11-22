"""POC (and every other existing strategy) must behave exactly as before BOOK JUMP (commit POC_BASELINE).
1. AST: every function/method in bot.py is identical to the baseline's, except the five hook points listed below.
2. Behaviour: with ENABLE_BOOKJUMP=0 the hook points produce identical output (config, /status, summaries,
   start-up Telegram message) to the baseline code on the same state."""
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
HOOKS = {"make_cfg", "Bot.__init__", "Bot.status_text", "Bot.report", "Bot.startup",
         "Bot.init_books", "Bot.sweep_retired", "Bot.wallet_line"}   # last three: skip books not in BOOKS
ADDED = {"Bot.bookjump_setup"}
POC_BASELINE = "7b88317"           # last commit before BOOK JUMP (bot.py as deployed 2026-11-22 08:32 UTC)
ENV = {"EXCHANGE": "binance", "DRY_RUN": "1", "ENABLE_POC": "1", "ENABLE_SHORT": "0", "ENABLE_LONG": "0",
       "ENABLE_BREAKOUT": "0", "POC_SKIP_US_SESSION": "1", "START_CAPITAL_INR": "11890", "USD_INR_RATE": "85"}


def main_source():
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


OLD_SRC = main_source()


@unittest.skipIf(OLD_SRC is None, "git / baseline commit not available")
class PocUnchanged(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.saved_env = {k: os.environ.get(k) for k in list(ENV) + ["ENABLE_BOOKJUMP"]}
        os.environ.update(ENV)
        os.environ.pop("ENABLE_BOOKJUMP", None)
        cls.tmp = tempfile.mkdtemp()
        p = os.path.join(cls.tmp, "bot_main.py")
        with open(p, "w") as fh:
            fh.write(OLD_SRC)
        spec = importlib.util.spec_from_file_location("bot_main", p)
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
        for name in ("Bot.tick_poc", "Bot.poc_pending_step", "Bot.poc_filled", "Bot.manage_poc_live",
                     "Bot.close_trade", "Bot.session_tasks", "Bot.in_us_session", "Bot.tick", "Bot.run"):
            self.assertEqual(old_f[name], new_f[name], name)

    def make(self, mod, data_dir):
        cfg = mod.make_cfg()
        cfg["data_dir"] = data_dir
        b = mod.Bot(cfg)
        b.state["exchange"] = "binance"                     # like the live state.json (not a first-ever start)
        sent = []
        b.tg.send = sent.append
        b.get_products = lambda force=False: [{"id": 1, "symbol": "BTCUSDT", "tick": 0.1, "cv": 0.001,
                                               "type": "perpetual_futures", "stock_token": False}]
        return b, sent

    def bots(self):
        d1, d2 = tempfile.mkdtemp(dir=self.tmp), tempfile.mkdtemp(dir=self.tmp)
        return self.make(self.old, d1), self.make(self.new, d2)

    def test_config_identical_apart_from_switch(self):
        old, new = self.old.make_cfg(), self.new.make_cfg()
        self.assertEqual(new.pop("enable_bookjump"), False)
        old.pop("data_dir"), new.pop("data_dir")            # each module's own folder
        self.assertEqual(old, new)

    def test_startup_status_summary_identical(self):
        (bo, so), (bn, sn) = self.bots()
        bo.startup()
        bn.startup()
        self.assertIsNone(bn.bj)
        self.assertEqual(so, sn)                            # start-up Telegram message(s)
        for b in (bo, bn):
            bk = b.state["books"]["POC RETURN"]
            bk["balance"] = 141.25
            bk["day"]["2026-11-20"] = {"trades": 2, "pnl": 1.36, "start_equity": 139.89, "notified": False}
            b.state["poc"] = {"made": 1763672400, "levels": {"ETHUSDT": {"poc": 3001.5, "product_id": 2,
                                                                         "armed_long": True, "armed_short": False}}}
        self.assertEqual(bo.status_text(), bn.status_text())
        self.assertEqual(bo.report(), bn.report())
        self.assertEqual(bo.report("2026-11-20"), bn.report("2026-11-20"))

    def test_refused_when_live(self):
        os.environ["ENABLE_BOOKJUMP"] = "1"
        os.environ["DRY_RUN"] = "0"
        try:
            cfg = self.new.make_cfg()
            cfg["data_dir"] = tempfile.mkdtemp(dir=self.tmp)
            b = self.new.Bot(cfg)
            b.event = lambda *a, **k: None
            line = b.bookjump_setup()
            self.assertIsNone(b.bj)
            self.assertIn("NOT started", line)
            self.assertIn("DRY_RUN is off", line)
        finally:
            os.environ.pop("ENABLE_BOOKJUMP")
            os.environ["DRY_RUN"] = "1"

    def test_bookjump_state_is_separate(self):
        """With BOOK JUMP on, the POC book / state.json are not touched by BOOK JUMP."""
        os.environ["ENABLE_BOOKJUMP"] = "1"
        try:
            (bo, so), (bn, sn) = self.bots()
            import bookjump
            bookjump.BookJump.refresh_universe = lambda self: ["SOLUSDT"]    # no network in tests
            bookjump.BookJump.start = lambda self: None
            bo.startup()
            bn.startup()
            self.assertIsNotNone(bn.bj)
            bj_book = bn.state["books"]["BOOK JUMP"]            # stored in state.json like the other books
            self.assertEqual((bj_book["balance"], bj_book["start"], bj_book["open_trade"]), (100.0, 100.0, None))
            self.assertEqual(bo.state["books"]["POC RETURN"], bn.state["books"]["POC RETURN"])
            lines = sn[-1].split("\n")
            bj_line = [x for x in lines if x.startswith("BOOK JUMP [PAPER]: ON")]
            self.assertEqual(len(bj_line), 1)
            self.assertEqual(so[-1], "\n".join(x for x in lines if x not in bj_line))   # only that line added
            self.assertIn("BOOK JUMP", bn.status_text())
            self.assertIn("BOOK JUMP (own paper book, not in ALL)", bn.report())
        finally:
            os.environ.pop("ENABLE_BOOKJUMP")

    # ---- the BOOK JUMP book sits in state["books"], but the bot's book logic must ignore it ----
    def poc_state(self, b):
        b.state["books"] = {"POC RETURN": {"start": 120.0, "balance": 120.0, "open_trade": None, "day": {},
                                           "month": {}, "trades": 3, "wins": 1}}
        b.state["swept_to_wallet"] = True
        b.state["books_since"] = "2026-11-01T00:00:00+05:30"

    def test_sweep_and_init_same_as_baseline(self):
        (bo, so), (bn, sn) = self.bots()
        for b in (bo, bn):
            self.poc_state(b)
            b.state["books"]["1H BREAKOUT"] = {"start": 7.0, "balance": 7.0, "open_trade": None, "day": {},
                                               "month": {}, "trades": 0, "wins": 0}
            b.sweep_retired()                               # retired breakout balance moves into POC
            b.init_books()
        self.assertEqual(bo.state["books"], bn.state["books"])
        self.assertEqual(so, sn)

    def test_bookjump_book_never_swept_or_donor(self):
        (bo, so), (bn, sn) = self.bots()
        import bookjump
        for b in (bo, bn):
            self.poc_state(b)
        bn.state["books"]["BOOK JUMP"] = bookjump.new_state(100.0)
        bn.sweep_retired()
        bo.sweep_retired()
        self.assertEqual(bn.state["books"]["BOOK JUMP"]["balance"], 100.0)
        self.assertEqual(bn.state["books"]["POC RETURN"], bo.state["books"]["POC RETURN"])
        self.assertEqual(so, sn)                            # no "moved into" message
        os.environ["ENABLE_BREAKOUT"] = "1"                 # a newly enabled strategy gets funded...
        try:
            for b in (bo, bn):
                b.cfg["enable_breakout"] = True
                b.init_books()
            self.assertEqual(bn.state["books"]["BOOK JUMP"]["balance"], 100.0)   # ...never from BOOK JUMP
            self.assertEqual(bn.state["books"]["1H BREAKOUT"], bo.state["books"]["1H BREAKOUT"])
            self.assertEqual(bn.state["books"]["POC RETURN"], bo.state["books"]["POC RETURN"])
        finally:
            os.environ["ENABLE_BREAKOUT"] = "0"
        self.assertNotIn("BOOK JUMP", bn.book_names())     # not in POC's status/summary loops


if __name__ == "__main__":
    unittest.main()
