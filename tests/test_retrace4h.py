"""4H RETRACE LONG / SHORT (retrace4h.py): the big-candle rule, then the live flow through the bot with a fake Binance
client: scan -> armed -> price back at the mid -> capped IOC entry (cap 0.1% past the mid) -> stop 1.5%, BE 2R."""
import csv
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import retrace4h as X  # noqa: E402
from tests.test_rsi1h import Runner, T0  # noqa: E402

P4 = 14400
BAR0 = T0 - T0 % P4


def candles(n=30, big=None, t0=BAR0):
    """n 4h candles with bodies of 1.0 (open 100 -> 101, range 100-101.2); `big` = (o, h, l, c) for the last one."""
    out = []
    for i in range(n):
        o, c = (100.0, 101.0) if i % 2 == 0 else (101.0, 100.0)
        out.append({"time": t0 + i * P4, "open": o, "high": 101.2, "low": 99.8, "close": c, "volume": 1})
    if big:
        o, h, lo, c = big
        out.append({"time": t0 + n * P4, "open": o, "high": h, "low": lo, "close": c, "volume": 1})
    return out


class Rules(unittest.TestCase):
    def test_big_green_candle(self):
        c = candles(big=(100.0, 103.1, 99.9, 103.0))          # body 3.0 > 2 x 1.0, 3.0 / 3.2 = 94% of the range
        s = X.setup_at(c, len(c) - 1)
        self.assertEqual((s["side"], s["mid"]), ("long", (103.1 + 99.9) / 2))
        self.assertAlmostEqual(s["body_x"], 3.0)

    def test_red_is_short(self):
        s = X.setup_at(candles(big=(103.0, 103.1, 99.9, 100.0)), 30)
        self.assertEqual(s["side"], "short")

    def test_small_body_or_long_wicks_rejected(self):
        self.assertIsNone(X.setup_at(candles(big=(100.0, 101.9, 99.9, 101.8)), 30))   # body 1.8 < 2 x 1.0
        self.assertIsNone(X.setup_at(candles(big=(100.0, 104.5, 99.5, 103.0)), 30))   # 3.0 / 5.0 = 60% < 80%

    def test_needs_24_candles_before(self):
        self.assertIsNone(X.setup_at(candles(n=20, big=(100.0, 103.1, 99.9, 103.0)), 20))

    def test_touched(self):
        self.assertTrue(X.touched("long", 101.5, 101.4, 102))
        self.assertFalse(X.touched("long", 101.5, 101.6, 102))
        self.assertTrue(X.touched("short", 101.5, 101.0, 101.5))


class Live(unittest.TestCase):
    SYM = "RTUSDT"
    ev = Runner.ev

    def setUp(self):
        Runner.setUp(self)
        self.x = self.b.retrace
        self.assertIsNotNone(self.x)
        self.x.ensure_books()
        self.fd.add(self.SYM, 9, 3_000_000, [])
        self.r.prods = self.r.universe()
        self.fd.depth_qty = 1e9
        self.fd.request = lambda m, path, params=None: (
            {"price": str(self.fd.px[params["symbol"]])} if params and params.get("symbol")
            else [{"symbol": s, "price": str(p)} for s, p in self.fd.px.items()])

    def scan(self, c4):
        """4h close right after the last candle in c4 (plus an optional forming candle)."""
        self.fd.c4 = {self.SYM: c4}
        orig = self.fd.candles
        self.fd.candles = lambda sym, res, count, now=None: (
            [x for x in self.fd.c4.get(sym, []) if now is None or x["time"] <= now] if res == "4h" else orig(sym, res, count, now))
        bar = c4[-1]["time"] + P4
        self.x.on_close(bar + 9, bar, self.x.fetch_all([self.SYM], bar))
        return bar

    def poll(self, now):
        self.b._all_px = None
        self.x.poll(now)

    def test_arm_enter_at_mid_stop_and_be(self):
        c = candles(big=(100.0, 103.1, 99.9, 103.0))
        bar = self.scan(c)
        key = f"{self.SYM}|long|{c[-1]['time']}"
        a = self.x.st["armed"][key]
        self.assertEqual(a["mid"], 101.5)
        self.assertEqual(a["expires"], c[-1]["time"] + P4 + 24 * 3600)
        self.assertEqual(self.ev("4H_SETUP")[-1]["mid"], 101.5)
        self.assertEqual(self.ev("4H_ARMED")[-1]["symbol"], self.SYM)
        self.fd.px[self.SYM] = 101.6
        self.poll(bar + 60)
        self.assertEqual(self.b.book_trades("4H RETRACE LONG"), [])          # not back at the mid yet
        self.fd.px[self.SYM] = 101.5
        self.poll(bar + 63)
        t = self.b.book("4H RETRACE LONG")["open_trade"]
        self.assertEqual((t["symbol"], t["side"], t["strategy"], t["book"]), (self.SYM, "long", "retrace4h", "4H RETRACE LONG"))
        self.assertAlmostEqual(t["stop"], t["entry"] * 0.985, delta=0.011)  # 1.5% stop
        self.assertAlmostEqual(t["risk0"], t["entry"] * 0.015)              # 1R = 1.5% of the entry
        self.assertEqual((t["be_r"], t["trail_start_r"]), (2, 3))
        self.assertAlmostEqual(t["exec"]["entry_cap"], 101.6)                 # 0.1% past the mid (101.5 x 1.001)
        self.assertNotIn(key, self.x.st["armed"])
        self.assertIn("ENTERED [4H RETRACE LONG]", self.sent[-1])
        with open(os.path.join(self.d, "candidates.csv"), newline="") as fh:
            row = list(csv.DictReader(fh))[-1]
        self.assertEqual((row["book"], row["taken"], row["level"]), ("4H RETRACE LONG", "yes", "101.5"))
        # BE at 2R (not 1R): 1.5R up keeps the stop, 2R moves it to the entry
        e, r = t["entry"], t["risk0"]
        self.fd.m1 = {self.SYM: [{"time": bar + 120, "open": e, "high": e + 1.5 * r, "low": e, "close": e + 1.5 * r, "volume": 1}]}
        t["last_checked"] = bar + 120
        self.assertIsNone(self.b.check_exit_by_candles(t, bar + 185))
        self.assertFalse(t["be_moved"])
        self.fd.m1[self.SYM].append({"time": bar + 180, "open": e, "high": e + 2.05 * r, "low": e + r, "close": e + 2 * r, "volume": 1})
        self.b.check_exit_by_candles(t, bar + 245)
        self.assertTrue(t["be_moved"])
        self.assertIn("Profit reached 2R", next(m for m in self.sent if m.startswith("STOP MOVED")))

    def test_short_book_and_separate_margin(self):
        c = candles(big=(103.0, 103.1, 99.9, 100.0))
        bar = self.scan(c)
        self.b.book("4H RETRACE LONG")["open_trade"] = {"symbol": "XUSDT", "product_id": 99, "side": "long", "margin": 100.0}
        self.fd.px[self.SYM] = 101.55
        self.poll(bar + 60)
        t = self.b.book("4H RETRACE SHORT")["open_trade"]
        self.assertEqual((t["side"], t["book"]), ("short", "4H RETRACE SHORT"))
        self.assertAlmostEqual(t["stop"], t["entry"] * 1.015, delta=0.011)

    def test_beyond_cap_is_retried_not_lost(self):
        c = candles(big=(100.0, 103.1, 99.9, 103.0))
        bar = self.scan(c)
        self.fd.px[self.SYM] = 101.5
        self.fd.books = {self.SYM: ([(101.0, 1e9)], [(101.9, 1e9)])}         # ask 101.9 > cap 101.6
        self.poll(bar + 60)
        self.assertEqual(self.b.book_trades("4H RETRACE LONG"), [])
        self.assertEqual(self.ev("4H_SKIP")[-1]["reason"][:8], "best ask")
        self.assertEqual(len(self.x.st["armed"]), 1)                         # still armed, retried after 60 s
        del self.fd.books[self.SYM]
        self.poll(bar + 70)
        self.assertEqual(self.b.book_trades("4H RETRACE LONG"), [])          # retry cool-down
        self.poll(bar + 121)
        self.assertEqual(len(self.b.book_trades("4H RETRACE LONG")), 1)

    def test_expiry(self):
        c = candles(big=(100.0, 103.1, 99.9, 103.0))
        bar = self.scan(c)
        self.x.expire(bar + 24 * 3600 + 1)
        self.assertEqual(self.x.st["armed"], {})
        self.assertIn("expired", self.ev("4H_CANCEL")[-1]["reason"])

    def test_touched_before_watching_is_cancelled(self):
        c = candles(big=(100.0, 103.1, 99.9, 103.0))
        c.append({"time": c[-1]["time"] + P4, "open": 103.0, "high": 103.2, "low": 101.4, "close": 102.5, "volume": 1})
        self.scan(c)                                                       # restart one candle later: mid traded
        self.assertEqual(self.x.st["armed"], {})
        self.assertIn("before it was watched", self.ev("4H_CANCEL")[-1]["reason"])

    def test_no_price_watch_before_the_first_scan(self):
        self.x.st["armed"]["RTUSDT|long|1"] = {"symbol": self.SYM, "side": "long", "candle": 1, "mid": 101.5,
                                               "expires": 10 ** 12, "next_try": 0, "vol24h": 1, "open": 100.0, "high": 103.1,
                                               "low": 99.9, "close": 103.0, "body_x": 3.0, "body_share": 0.94}
        self.fd.px[self.SYM] = 101.0
        self.x.st["last_bar"] = BAR0 + 3 * P4               # no new 4h close: no scan, so not ready
        self.x.tick(BAR0 + 3 * P4 + 20)
        self.assertFalse(self.x.ready)
        self.assertEqual(self.b.book_trades("4H RETRACE LONG"), [])    # stale armed setups are not traded
        self.x.ready = True
        self.b._all_px = None
        self.x.tick(BAR0 + 3 * P4 + 30)
        self.assertEqual(len(self.b.book_trades("4H RETRACE LONG")), 1)

    def test_no_setup_no_arm(self):
        self.scan(candles())
        self.assertEqual(self.x.st["armed"], {})


if __name__ == "__main__":
    unittest.main()
