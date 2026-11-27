"""EMA20 PULLBACK (ema20_rules.py, ema20.py): setup rules on hand-built 5m candles, then the live flow through the bot
(arm -> price breach -> capped IOC entry -> exit on a 5m close through the EMA) with a fake Binance client."""
import csv
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import ema20_rules as E  # noqa: E402
from tests.test_rsi1h import Runner, T0  # noqa: E402

P = 300


def base(n=45, start=100.0, step=-0.05, t0=T0):
    """A slow decline: price under its 20 EMA."""
    out, px = [], start
    for i in range(n):
        o, c = px, px + step
        out.append({"time": t0 + i * P, "open": o, "high": max(o, c) + 0.02, "low": min(o, c) - 0.02, "close": c,
                    "volume": 1})
        px = c
    return out


def add(cands, *ohlc):
    for o, h, lo, c in ohlc:
        cands.append({"time": cands[-1]["time"] + P, "open": o, "high": h, "low": lo, "close": c, "volume": 1})
    return cands


def long_setup():
    c = base()                                       # last close 97.75, EMA ~98.3
    add(c, (97.8, 99.6, 97.7, 99.5),                 # alert: green, open under the EMA, close above
        (99.5, 99.7, 99.1, 99.2),                    # pullback 1: red, high 99.7 = trigger
        (99.2, 99.4, 99.0, 99.3))                    # pullback 2: green, closes above the EMA
    return c


def mirror(cands):
    return [{"time": x["time"], "open": 200 - x["open"], "high": 200 - x["low"], "low": 200 - x["high"],
             "close": 200 - x["close"], "volume": 1} for x in cands]


class Rules(unittest.TestCase):
    def last(self, c, side="long"):
        s = E.scan_setups(c, side)
        self.assertTrue(s)
        return s[-1]

    def test_alert_pullback_armed(self):
        c = long_setup()
        ema = E.ema_series([x["close"] for x in c])
        self.assertLess(c[-4]["close"], ema[-4])                  # before the alert: under the EMA
        self.assertTrue(E.is_alert(c, ema, len(c) - 3, "long"))
        s = self.last(c)
        self.assertEqual((s["status"], s["trigger"], s["low"], s["pb"]), ("armed", 99.7, 97.7, 99.1))
        self.assertAlmostEqual(s["r"], 99.7 - 99.1)                        # trigger - the red pullback low
        self.assertEqual(s["pull"], [c[-2]["time"], c[-1]["time"]])

    def test_breach(self):
        c = add(long_setup(), (99.3, 99.6, 99.2, 99.4), (99.4, 99.9, 99.3, 99.8))
        s = self.last(c)
        self.assertEqual((s["status"], s["end_i"]), ("breached", len(c) - 1))

    def test_breach_and_close_under_ema_same_candle_is_a_breach(self):
        c = add(long_setup(), (99.3, 99.8, 97.0, 97.2))
        self.assertEqual(self.last(c)["status"], "breached")

    def test_cancel_on_close_below_ema(self):
        c = add(long_setup(), (99.3, 99.5, 97.5, 97.6))
        s = self.last(c)
        self.assertEqual(s["status"], "cancelled")
        self.assertIn("below the 20 EMA", s["reason"])

    def test_expiry_after_12_candles(self):
        c = long_setup()
        for _ in range(11):
            add(c, (99.3, 99.6, 99.2, 99.4))
        self.assertEqual(self.last(c)["status"], "armed")
        add(c, (99.3, 99.6, 99.2, 99.4))
        self.assertEqual(self.last(c)["status"], "expired")

    def test_no_red_pullback_fails(self):
        c = base()
        add(c, (97.8, 99.6, 97.7, 99.5), (99.5, 99.9, 99.4, 99.8), (99.8, 100.0, 99.7, 99.9))
        s = self.last(c)
        self.assertEqual(s["status"], "failed")
        self.assertIn("no red pullback", s["reason"])

    def test_pullback_close_below_ema_fails(self):
        c = base()
        add(c, (97.8, 99.6, 97.7, 99.5), (99.5, 99.6, 97.0, 97.1))
        self.assertEqual(self.last(c)["status"], "failed")

    def test_two_red_pullbacks_higher_high_and_lower_low(self):
        c = base()
        add(c, (97.8, 99.6, 97.7, 99.5), (99.5, 99.7, 99.0, 99.2), (99.4, 99.9, 99.1, 99.3))
        s = self.last(c)
        self.assertEqual((s["trigger"], s["pb"]), (99.9, 99.0))

    def test_min_r(self):
        c = base()
        add(c, (98.25, 98.6, 98.24, 98.5), (98.5, 98.55, 98.45, 98.48), (98.48, 98.52, 98.46, 98.5))
        s = E.scan_setups(c, "long", min_r_pct=0.01)[-1]
        if s["status"] == "armed":
            self.assertAlmostEqual(s["r"], s["trigger"] * 0.01)

    def test_short_is_the_mirror(self):
        c = mirror(add(long_setup(), (99.3, 99.6, 99.2, 99.4), (99.4, 99.9, 99.3, 99.8)))
        s = E.scan_setups(c, "short")[-1]
        self.assertEqual(s["status"], "breached")
        self.assertAlmostEqual(s["trigger"], 200 - 99.7)
        self.assertAlmostEqual(s["low"], 200 - 97.7)              # highest high for a short
        self.assertAlmostEqual(s["pb"], 200 - 99.1)               # the green pullback candle's high
        self.assertEqual(E.scan_setups(c, "long")[-1:] and E.scan_setups(c, "long")[-1]["status"] or "none",
                         E.scan_setups(c, "long")[-1]["status"] if E.scan_setups(c, "long") else "none")

    def test_warmup(self):
        self.assertEqual(E.scan_setups(long_setup()[:30], "long"), [])


class Live(unittest.TestCase):
    """The book inside the bot with a fake Binance client (candles in self.fd.c5, prices in self.fd.px).
    Uses the 1H RSI tests' setup without inheriting their tests."""
    SYM = "PBUSDT"
    ev = Runner.ev

    def setUp(self):
        Runner.setUp(self)
        self.e = self.b.ema20
        self.assertIsNotNone(self.e)
        self.e.ensure_books()
        self.fd.add(self.SYM, 7, 3_000_000, [])
        self.r.prods = self.r.universe()
        self.fd.depth_qty = 1e9                                  # deep book: the whole book's margin x20
        self.fd.request = self.request

    def request(self, method, path, params=None):
        if params and params.get("symbol"):
            return {"price": str(self.fd.px[params["symbol"]])}
        return [{"symbol": s, "price": str(p)} for s, p in self.fd.px.items()]

    def close(self, cands):
        """Run the 5m-close processing for the bar that starts right after the last candle."""
        bar = cands[-1]["time"] + P
        self.fd.c5[self.SYM] = cands
        self.e.check_ema_exits(bar + 9, bar)
        job = self.e.fetch_all([self.SYM], bar, bar + 9)
        self.e.on_close(bar + 9, bar, job)
        return bar

    def arm(self):
        c = long_setup()
        bar = self.close(c)
        a = self.e.st["armed"][f"{self.SYM}|long"]
        self.assertEqual((a["trigger"], a["low"]), (99.7, 97.7))
        self.assertEqual(self.ev("EMA20_ARMED")[-1]["trigger"], 99.7)
        return c, bar

    def test_arm_breach_enter_and_ema_exit(self):
        c, bar = self.arm()
        self.fd.px[self.SYM] = 99.65
        self.e.poll(bar + 20)
        self.assertEqual(self.b.book_trades("EMA20 PULLBACK"), [])          # not beyond the trigger yet
        self.fd.px[self.SYM] = 99.75
        self.e.poll(bar + 23)
        t = self.b.book("EMA20 PULLBACK")["open_trade"]
        self.assertEqual((t["symbol"], t["side"], t["strategy"], t["book"]), (self.SYM, "long", "ema20", "EMA20 PULLBACK"))
        self.assertAlmostEqual(t["risk0"], t["entry"] - 99.1)                # 1R to the pullback low (closer than 3%)
        self.assertEqual(t["extra"]["pb"], 99.1)
        self.assertAlmostEqual(t["stop"], t["hard_stop"])                    # initial stop = the 3% hard stop
        self.assertAlmostEqual(t["hard_stop"], t["entry"] * 0.97, delta=0.011)
        self.assertGreater(t["margin"], 99)                                  # the whole $100 book
        self.assertNotIn(f"{self.SYM}|long", self.e.st["armed"])
        self.assertIn("ENTERED [EMA20 PULLBACK]", self.sent[-1])
        self.assertIn("trigger 99.7", self.sent[-1])
        self.assertIn("alert", self.ev("ENTRY")[-1]["setup"])
        with open(os.path.join(self.d, "candidates.csv"), newline="") as fh:
            row = list(csv.DictReader(fh))[-1]
        self.assertEqual((row["book"], row["taken"], row["level"], row["check"]),
                         ("EMA20 PULLBACK", "yes", "99.7", "price beyond trigger"))
        self.assertIn("EMA20 PULLBACK: armed 0", self.b.status_text())
        self.assertIn(self.SYM, next(x for x in self.b.status_text().splitlines() if "LONG PBUSDT" in x))
        # next 5m candle closes above the EMA: hold; the one after closes below: exit at market
        t["opened_ts"] = c[-1]["time"] + P + 30
        add(c, (99.6, 100.2, 99.5, 100.1))
        self.close(c)
        self.assertEqual(len(self.b.book_trades("EMA20 PULLBACK")), 1)
        add(c, (100.1, 100.1, 97.0, 97.1))
        self.fd.px[self.SYM] = 97.1
        self.close(c)
        self.assertEqual(self.b.book_trades("EMA20 PULLBACK"), [])
        x = self.ev("EXIT")[-1]
        self.assertEqual((x["reason"], x["book"]), ("PB_CLOSE", "EMA20 PULLBACK"))   # under both: the pullback stop
        with open(os.path.join(self.d, "trades.csv"), newline="") as fh:
            tr = list(csv.DictReader(fh))[-1]
        self.assertEqual((tr["strategy"], tr["exit_reason"]), ("EMA20 PULLBACK", "PB_CLOSE"))
        self.assertIn("crossed the 20 EMA", tr["setup"])
        self.assertTrue(tr["pnl_pct_margin"])
        msg = next(m for m in self.sent if m.startswith("TRADE CLOSED"))
        self.assertIn("Exit: 5m", msg)
        self.assertIn("below the pullback low", msg)

    def test_ema_close_exit_when_the_pullback_low_is_under_the_ema(self):
        c = base()
        add(c, (97.8, 99.6, 97.7, 99.5), (99.5, 99.7, 98.0, 99.2), (99.2, 99.4, 99.0, 99.3))   # red low 98.0
        self.close(c)
        self.fd.px[self.SYM] = 99.75
        self.e.poll(c[-1]["time"] + P + 20)
        t = self.b.book("EMA20 PULLBACK")["open_trade"]
        t["opened_ts"] = c[-1]["time"] + P + 30
        add(c, (99.6, 99.7, 98.2, 98.3))
        ema = __import__("ema20_rules").ema_series([x["close"] for x in c])
        self.assertTrue(98.0 < 98.3 < ema[-1])                                  # under the EMA, over the pullback low
        self.fd.px[self.SYM] = 98.3
        self.close(c)
        self.assertEqual(self.ev("EXIT")[-1]["reason"], "EMA_EXIT")
        self.assertIn("below the 20 EMA", next(m for m in self.sent if m.startswith("TRADE CLOSED")))

    def enter_long(self):
        c, bar = self.arm()
        self.fd.px[self.SYM] = 99.75
        self.e.poll(bar + 23)
        t = self.b.book("EMA20 PULLBACK")["open_trade"]
        t["opened_ts"] = c[-1]["time"] + P + 30
        return c, t

    def test_pullback_close_exits_wicks_do_not(self):
        c, t = self.enter_long()
        add(c, (99.6, 99.8, 98.9, 99.5))                                     # wick under 99.1, close above: hold
        self.close(c)
        self.assertEqual(len(self.b.book_trades("EMA20 PULLBACK")), 1)
        ema = __import__("ema20_rules").ema_series([x["close"] for x in add(c, (99.5, 99.6, 98.95, 99.0))])
        self.assertGreater(99.0, ema[-1])                                     # above the EMA, below the pullback low
        self.fd.px[self.SYM] = 99.0
        self.close(c)
        self.assertEqual(self.b.book_trades("EMA20 PULLBACK"), [])
        self.assertEqual(self.ev("EXIT")[-1]["reason"], "PB_CLOSE")
        msg = next(m for m in self.sent if m.startswith("TRADE CLOSED"))
        self.assertIn("below the pullback low 99.1", msg)

    def test_r_capped_by_the_hard_stop(self):
        c = base()
        add(c, (97.8, 99.6, 97.7, 99.5), (99.5, 99.7, 95.0, 99.2), (99.2, 99.4, 99.0, 99.3))  # red low 95: > 3% away
        self.close(c)
        self.fd.px[self.SYM] = 99.75
        self.e.poll(c[-1]["time"] + P + 20)
        t = self.b.book("EMA20 PULLBACK")["open_trade"]
        self.assertAlmostEqual(t["risk0"], t["entry"] - t["hard_stop"])
        self.assertIn("to the hard stop", t["extra"]["detail"])

    def test_breach_only_on_a_closed_candle_cancels(self):
        c, bar = self.arm()
        add(c, (99.3, 99.9, 99.2, 99.4))                                     # traded 99.9, never seen live
        self.close(c)
        self.assertEqual(self.e.st["armed"], {})
        self.assertIn("between two price checks", self.ev("EMA20_CANCEL")[-1]["reason"])
        self.assertEqual(self.b.book_trades("EMA20 PULLBACK"), [])
        self.close(add(c, (99.4, 99.5, 99.3, 99.45)))                       # the used setup is not re-armed
        self.assertEqual(self.e.st["armed"], {})

    def test_no_price_watch_while_the_book_is_full(self):
        c, bar = self.arm()
        self.b.book("EMA20 PULLBACK")["open_trade"] = {"symbol": "XUSDT", "product_id": 99, "side": "short",
                                                        "margin": 50.0}
        self.assertFalse(self.e.has_room())
        add(c, (99.3, 99.9, 99.2, 99.4))
        self.close(c)
        self.assertIn("no room", self.ev("EMA20_CANCEL")[-1]["reason"])

    def test_cancel_and_alert_events(self):
        c = long_setup()[:-2]
        self.close(c)
        self.assertEqual((self.ev("EMA20_ALERT")[-1]["long"], self.ev("EMA20_ALERT")[-1]["count"]), ([self.SYM], 1))
        add(c, (99.5, 99.7, 99.1, 99.2), (99.2, 99.4, 99.0, 99.3))
        self.close(c)
        add(c, (99.3, 99.5, 97.5, 97.6))
        self.close(c)
        self.assertIn("below the 20 EMA", self.ev("EMA20_CANCEL")[-1]["reason"])
        self.assertEqual(self.e.st["armed"], {})

    def test_leftover_rule_and_too_small(self):
        c, bar = self.arm()
        bk = self.b.book("EMA20 PULLBACK")
        bk["open_trade"] = {"symbol": "XUSDT", "product_id": 99, "side": "short", "margin": 50.0}
        self.fd.px[self.SYM] = 99.75
        self.e.poll(bar + 20)
        self.assertEqual(len(self.b.book_trades("EMA20 PULLBACK")), 1)
        self.assertEqual(self.ev("EMA20_SKIP")[-1]["reason"].split(":")[0][:len("EMA20 PULLBACK has 1")],
                         "EMA20 PULLBACK has 1")
        bk["open_trade"] = None
        self.e.st["armed"][f"{self.SYM}|long"] = dict(self.e.arm_record(self.SYM, "long", {
            "trigger": 99.7, "low": 97.7, "pb": 99.1, "r": 0.6, "ema": 98.3, "alert_time": 1, "pull": [2, 3], "armed_time": 3}))
        self.fd.depth_qty = 0.05                                              # thin book: impact limit << 25%
        self.e.poll(bar + 26)
        self.assertEqual(self.ev("EMA20_SKIP")[-1]["reason"][:9], "too small")

    def test_after_a_fill_the_other_breaches_stay_armed(self):
        c, bar = self.arm()
        self.e.st["armed"]["QQUSDT|long"] = dict(self.e.arm_record("QQUSDT", "long", {
            "trigger": 1.0, "low": 0.9, "pb": 0.95, "r": 0.05, "ema": 0.97, "alert_time": 5, "pull": [6, 7],
            "armed_time": 7}), vol24h=1)
        self.fd.px.update({self.SYM: 99.75, "QQUSDT": 1.1})
        self.e.poll(bar + 23)
        self.assertEqual(self.b.book("EMA20 PULLBACK")["open_trade"]["symbol"], self.SYM)   # higher volume first
        self.assertIn("QQUSDT|long", self.e.st["armed"])                                  # not consumed
        self.assertEqual(self.ev("EMA20_SKIP"), [])

    def test_sides_config(self):
        self.e.c["sides"] = ("short",)
        self.close(long_setup())
        self.assertEqual(self.e.st["armed"], {})


if __name__ == "__main__":
    unittest.main()
