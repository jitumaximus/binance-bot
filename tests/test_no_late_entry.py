"""No late entries (28 Nov 2026), every book and side, through the bot with the fake Binance client:
- the level trades while the book is full -> no entry (the setup is cancelled / the signal skipped);
- price 0.05% past the level -> entry; price 0.5% past the level -> cancelled / skipped;
- 1H RSI: latest closed 1H RSI 5 (long) / 95 (short) -> skipped; candle stop more than 2% away -> skipped.
Every cancel / skip is a candidates.csv row with taken=no, the reason, the level, the LTP and the distance %."""
import csv
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import rsi1h as R  # noqa: E402
from tests import test_ema20 as TE  # noqa: E402
from tests import test_retrace4h as TX  # noqa: E402
from tests import test_rsi1h as TR  # noqa: E402

T0, find_signal = TR.T0, TR.find_signal

FULL = "level traded while the book was full: no late entry"


def rows(d):
    p = os.path.join(d, "candidates.csv")
    if not os.path.exists(p):
        return []
    with open(p, newline="") as fh:
        return list(csv.DictReader(fh))


def fill_book(b, book):
    b.book(book)["open_trade"] = {"symbol": "XUSDT", "product_id": 999, "side": "long", "margin": b.book(book)["balance"]}


# ---------------------------------------------------------------- 4H RETRACE: band = mid x (1 -/+ 0.1%)
class Retrace(TX.Live):
    LONG = (100.0, 103.1, 99.9, 103.0)              # green 4h candle, mid 101.5
    SHORT = (103.0, 103.1, 99.9, 100.0)             # red 4h candle, mid 101.5

    def go(self, side, move_pct, full=False):
        bar = self.scan(TX.candles(big=self.LONG if side == "long" else self.SHORT))
        book = "4H RETRACE LONG" if side == "long" else "4H RETRACE SHORT"
        if full:
            fill_book(self.b, book)
        past = -1 if side == "long" else 1          # a long comes down to the mid: "past" is below it
        self.fd.px[self.SYM] = round(101.5 * (1 + past * move_pct / 100), 6)
        self.poll(bar + 60)
        return book

    def check_cancel(self, book, text):
        self.assertEqual(self.x.st["armed"], {})
        self.assertIn(text, self.ev("4H_CANCEL")[-1]["reason"])
        r = rows(self.d)[-1]
        self.assertEqual((r["book"], r["taken"], r["level"]), (book, "no", "101.5"))
        self.assertIn(text, r["reason"])
        self.assertTrue(r["ltp"] and r["distance_pct"])

    def test_long_full_book(self):
        book = self.go("long", 0.0, full=True)
        self.assertEqual(len(self.b.book_trades(book)), 1)          # only the filler
        self.check_cancel(book, FULL)

    def test_short_full_book(self):
        book = self.go("short", 0.0, full=True)
        self.assertEqual(len(self.b.book_trades(book)), 1)
        self.check_cancel(book, FULL)

    def test_long_005_enters_05_cancels(self):
        book = self.go("long", 0.05)
        self.assertEqual(self.b.book_trades(book)[0]["symbol"], self.SYM)
        self.setUp()
        book = self.go("long", 0.5)
        self.assertEqual(self.b.book_trades(book), [])
        self.check_cancel(book, "price past the cap: no late entry")

    def test_short_005_enters_05_cancels(self):
        book = self.go("short", 0.05)
        self.assertEqual(self.b.book_trades(book)[0]["symbol"], self.SYM)
        self.setUp()
        book = self.go("short", 0.5)
        self.assertEqual(self.b.book_trades(book), [])
        self.check_cancel(book, "price past the cap: no late entry")

    def test_ltc_example(self):
        """LTC 28 Nov: mid 44.575, bought at 43.99 (1.3% below) 4 s after the book freed up. Now: cancelled."""
        bar = self.scan(TX.candles(big=self.LONG))
        fill_book(self.b, "4H RETRACE LONG")
        self.fd.px[self.SYM] = 101.0                              # the mid trades while the book is full
        self.poll(bar + 60)
        self.b.book("4H RETRACE LONG")["open_trade"] = None       # the book frees up ...
        self.fd.px[self.SYM] = 101.5 * (1 - 0.013)
        self.poll(bar + 64)                                       # ... and nothing is bought late
        self.assertEqual(self.b.book_trades("4H RETRACE LONG"), [])
        self.assertIn(FULL, self.ev("4H_CANCEL")[-1]["reason"])


# ---------------------------------------------------------------- EMA20: long [trigger, x1.001], short [x0.999, trigger]
class Ema20(TE.Live):
    def arm_side(self, side):
        if side == "long":
            c = TE.long_setup()
            bar = self.close(c)
            return bar, 99.7, "EMA20 LONG"
        self.fd.add("SHUSDT", 8, 3_000_000, [])
        self.r.prods = self.r.universe()
        self.SYM = "SHUSDT"
        c = TE.mirror(TE.long_setup())
        bar = self.close(c)
        a = self.e.st["armed"]["SHUSDT|short"]
        self.assertAlmostEqual(a["trigger"], 100.3)
        return bar, a["trigger"], "EMA20 SHORT"

    def go(self, side, move_pct, full=False):
        bar, trig, book = self.arm_side(side)
        if full:
            fill_book(self.b, book)
        d = 1 if side == "long" else -1
        self.fd.px[self.SYM] = round(trig * (1 + d * move_pct / 100), 6)
        self.poll(bar + 20)
        return book, trig

    def check_cancel(self, book, trig, text):
        self.assertEqual(self.e.st["armed"], {})
        self.assertIn(text, self.ev("EMA20_CANCEL")[-1]["reason"])
        r = rows(self.d)[-1]
        self.assertEqual((r["book"], r["taken"], r["check"]), (book, "no", "cancel"))
        self.assertAlmostEqual(float(r["level"]), trig)
        self.assertIn(text, r["reason"])
        self.assertTrue(r["ltp"] and r["distance_pct"])

    def test_long_full_book(self):
        book, trig = self.go("long", 0.05, full=True)
        self.assertEqual(len(self.b.book_trades(book)), 1)
        self.check_cancel(book, trig, FULL)

    def test_short_full_book(self):
        book, trig = self.go("short", 0.05, full=True)
        self.assertEqual(len(self.b.book_trades(book)), 1)
        self.check_cancel(book, trig, FULL)

    def test_long_005_enters(self):
        book, trig = self.go("long", 0.05)
        t = self.b.book_trades(book)[0]
        self.assertLessEqual(t["exec"]["entry_cap"], trig * 1.001)         # IOC cap from the trigger, not the price

    def test_long_05_cancels(self):
        book, trig = self.go("long", 0.5)
        self.assertEqual(self.b.book_trades(book), [])
        self.check_cancel(book, trig, "price past the cap: no late entry")

    def test_short_005_enters(self):
        book, trig = self.go("short", 0.05)
        t = self.b.book_trades(book)[0]
        self.assertGreaterEqual(t["exec"]["entry_cap"], trig * 0.999)

    def test_short_05_cancels(self):
        book, trig = self.go("short", 0.5)
        self.assertEqual(self.b.book_trades(book), [])
        self.check_cancel(book, trig, "price past the cap: no late entry")

    def test_yb_example(self):
        """YB 28 Nov: trigger 0.05756, bought at 0.05834 (+1.36%). Now: cancelled."""
        book, trig = self.go("long", 1.36)
        self.assertEqual(self.b.book_trades(book), [])
        self.check_cancel(book, trig, "price past the cap")


# ---------------------------------------------------------------- 1H RSI: level = the divergence candle's close
class Rsi1h(TR.Runner):
    def go(self, side, move_pct=0.0, rsi=None, full=False, far=None):
        tf, win = (300, 45) if side == "short" else (900, 120)
        end = T0 + tf
        cands, sig = find_signal(side, tf, win, end)
        d = cands[-1]
        if far is not None:                     # move the divergence candle's low / high so the close is `far`% away
            if side == "long":
                d["low"] = round(d["close"] / (1 + far / 100), 6)
            else:
                d["high"] = round(d["close"] / (1 - far / 100), 6)
            sig = R.div_trigger(cands, side, win)
            self.assertIsNotNone(sig)
        sym = "AUSDT"
        self.arm(side, [(sym, rsi if rsi is not None else (75.0 if side == "short" else 25.0))])
        (self.fd.c5 if side == "short" else self.fd.c15)[sym] = cands
        book = "1H RSI SHORT" if side == "short" else "1H RSI LONG"
        if full:
            fill_book(self.b, book)
        dd = 1 if side == "long" else -1
        self.fd.px = {sym: round(sig["div_close"] * (1 + dd * move_pct / 100), 6)}
        self.r.tick(end + 8)
        return book, sig

    def skipped(self, book, sig, text):
        self.assertIn(text, self.ev("RSI1H_SKIP")[-1]["reason"])
        r = rows(self.d)[-1]
        self.assertEqual((r["book"], r["taken"]), (book, "no"))
        self.assertAlmostEqual(float(r["level"]), sig["div_close"])
        self.assertTrue(r["ltp"] and r["distance_pct"] != "")

    def test_full_book(self):
        for side in ("long", "short"):
            self.setUp()
            book, sig = self.go(side, full=True)
            self.assertEqual(len(self.b.book_trades(book)), 1)
            self.skipped(book, sig, FULL)
            self.assertEqual(rows(self.d)[-1]["reason"], "book full")

    def test_005_enters_05_skips(self):
        for side in ("long", "short"):
            self.setUp()
            book, sig = self.go(side, 0.05)
            self.assertEqual(self.b.book_trades(book)[0]["symbol"], "AUSDT", side)
            self.setUp()
            book, sig = self.go(side, 0.5)
            self.assertEqual(self.b.book_trades(book), [])
            self.skipped(book, sig, "price past the cap: no late entry")

    def go_px(self, side, px_of):
        """Like go(), with the price set from the signal: px_of(sig) -> price."""
        tf, win = (300, 45) if side == "short" else (900, 120)
        end = T0 + tf
        cands, sig = find_signal(side, tf, win, end)
        self.arm(side, [("AUSDT", 75.0 if side == "short" else 25.0)])
        (self.fd.c5 if side == "short" else self.fd.c15)["AUSDT"] = cands
        self.fd.px = {"AUSDT": round(px_of(sig), 6)}
        self.r.tick(end + 8)
        return ("1H RSI SHORT" if side == "short" else "1H RSI LONG"), sig

    def test_better_price_than_the_close_enters(self):
        """28 Nov 2026 (TRUMP / MMT / CVC / ENSO were one tick on the better side of the close): a long below the
        close / a short above it enters (still past the candle's open)."""
        for side in ("long", "short"):
            self.setUp()
            book, sig = self.go_px(side, lambda g: (g["div_close"] + g["div_open"]) / 2)
            self.assertEqual(self.b.book_trades(book)[0]["symbol"], "AUSDT", side)

    def test_candle_stop_wider_than_2pct_skips(self):
        book, sig = self.go("long", 0.0, far=2.5)
        self.assertEqual(self.b.book_trades(book), [])
        self.assertIn("(> 2%)", self.ev("RSI1H_SKIP")[-1]["reason"])
        self.assertEqual(rows(self.d)[-1]["reason"], "stop too wide")

    def test_extreme_rsi(self):
        book, sig = self.go("long", 0.0, rsi=5.0)
        self.assertEqual(self.b.book_trades(book), [])
        self.assertIn("1H RSI too extreme: falling knife", self.ev("RSI1H_SKIP")[-1]["reason"])
        self.assertEqual(rows(self.d)[-1]["reason"], "1H RSI too extreme")
        self.setUp()
        book, sig = self.go("short", 0.0, rsi=95.0)
        self.assertEqual(self.b.book_trades(book), [])
        self.assertIn("1H RSI too extreme: runaway pump", self.ev("RSI1H_SKIP")[-1]["reason"])
        self.setUp()
        book, sig = self.go("long", 0.0, rsi=10.5)                 # inside the limit: trades
        self.assertEqual(len(self.b.book_trades(book)), 1)

    def test_far_from_the_low_high_but_stop_within_2pct_enters(self):
        """The 1%-from-the-low/high rule was dropped (28 Nov 2026): 1.2% away trades, the stop is within 2%."""
        for side in ("long", "short"):
            self.setUp()
            book, sig = self.go(side, 0.0, far=1.2)
            self.assertEqual(len(self.b.book_trades(book)), 1, side)


# run only the tests above, not the ones inherited from the fixtures' classes
for _cls, _base in ((Retrace, TX.Live), (Ema20, TE.Live), (Rsi1h, TR.Runner)):
    for _n in dir(_base):
        if _n.startswith("test_") and _n not in _cls.__dict__:
            setattr(_cls, _n, None)
del _cls, _base, _n                  # a TestCase class left in the module namespace would run again


if __name__ == "__main__":
    unittest.main()
