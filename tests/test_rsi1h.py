"""1H RSI LONG / 1H RSI SHORT (rsi1h.py): divergence rule, stop, sizing, hourly scan, entries, exits, cooldown.
Run: venv/bin/python -m unittest discover -s tests -v   (no network: a fake Binance data client)"""
import csv
import math
import json
import os
import random
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import rsi1h as R  # noqa: E402

T0 = 1_763_942_400                       # 2025-11-24 00:00 UTC, a 1H / 15m / 5m boundary


def walk(n, tf, seed, start=None, drift=0.0):
    rng = random.Random(seed)
    t = (start if start is not None else T0) - n * tf
    out, px = [], 100.0
    for i in range(n):
        o = px
        c = max(1.0, o * (1 + rng.gauss(drift, 0.004)))
        h, lo = max(o, c) * (1 + rng.random() * 0.002), min(o, c) * (1 - rng.random() * 0.002)
        out.append({"time": t + i * tf, "open": round(o, 4), "high": round(h, 4), "low": round(lo, 4),
                    "close": round(c, 4), "volume": 1.0})
        px = c
    return out


def reference(cands, side, window, gap=2, min_diff=3.0):
    """Written from the spec, independently of rsi1h.div_trigger's loop."""
    closes = [c["close"] for c in cands]
    e, r = R.ema_series(closes, 5), R.rsi_series(closes, 14)
    n = len(cands) - 1
    d = cands[n]
    ok_idx = [k for k in range(0, n - gap) if (d["time"] - cands[k]["time"]) / 60 <= window and r[k] is not None]
    # the window is contiguous back from the candidate: stop at the first candle outside it
    if side == "short":
        refs = [k for k in ok_idx if cands[k]["close"] < cands[k]["open"] and cands[k]["close"] > e[k]
                and cands[k]["low"] > e[k]]
        if not refs:
            return None
        k = max(refs, key=lambda i: (cands[i]["high"], i))
        hit = d["close"] < d["open"] and d["high"] > cands[k]["high"] and r[k] - r[n] >= min_diff
    else:
        refs = [k for k in ok_idx if cands[k]["close"] > cands[k]["open"] and cands[k]["high"] < e[k]]
        if not refs:
            return None
        k = min(refs, key=lambda i: (cands[i]["low"], -i))
        hit = d["close"] > d["open"] and d["low"] < cands[k]["low"] and r[n] - r[k] >= min_diff
    return (k, round(abs(r[k] - r[n]), 2)) if hit else None


class Indicators(unittest.TestCase):
    def test_rsi_series_matches_bot_rsi_wilder(self):
        import bot
        closes = [c["close"] for c in walk(120, 300, 1)]
        s = R.rsi_series(closes, 14)
        for i in (14, 30, 119):
            self.assertAlmostEqual(s[i], bot.rsi_wilder(closes[:i + 1], 14), places=9)
        self.assertIsNone(s[13])


class Divergence(unittest.TestCase):
    def scan(self, side, tf, window):
        hits = old = 0
        for seed in range(150):
            cands = walk(98, tf, seed, drift=0.0005 if side == "short" else -0.0005)
            for m in range(40, 99):
                part = cands[:m]
                got = R.div_trigger(part, side, window)
                want = reference(part, side, window)
                self.assertEqual(got is not None, want is not None, f"seed {seed} m {m}")
                any_colour = self.any_colour(part, side, window)
                old += any_colour
                if got:
                    hits += 1
                    k = want[0]
                    self.assertEqual(got["ref_time"], part[k]["time"])
                    self.assertAlmostEqual(got["rsi_gap_exact"], want[1], places=1)
                    self.assertGreaterEqual(got["between"], 2)
                    self.assertGreaterEqual(got["rsi_gap_exact"], 3)
                    for k_ in ("rsi_gap", "rsi_ref", "rsi_div"):
                        self.assertIn(f"{got[k_]}", got["detail"])
                    d = part[-1]
                    self.assertEqual(got["div_open"], d["open"])
                    self.assertEqual(got["stop_ref"], d["high"] if side == "short" else d["low"])
        self.assertGreater(hits, 0)
        self.assertGreater(old, hits)                       # colour + 3-point rules remove signals
        return hits

    @staticmethod
    def any_colour(part, side, window):
        """Old 20 Nov rule (any colour, any RSI difference) - just to show the filters do something."""
        closes = [c["close"] for c in part]
        e, r = R.ema_series(closes, 5), R.rsi_series(closes, 14)
        n, d = len(part) - 1, part[-1]
        best = None
        for k in range(n - 3, -1, -1):
            if (d["time"] - part[k]["time"]) / 60 > window:
                break
            x = part[k]
            ok = (x["close"] > e[k] and x["low"] > e[k]) if side == "short" else x["high"] < e[k]
            if ok and r[k] is not None and (best is None or (x["high"] > part[best]["high"] if side == "short"
                                                             else x["low"] < part[best]["low"])):
                best = k
        if best is None or r[n] is None:
            return False
        if side == "short":
            return d["close"] < d["open"] and d["high"] > part[best]["high"] and r[n] < r[best]
        return d["close"] > d["open"] and d["low"] < part[best]["low"] and r[n] > r[best]

    def test_short_5m_45min(self):
        print("short signals", self.scan("short", 300, 45))

    def test_long_15m_120min(self):
        print("long signals", self.scan("long", 900, 120))

    def test_no_day_or_session_logic(self):
        """Shifting every candle by any amount (across midnight IST / UTC, weekends) changes nothing."""
        for seed in range(40):
            cands = walk(98, 300, seed, drift=0.0005)
            base = [R.div_trigger(cands[:m], "short", 45) for m in range(40, 99)]
            for shift in (3 * 3600 + 1800, 19 * 3600, 5 * 86400 + 600):
                moved = [dict(c, time=c["time"] + shift) for c in cands]
                got = [R.div_trigger(moved[:m], "short", 45) for m in range(40, 99)]
                self.assertEqual([g and g["rsi_gap"] for g in got], [b and b["rsi_gap"] for b in base])

    def test_window_and_gap(self):
        cands = walk(98, 300, 7)
        with_gap0 = sum(bool(R.div_trigger(cands[:m], "short", 45, min_gap=0)) for m in range(40, 99))
        with_gap2 = sum(bool(R.div_trigger(cands[:m], "short", 45, min_gap=2)) for m in range(40, 99))
        self.assertGreaterEqual(with_gap0, with_gap2)
        self.assertIsNone(R.div_trigger(cands[:20], "short", 45))          # not enough candles


class StopAndSize(unittest.TestCase):
    def test_stop_one_tick_beyond_or_widened(self):
        self.assertEqual(R.stop_for("short", 101.0, 100.0, 0.01, 1, 0.5), (101.01, 101.01, False))
        stop, candle, widened = R.stop_for("short", 100.2, 100.0, 0.01, 1, 0.5)
        self.assertEqual((round(stop, 2), round(candle, 2), widened), (100.5, 100.21, True))
        stop, candle, widened = R.stop_for("long", 99.8, 100.0, 0.01, 1, 0.5)
        self.assertEqual((round(stop, 2), round(candle, 2), widened), (99.5, 99.79, True))
        self.assertEqual(R.stop_for("long", 98.0, 100.0, 0.01, 1, 0.5)[2], False)

    def test_size_free_margin_x20_or_impact_limit(self):
        n, info = R.size_for(100, 20, 100.0, 0.01, 5, 1e9)              # deep book: $100 x 20 = $2000 = 20 coins
        self.assertEqual((n, info["binding"], info["target_qty"]), (2000, "free margin", 20.0))
        self.assertAlmostEqual(info["margin"], 100.0)
        n, info = R.size_for(100, 20, 100.0, 0.01, 5, 3.456)            # impact limit 3.456 coins -> 3.45
        self.assertEqual((n, info["binding"], info["impact_qty"]), (345, "price impact", 3.45))
        self.assertAlmostEqual(info["margin"], 3.45 * 100 / 20)
        n, info = R.size_for(40, 20, 100.0, 0.01, 5, 1e9)               # sized from the free margin left
        self.assertEqual(n, 800)

    def test_min_notional_skip(self):
        n, info = R.size_for(100, 20, 100.0, 1, 100, 0.5)               # impact 0.5 coin = $50 < $100 min
        self.assertEqual((n, info["short"]), (0, "too little liquidity"))
        self.assertIn("min notional $100", info["skip"])
        n, info = R.size_for(0.2, 20, 100.0, 0.01, 5, 1e9)              # $0.20 x 20 = $4 < $5
        self.assertEqual((n, info["short"]), (0, "no free margin"))

    def test_impact_limit_walks_the_book(self):
        bids = [(100.0, 2.0), (99.97, 2.0), (99.9, 10.0)]               # band 0.05% = 0.05
        # 100: room 0.05 x 2 = 0.1; 99.97: 0.03 off, 0.02 inside the band -> room += 0.02 x 2 -> 0.14;
        # 99.9: 0.05 over -> 0.14 / 0.05 = 2.8 more -> 6.8 coins, average exactly 99.95
        q = R.impact_limit(bids, 0.01, 0.0005)
        self.assertAlmostEqual(q, 6.8)
        cost = 2 * 100 + 2 * 99.97 + 2.8 * 99.9
        self.assertAlmostEqual(cost / q, 99.95)
        asks = [(100.0, 1.0), (100.2, 50.0)]                             # 0.15 over: 0.05 / 0.15 = 0.333
        self.assertAlmostEqual(R.impact_limit(asks, 0.01, 0.0005), 1.33)
        self.assertEqual(R.impact_limit([], 0.01, 0.0005), 0.0)
        self.assertAlmostEqual(R.impact_limit([(100.0, 5.0), (100.01, 5.0)], 0.01, 0.0005), 10.0)   # all inside

    def test_liquidation_hard_stop_and_loss(self):
        self.assertAlmostEqual(R.liq_price("long", 100.0, 20, 0.01), 100 * 0.95 / 0.99)     # ~4.04% below
        self.assertAlmostEqual(R.liq_price("short", 100.0, 20, 0.01), 100 * 1.05 / 1.01)    # ~3.96% above
        self.assertAlmostEqual(R.hard_stop_for("short", 100.0, 0.01, 0.03), 103.0)
        self.assertAlmostEqual(R.hard_stop_for("long", 100.0, 0.01, 0.03), 97.0)
        self.assertEqual(R.active_stop("short", 101.0, 103.0), (101.0, "candle"))
        self.assertEqual(R.active_stop("short", 103.5, 103.0), (103.0, "hard"))
        self.assertEqual(R.active_stop("long", 96.0, 97.0), (97.0, "hard"))
        usd, pct = R.loss_if_stopped("short", 20.0, 100.0, 101.0, 0.05, 100.0)
        self.assertAlmostEqual(usd, 20 * 1 + 0.0005 * 20 * 201)                             # $22.01
        self.assertAlmostEqual(pct, usd)


# ---------------- runner with a fake Binance data client ----------------
class FakeData:
    def __init__(self):
        self.meta = {}
        self.prods, self.vol, self.c1h, self.c5, self.c15, self.px = [], {}, {}, {}, {}, {}
        self.calls = []

    def add(self, sym, pid, vol, c1h, tick=0.01, step=0.01):
        self.prods.append({"id": pid, "symbol": sym, "state": "live", "tick_size": tick, "contract_value": step})
        self.meta[pid] = {"min_notional": 5}
        self.vol[sym] = vol
        self.c1h[sym] = c1h

    def products(self, ct):
        return self.prods

    def tickers(self, ct):
        return [{"symbol": s, "mark_price": 1, "turnover_usd": v} for s, v in self.vol.items()]

    def candles(self, sym, res, count, now=None):
        self.calls.append((sym, res))
        src = {"1h": self.c1h, "5m": self.c5, "15m": self.c15, "1m": getattr(self, "m1", {})}[res].get(sym, [])
        return [c for c in src if now is None or c["time"] <= now]

    def request(self, method, path, params=None):
        return {"price": str(self.px[params["symbol"]])}

    depth_qty = 0.5    # coins per level: 5 levels inside 0.05% = 2.5 coins (~$250, 1/8 of a $100 book x 20)

    def orderbook(self, sym, depth=100):   # book at the last price: the capped IOC entry fills there
        self.calls.append((sym, "depth"))
        if sym in getattr(self, "books", {}):
            return self.books[sym]
        px, q = self.px.get(sym, 1.0), self.depth_qty
        return [(px, q)] + [(px * (1 - k * 1e-4), q) for k in range(1, 5)], \
            [(px, q)] + [(px * (1 + k * 1e-4), q) for k in range(1, 5)]

    def ticker(self, sym):
        return {"mark_price": self.px.get(sym, 0)}


def trend(n, tf, up, end):
    px, out = 100.0, []
    for i in range(n):
        o = px
        c = o * (1.004 if up else 0.996) if i % 5 else o * (0.999 if up else 1.001)
        out.append({"time": end - (n - i) * tf, "open": o, "high": max(o, c) * 1.001, "low": min(o, c) * 0.999,
                    "close": c, "volume": 1})
        px = c
    return out


def find_signal(side, tf, window, end):
    """A random-walk 5m / 15m series whose last closed candle (ending at `end`) is a divergence."""
    for seed in range(5000):
        cands = walk(98, tf, seed, start=end, drift=0.0005 if side == "short" else -0.0005)
        sig = R.div_trigger(cands, side, window)
        if sig:
            return cands, sig
    raise AssertionError("no signal found")


class Runner(unittest.TestCase):
    def setUp(self):
        import bot
        self.bot_mod = bot
        sl = mock.patch.object(bot.time, "sleep", lambda x: None)     # IOC children wait 1 s each: not in tests
        sl.start()
        self.addCleanup(sl.stop)
        self.d = tempfile.mkdtemp()
        saved = {k: os.environ.get(k) for k in ("EXCHANGE", "DRY_RUN", "RSI1H_ENABLED", "ENABLE_POC")}
        os.environ.update(EXCHANGE="binance", DRY_RUN="1", RSI1H_ENABLED="1", ENABLE_POC="1")
        try:
            cfg = bot.make_cfg()
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        cfg["data_dir"] = self.d
        b = bot.Bot(cfg)
        b.state["books"] = {"POC RETURN": {"start": 31.42, "balance": 31.42, "open_trade": None, "day": {},
                                           "month": {}, "trades": 0, "wins": 0}}
        b.state["books_since"] = "2026-11-01T00:00:00+05:30"
        self.sent = []
        b.tg.send = self.sent.append
        self.events = []
        b.event = lambda typ, **kw: self.events.append((typ, kw))
        self.fd = FakeData()
        b.data = self.fd
        b.rsi1h.ensure_books()
        b.rsi1h.ensure_trades_header()
        self.b, self.r = b, b.rsi1h

    def ev(self, typ):
        return [kw for t, kw in self.events if t == typ]

    def test_scan_universe_and_arming(self):
        b1 = T0
        self.fd.add("UPUSDT", 1, 2_000_000, trend(98, 3600, True, b1))
        self.fd.add("DOWNUSDT", 2, 600_000, trend(98, 3600, False, b1))
        self.fd.add("THINUSDT", 3, 400_000, trend(98, 3600, True, b1))         # below $500k: not in the universe
        self.fd.add("FLATUSDT", 4, 9_000_000, walk(98, 3600, 3, start=b1))
        self.fd.add("STALEUSDT", 5, 9_000_000, trend(98, 3600, True, b1 - 3600))  # last closed 1H candle missing
        self.r.tick(b1 + 9)
        st = self.r.st
        self.assertEqual(st["universe"], 4)
        self.assertEqual(list(st["armed_short"]), ["UPUSDT"])
        self.assertEqual(list(st["armed_long"]), ["DOWNUSDT"])
        x = self.ev("RSI1H_SCAN")[-1]
        self.assertEqual((x["universe"], x["checked"], x["stale"], x["armed_short"], x["armed_long"]), (4, 3, 1, 1, 1))
        self.assertNotIn(("THINUSDT", "1h"), self.fd.calls)                  # 1H candles only for the universe
        self.assertEqual({s for s, res in self.fd.calls if res == "5m"}, {"UPUSDT"})     # 5m: armed shorts only
        self.assertEqual({s for s, res in self.fd.calls if res == "15m"}, {"DOWNUSDT"})  # 15m: armed longs only
        n = len(self.fd.calls)
        self.r.tick(b1 + 20)                                                 # once an hour
        self.assertEqual(len([c for c in self.fd.calls[n:] if c[1] == "1h"]), 0)
        # next hour: UP's RSI back under 70 -> no longer armed
        self.fd.c1h["UPUSDT"] = walk(98, 3600, 11, start=b1 + 3600)
        self.fd.c1h["DOWNUSDT"] = trend(98, 3600, False, b1 + 3600)
        self.r.tick(b1 + 3600 + 9)
        self.assertNotIn("UPUSDT", st["armed_short"])
        self.assertIn("DOWNUSDT", st["armed_long"])
        self.assertEqual(self.ev("RSI1H_SCAN")[-1]["dropped_short"], ["UPUSDT"])

    def arm(self, side, syms):
        self.r.st["last_scan"] = T0
        self.r.st["armed_" + side] = {s: {"rsi": rsi, "candle": T0 - 3600} for s, rsi in syms}
        for i, (s, _) in enumerate(syms):
            self.r.prods[s] = {"id": 100 + i, "symbol": s, "tick": 0.0001, "cv": 0.01, "min_notional": 5}

    def test_short_entry_most_extreme_first_then_exit_and_cooldown(self):
        end = T0 + 300
        cands, sig = find_signal("short", 300, 45, end)
        self.arm("short", [("AUSDT", 75.0), ("BUSDT", 88.0)])
        self.fd.c5 = {"AUSDT": cands, "BUSDT": cands}
        px = round(sig["div_open"] * 0.999, 4)
        self.fd.px = {"AUSDT": px, "BUSDT": px}
        self.r.tick(end + 5)                                                 # before the 8 s delay: nothing
        self.assertEqual(self.ev("RSI1H_SIGNAL"), [])
        self.r.tick(end + 8)
        sigs = self.ev("RSI1H_SIGNAL")
        self.assertEqual([s["symbol"] for s in sigs], ["BUSDT", "AUSDT"])     # RSI 88 before 75
        self.assertEqual(sigs[0]["rsi_gap"], sig["rsi_gap"])
        t = self.b.book("1H RSI SHORT")["open_trade"]
        self.assertEqual((t["symbol"], t["side"], t["strategy"], t["tp"], t["trail"], t["be_r"], t["trail_start_r"]),
                         ("BUSDT", "short", "rsi1h", None, True, 1, 3))
        self.assertAlmostEqual(t["entry"], px, delta=px * 0.0005)            # one sweep of the 5 levels, <= 0.05%
        self.assertEqual(t["exec"]["entry_children"], 1)
        # parallel trades: AUSDT is taken too (margin left after BUSDT)
        self.assertEqual([x["symbol"] for x in self.b.book("1H RSI SHORT")["more_trades"]], ["AUSDT"])
        risk = abs(t["stop"] - t["entry"])
        self.assertGreaterEqual(risk / t["entry"] * 100, 0.5 - 1e-9)
        self.assertLessEqual(risk / t["entry"], 0.03)
        self.assertAlmostEqual(t["contracts"] * t["cv"], 2.5)                  # the 0.05% impact limit binds
        self.assertEqual(t["extra"]["size_binding"], "price impact")
        self.assertAlmostEqual(t["margin"], 2.5 * t["entry"] / 20)
        self.assertAlmostEqual(t["hard_stop"], R.round_tick(t["entry"] * 1.03, t["tick"], "up"))
        self.assertGreater(t["liq_price"], t["stop"])
        usd, pct = R.loss_if_stopped("short", 2.5, t["entry"], t["stop"], 0.05, t["equity"])
        self.assertAlmostEqual(t["loss_if_stopped_pct"], round(pct, 2))
        e = self.ev("ENTRY")[-1]
        self.assertEqual(e["book"], "1H RSI SHORT")
        self.assertIn("RSI gap", e["setup"])
        self.assertIn("SHORT ENTERED [1H RSI SHORT]", self.sent[-1])
        self.assertIn("Target: none - stop to entry at 1R, then trailing in whole R from 3R", self.sent[-1])
        self.assertIn("Loss if stopped: $", self.sent[-1])
        self.assertIn("Liquidation: ", self.sent[-1])
        # exit: BE at 1R, trail from 3R (2R locked), then the trail is hit
        e0, r0 = t["entry"], risk
        steps = [e0 - 1.05 * r0, e0 - 3.2 * r0, e0 - 4.1 * r0, e0 - 2.9 * r0]
        self.fd.m1 = {"BUSDT": [{"time": end + 60 * (k + 1), "open": p, "high": p + r0 * 0.02, "low": p - r0 * 0.02,
                                 "close": p, "volume": 1} for k, p in enumerate(steps)]}
        res = None
        for k in range(1, 8):
            res = self.b.check_exit_by_candles(t, end + 60 * (k + 1))
            if res:
                break
        self.assertEqual(res["reason"], "TRAIL_STOP")
        self.assertAlmostEqual(res["price"], R.round_tick(e0 - 3 * r0, 0.0001, "down"), places=6)
        self.b.close_trade(t, res)
        bk = self.b.book("1H RSI SHORT")
        self.assertEqual((bk["open_trade"]["symbol"], bk["more_trades"]), ("AUSDT", []))   # the next one moves up
        x = self.ev("EXIT")[-1]
        fees = 0.05 / 100 * t["contracts"] * t["cv"] * (e0 + res["price"])
        self.assertAlmostEqual(x["est_fees_usd"], round(fees, 4))           # taker fee both sides
        self.assertGreater(x["net_pnl_usd"], 0)
        with open(os.path.join(self.d, "trades.csv"), newline="") as fh:
            rows = list(csv.reader(fh))
        self.assertEqual(rows[0][21:24], ["session", "shadow_exit_pnl", "setup"])
        self.assertEqual(rows[0][24:], R.Rsi1h.TRADE_TAIL[3:])                  # execution columns
        self.assertEqual((rows[-1][19], rows[-1][10]), ("1H RSI SHORT", "TRAIL_STOP"))
        self.assertIn("RSI gap", rows[-1][23])
        self.assertEqual(self.b.book("POC RETURN")["balance"], 31.42)
        # cooldown: a new BUSDT short signal within 30 min is skipped
        end2 = end + 900
        cands2, sig2 = find_signal("short", 300, 45, end2)
        self.fd.c5 = {"BUSDT": cands2}
        self.fd.px = {"BUSDT": round(sig2["div_open"] * 0.999, 4)}
        self.r.st["armed_short"] = {"BUSDT": {"rsi": 80.0, "candle": T0}}
        self.r.tick(end2 + 8)
        self.assertIn("cooldown", self.ev("RSI1H_SKIP")[-1]["reason"])
        self.assertEqual([t["symbol"] for t in self.b.book_trades("1H RSI SHORT")], ["AUSDT"])   # still open

    def test_short_skipped_when_price_not_below_red_open(self):
        end = T0 + 300
        cands, sig = find_signal("short", 300, 45, end)
        self.arm("short", [("AUSDT", 75.0)])
        self.fd.c5 = {"AUSDT": cands}
        self.fd.px = {"AUSDT": sig["div_open"]}
        self.r.tick(end + 8)
        self.assertIn("not below the red divergence candle's open", self.ev("RSI1H_SKIP")[-1]["reason"])
        self.assertIsNone(self.b.book("1H RSI SHORT")["open_trade"])

    def test_long_on_15m(self):
        end = T0 + 900
        cands, sig = find_signal("long", 900, 120, end)
        self.arm("long", [("LUSDT", 22.0)])
        self.fd.c15 = {"LUSDT": cands}
        self.fd.px = {"LUSDT": round(sig["div_open"] * 1.001, 4)}
        self.r.tick(end - 300 + 8)                                           # a 5m close that is not a 15m close
        self.assertEqual(self.ev("RSI1H_SIGNAL"), [])
        self.r.tick(end + 8)
        t = self.b.book("1H RSI LONG")["open_trade"]
        self.assertEqual((t["side"], t["symbol"]), ("long", "LUSDT"))
        self.assertLess(t["stop"], t["entry"])
        self.assertTrue(all(res in ("15m", "1m", "depth") for _, res in self.fd.calls))

    def test_long_cooldown_60_min(self):
        end = T0 + 900
        cands, sig = find_signal("long", 900, 120, end)
        self.arm("long", [("LUSDT", 22.0)])
        self.fd.c15 = {"LUSDT": cands}
        self.fd.px = {"LUSDT": round(sig["div_open"] * 1.001, 4)}
        self.r.on_close({"side": "long", "symbol": "LUSDT"}, {"ts": end - 59 * 60})   # a long closed 59 min ago
        self.assertNotIn("LUSDT", self.r.st["cooldown"])                               # shorts' cooldown untouched
        self.r.tick(end + 8)
        self.assertIsNone(self.b.book("1H RSI LONG")["open_trade"])
        self.assertIn("cooldown: last LUSDT long closed", self.ev("RSI1H_SKIP")[-1]["reason"])
        self.assertIn("(< 60 min)", self.ev("RSI1H_SKIP")[-1]["reason"])

    def test_long_after_cooldown_and_short_cooldown_does_not_block_long(self):
        end = T0 + 900
        cands, sig = find_signal("long", 900, 120, end)
        self.arm("long", [("LUSDT", 22.0)])
        self.fd.c15 = {"LUSDT": cands}
        self.fd.px = {"LUSDT": round(sig["div_open"] * 1.001, 4)}
        self.r.st["cooldown_long"]["LUSDT"] = end - 61 * 60            # long closed 61 min ago: allowed
        self.r.st["cooldown"]["LUSDT"] = end - 60                       # a short closed 1 min ago: shorts only
        self.r.tick(end + 8)
        self.assertEqual(self.b.book("1H RSI LONG")["open_trade"]["symbol"], "LUSDT")

    def test_no_candle_fetch_without_free_margin(self):
        self.arm("short", [("AUSDT", 75.0)])
        bal = self.b.book("1H RSI SHORT")["balance"]
        self.b.book("1H RSI SHORT")["open_trade"] = {"symbol": "XUSDT", "product_id": 9, "side": "short", "margin": bal}
        self.r.tick(T0 + 300 + 8)
        self.assertEqual(self.fd.calls, [])

    def test_widened_stop_logged(self):
        end = T0 + 300
        for seed in range(5000):
            cands = walk(98, 300, seed, start=end, drift=0.0005)
            sig = R.div_trigger(cands, "short", 45)
            if sig and (sig["stop_ref"] - sig["div_open"] * 0.999) / sig["div_open"] < 0.004:
                break
        self.arm("short", [("AUSDT", 75.0)])
        self.fd.c5 = {"AUSDT": cands}
        self.fd.px = {"AUSDT": round(sig["div_open"] * 0.999, 4)}
        self.r.tick(end + 8)
        w = self.ev("RSI1H_STOP_WIDENED")
        self.assertEqual(len(w), 1)
        t = self.b.book("1H RSI SHORT")["open_trade"]
        self.assertTrue(t["extra"]["stop_widened"])
        self.assertIn("widened to 0.5%", t["extra"]["detail"])


if __name__ == "__main__":
    unittest.main()


class Candidates(Runner):
    """candidates.csv: one row per divergence signal per check, taken or not, same columns as the fyers-bot's."""
    def rows(self):
        with open(os.path.join(self.d, "candidates.csv"), newline="") as fh:
            r = list(csv.DictReader(fh))
        self.assertEqual(list(r[0].keys()), R.CAND_COLS)
        return r

    def test_taken_and_ranked_lower(self):
        end = T0 + 300
        cands, sig = find_signal("short", 300, 45, end)
        self.arm("short", [("AUSDT", 75.0), ("BUSDT", 88.0)])
        self.fd.c5 = {"AUSDT": cands, "BUSDT": cands}
        px = round(sig["div_open"] * 0.999, 4)
        self.fd.px = {"AUSDT": px, "BUSDT": px}
        n_before = len(self.fd.calls)
        self.r.tick(end + 8)
        b, a = self.rows()
        self.assertEqual((b["symbol"], b["taken"], b["rank"], b["reason"], b["check"]), ("BUSDT", "yes", "1", "", "5m close"))
        self.assertEqual((a["symbol"], a["taken"], a["rank"], a["reason"]), ("AUSDT", "yes", "2", ""))   # parallel
        t = self.b.book("1H RSI SHORT")["open_trade"]
        self.assertEqual((b["bot"], b["book"], b["side"], float(b["arm_rsi"])), ("binance-bot", "1H RSI SHORT", "SHORT", 88.0))
        self.assertEqual((float(b["rsi_gap"]), float(b["ref_rsi"]), float(b["div_rsi"])),
                         (sig["rsi_gap"], sig["rsi_ref"], sig["rsi_div"]))
        self.assertEqual((float(b["ref_price"]), float(b["div_price"]), float(b["div_open"])),
                         (sig["ref_price"], sig["div_price"], sig["div_open"]))
        self.assertEqual(float(b["div_price"]), sig["stop_ref"])               # short: the divergence candle's high
        self.assertEqual((float(b["ltp"]), float(b["stop_used"])), (px, t["stop"]))
        self.assertAlmostEqual(float(b["size"]), t["contracts"] * t["cv"])
        self.assertEqual(b["entry_check"], f"{px:g} < open {sig['div_open']:g}")
        self.assertEqual(float(a["ltp"]), px)
        self.assertEqual(len([c for c in self.fd.calls[n_before:] if c[1] == "5m"]), 2)  # no extra candle calls

    def test_entry_check_and_cooldown_reasons(self):
        end = T0 + 300
        cands, sig = find_signal("short", 300, 45, end)
        self.arm("short", [("AUSDT", 75.0), ("CUSDT", 74.0)])
        self.fd.c5 = {"AUSDT": cands, "CUSDT": cands}
        self.fd.px = {"AUSDT": sig["div_open"], "CUSDT": sig["div_open"]}
        self.r.st["cooldown"]["CUSDT"] = end - 600
        self.r.tick(end + 8)
        a, c = self.rows()
        self.assertEqual((a["reason"], a["entry_check"]), ("entry check", f"{sig['div_open']:g} >= open {sig['div_open']:g}"))
        self.assertEqual(c["reason"], "cooldown")

    def test_no_rows_while_book_busy(self):
        self.arm("short", [("AUSDT", 75.0)])
        self.b.book("1H RSI SHORT")["open_trade"] = {"symbol": "XUSDT", "product_id": 9, "side": "short"}
        self.r.tick(T0 + 300 + 8)
        self.assertFalse(os.path.exists(os.path.join(self.d, "candidates.csv")))


class RankByVolume(Runner):
    """RSI1H_RANK_BY: volume (default) = highest 24h quote volume first; rsi = most extreme 1H RSI first."""
    def setup_two(self, px_factor=0.999, vols=(1_000_000, 50_000_000)):
        end = T0 + 300
        cands, sig = find_signal("short", 300, 45, end)
        self.arm("short", [("AUSDT", 75.0), ("BUSDT", 88.0)])                # B has the more extreme RSI
        self.r.prods["AUSDT"]["vol24h"], self.r.prods["BUSDT"]["vol24h"] = vols
        self.fd.c5 = {"AUSDT": cands, "BUSDT": cands}
        px = round(sig["div_open"] * px_factor, 4)
        self.fd.px = {"AUSDT": px, "BUSDT": px}
        return end, sig

    def rows(self):
        with open(os.path.join(self.d, "candidates.csv"), newline="") as fh:
            return list(csv.DictReader(fh))

    def test_default_is_volume(self):
        self.assertEqual(R.load_cfg({})["rank_by"], "volume")
        self.assertEqual(R.load_cfg({"RSI1H_RANK_BY": "rsi"})["rank_by"], "rsi")
        self.assertEqual(R.load_cfg({"RSI1H_RANK_BY": " RSI "})["rank_by"], "rsi")

    def test_most_liquid_first(self):
        end, _ = self.setup_two(vols=(90_000_000, 2_000_000))                 # A far more liquid, lower RSI
        self.r.tick(end + 8)
        self.assertEqual(self.b.book("1H RSI SHORT")["open_trade"]["symbol"], "AUSDT")
        a, b = self.rows()
        self.assertEqual((a["symbol"], a["rank"], a["taken"], a["vol24h_usdt"], a["rank_by"], a["arm_rsi"]),
                         ("AUSDT", "1", "yes", "90000000", "volume", "75.0"))
        self.assertEqual((b["symbol"], b["rank"], b["taken"], b["vol24h_usdt"], b["arm_rsi"]),
                         ("BUSDT", "2", "yes", "2000000", "88.0"))                # parallel: taken too
        self.assertTrue(a["rsi_gap"] and b["rsi_gap"])
        s = self.ev("RSI1H_SIGNAL")
        self.assertEqual([(x["symbol"], x["rank"], x["vol24h_usdt"]) for x in s],
                         [("AUSDT", 1, 90000000), ("BUSDT", 2, 2000000)])

    def test_rsi_mode_keeps_the_old_order(self):
        self.r.c["rank_by"] = "rsi"
        end, _ = self.setup_two(vols=(90_000_000, 2_000_000))
        self.r.tick(end + 8)
        self.assertEqual(self.b.book("1H RSI SHORT")["open_trade"]["symbol"], "BUSDT")
        self.assertEqual([r["rank_by"] for r in self.rows()], ["rsi", "rsi"])

    def test_top_coin_fails_a_check_next_one_is_tried(self):
        end, sig = self.setup_two(vols=(90_000_000, 2_000_000))
        self.fd.px["AUSDT"] = sig["div_open"]                                  # A fails the entry check
        self.r.tick(end + 8)
        self.assertEqual(self.b.book("1H RSI SHORT")["open_trade"]["symbol"], "BUSDT")
        a, b = self.rows()
        self.assertEqual((a["symbol"], a["reason"], b["symbol"], b["taken"]), ("AUSDT", "entry check", "BUSDT", "yes"))

    def test_rank_signals_unknown_volume_last_ties_by_rsi(self):
        sigs = [{"symbol": "X", "rsi1h": 80, "vol24h": None}, {"symbol": "Y", "rsi1h": 75, "vol24h": 5e6},
                {"symbol": "Z", "rsi1h": 90, "vol24h": 5e6}]
        self.assertEqual([s["symbol"] for s in R.rank_signals(sigs, "short", "volume")], ["Z", "Y", "X"])
        self.assertEqual([s["symbol"] for s in R.rank_signals(sigs, "short", "rsi")], ["Z", "X", "Y"])
        longs = [{"symbol": "X", "rsi1h": 20, "vol24h": 1e6}, {"symbol": "Y", "rsi1h": 25, "vol24h": 1e6}]
        self.assertEqual([s["symbol"] for s in R.rank_signals(longs, "long", "volume")], ["X", "Y"])

    def test_universe_carries_24h_volume(self):
        self.fd.add("UPUSDT", 1, 2_000_000, trend(98, 3600, True, T0))
        self.r.tick(T0 + 9)
        self.assertEqual(self.r.prods["UPUSDT"]["vol24h"], 2_000_000)


class StartupUniverse(Runner):
    def test_universe_loaded_after_restart_without_a_scan(self):
        self.fd.add("AUSDT", 7, 3_000_000, trend(98, 3600, True, T0))
        self.r.st["last_scan"] = T0                                           # this hour's scan ran before the restart
        self.r.st["armed_short"] = {"AUSDT": {"rsi": 80.0, "candle": T0 - 3600}}
        end = T0 + 300
        cands, sig = find_signal("short", 300, 45, end)
        self.fd.c5 = {"AUSDT": cands}
        self.fd.px = {"AUSDT": round(sig["div_open"] * 0.999, 4)}
        self.r.tick(end + 8)
        self.assertEqual(self.r.prods["AUSDT"]["vol24h"], 3_000_000)
        self.assertNotIn(("AUSDT", "1h"), self.fd.calls)                     # no rescan, no 1h candles
        self.assertEqual(self.b.book("1H RSI SHORT")["open_trade"]["symbol"], "AUSDT")

    def test_failed_load_retried_after_a_minute(self):
        calls = []
        self.fd.products = lambda ct: calls.append(1) or []
        self.r.st["last_scan"] = T0
        self.r.tick(T0 + 100)
        self.r.tick(T0 + 130)
        self.assertEqual(len(calls), 1)
        self.r.tick(T0 + 161)
        self.assertEqual(len(calls), 2)


class Execution(Runner):
    """Liquidity cap and capped IOC entry for the 1H RSI books (paper: simulated on the live depth)."""
    def go(self, bids):
        end = T0 + 300
        cands, sig = find_signal("short", 300, 45, end)
        self.arm("short", [("AUSDT", 80.0)])
        self.fd.c5 = {"AUSDT": cands}
        px = round(sig["div_open"] * 0.999, 4)
        self.fd.px = {"AUSDT": px}
        self.fd.books = {"AUSDT": (bids(px), [(px * 1.0001, 1e9)])}
        with mock.patch.object(self.bot_mod.time, "sleep", lambda s: None):
            self.r.tick(end + 8)
        with open(os.path.join(self.d, "candidates.csv"), newline="") as fh:
            return px, list(csv.DictReader(fh))[-1]

    def test_too_little_liquidity(self):
        px, row = self.go(lambda px: [(px, 0.008)] * 5)         # 0.04 coins within 0.05%: under the $5 min notional
        self.assertEqual((row["taken"], row["reason"], float(row["impact_qty"])), ("no", "too little liquidity", 0.04))
        self.assertIsNone(self.b.book("1H RSI SHORT")["open_trade"])

    def test_liquidity_cap_and_partial_fill(self):
        # best bid holds 3 coins, the next level is 1% lower (beyond the 0.1% cap): 3 coins fill, the rest doesn't
        px, row = self.go(lambda px: [(px, 3.0)] + [(round(px * 0.99, 4), 1000.0)] * 4)
        t = self.b.book("1H RSI SHORT")["open_trade"]
        self.assertAlmostEqual(t["contracts"] * t["cv"], 3.0)
        self.assertEqual(t["entry"], px)
        self.assertEqual((row["taken"], row["entry_ref"], float(row["qty_filled"]), row["liq_capped"]),
                         ("yes", f"{px}", 3.0, "yes"))                    # impact limit 3.15, 3 fill within the cap
        self.assertGreater(float(row["qty_intended"]), 3.0)
        self.assertEqual(row["entry_children"], "1")                     # one sweep up to the cap
        self.assertEqual(float(row["entry_slip_pct"]), 0.0)
        self.assertLess(abs(t["entry"] - t["stop"]) * 3.0, 5.0 + 1e-9)              # risk on what filled
        self.assertIn("fill 3/", t["extra"]["detail"])
        # exit: trades.csv gets the entry and the (paper-estimated) exit execution
        self.b.close_trade(t, {"reason": "STOP_LOSS", "price": t["stop"], "ts": T0 + 900, "estimated": True})
        with open(os.path.join(self.d, "trades.csv"), newline="") as fh:
            tr = list(csv.DictReader(fh))[-1]
        self.assertEqual((tr["entry_ref"], tr["qty_filled"], tr["exit_children"]), (f"{px}", "3.0", "1"))
        self.assertTrue(tr["exit_slip_estimated"].startswith("yes"))
        self.assertEqual((tr["exit_estimated"], tr["strategy"]), ("True", "1H RSI SHORT"))
        self.assertEqual(tr["exit_ref"], f"{px * 1.0001}")                              # a short exit buys the ask
        # 26 Nov 2026: also vs the stop (signed) and the entry spread
        walk = px * 1.0001                                                              # the ask holds it all
        self.assertAlmostEqual(float(tr["exit_trigger"]), t["stop"])
        self.assertAlmostEqual(float(tr["exit_trigger_slip_pct"]), round((walk - t["stop"]) / t["stop"] * 100, 4))
        self.assertLess(float(tr["exit_trigger_slip_pct"]), 0)                         # ask under the stop: better
        self.assertAlmostEqual(float(tr["entry_spread"]), px * 1.0001 - px)
        msg = next(m for m in self.sent if m.startswith("TRADE CLOSED"))
        self.assertIn("% vs stop ", msg)

    def test_impact_limit_cuts_the_size(self):
        bids = lambda px: [(px, 6.0)] + [(round(px * 0.95, 4), 1.0)] * 4                 # noqa: E731
        px, row = self.go(bids)
        lim = R.impact_limit(bids(px), 0.01, 0.0005)        # 6 + 0.06 of the 5%-lower level (avg just inside 0.05%)
        self.assertAlmostEqual(lim, 6.06)
        t = self.b.book("1H RSI SHORT")["open_trade"]
        self.assertAlmostEqual(t["contracts"] * t["cv"], 6.0)                          # 0.95 is beyond the IOC cap
        self.assertEqual((row["liq_capped"], float(row["impact_qty"]), float(row["qty_intended"])), ("yes", 6.06, 6.06))
        self.assertAlmostEqual(float(row["target_qty"]), math.floor(100 * 20 / px / 0.01) * 0.01)

    def test_deep_book_uses_the_whole_free_margin(self):
        px, row = self.go(lambda px: [(px, 1e9)] * 5)
        t = self.b.book("1H RSI SHORT")["open_trade"]
        self.assertEqual((t["extra"]["size_binding"], row["liq_capped"]), ("free margin", "no"))
        self.assertAlmostEqual(t["margin"], 100.0, delta=px * 0.01 / 20 + 1e-9)
        self.assertLess(self.b.free_margin("1H RSI SHORT"), 0.1)
        self.assertGreater(float(row["loss_pct_if_stopped"]), 10)            # 20x the stop distance, no risk cap


class LimitExits(Runner):
    """26 Nov 2026: a TRAIL_STOP whose market exit would slip > 0.1% exits with limit orders (paper, simulated on the
    live depth); the rest waits at the floor and goes at market once price is back to entry - 0.5R (short)."""
    go = Execution.go

    def open_short(self):
        px, _ = self.go(lambda px: [(px, 1e9)] * 5)
        t = self.b.book("1H RSI SHORT")["open_trade"]
        r0 = abs(t["stop"] - t["entry"])
        t["trail_r"], t["be_moved"] = 3, True
        t["stop"] = round(t["entry"] - 2 * r0, 2)                 # 2R locked
        self.q = round(t["contracts"] * t["cv"], 10)
        self.half = round(math.floor(self.q / 2 / t["cv"]) * t["cv"], 10)
        return t, r0

    def rows(self):
        with open(os.path.join(self.d, "trades.csv"), newline="") as fh:
            return list(csv.DictReader(fh))

    def res(self, t):
        return {"reason": "TRAIL_STOP", "price": t["stop"], "ts": T0 + 3600, "estimated": True}

    def test_deep_book_trail_stop_stays_market(self):
        t, _ = self.open_short()
        self.fd.books = {"AUSDT": ([(t["stop"], 1e9)], [(t["stop"], 1e9)])}
        self.assertFalse(self.b.start_limit_exit(t, self.res(t)))

    def test_thin_book_limit_then_rest_fills(self):
        t, r0 = self.open_short()
        s = t["stop"]
        self.fd.books = {"AUSDT": ([(s, 1e9)], [(s, self.half), (round(s * 1.01, 2), 1e9)])}
        with mock.patch.object(self.bot_mod.time, "sleep", lambda x: None):
            with mock.patch.object(self.b, "check_exit_by_candles", lambda t, now: self.res(t)):
                self.b.manage_open_trades(T0 + 3700)
        pend = t["pending_exit"]
        tk = t["tick"]
        self.assertAlmostEqual(pend["floor"], math.floor(s * 1.001 / tk + 1e-9) * tk)
        self.assertAlmostEqual(pend["guard"], t["entry"] - 0.5 * r0)
        self.assertAlmostEqual(pend["lim_qty"], self.half)
        self.assertIn("-> limit exit", self.sent[-1])
        self.assertAlmostEqual(self.ev("LIMIT_EXIT")[0]["filled"], self.half)
        self.fd.books = {"AUSDT": ([(s, 1e9)], [(round(pend["floor"] + tk, 6), 1e9)])}
        self.b.manage_open_trades(T0 + 3715)                        # above the floor: waits
        self.assertIn("pending_exit", t)
        self.fd.books = {"AUSDT": ([(s, 1e9)], [(pend["floor"], 1e9)])}
        self.b.manage_open_trades(T0 + 3730)                        # fills at the floor
        self.assertIsNone(self.b.book("1H RSI SHORT")["open_trade"])
        row = self.rows()[-1]
        self.assertEqual((row["exit_reason"], row["exit_type"]), ("TRAIL_STOP", "limit"))
        left = round(self.q - self.half, 10)
        self.assertAlmostEqual(float(row["exit_price"]), (self.half * s + left * pend["floor"]) / self.q, places=2)
        self.assertIn("Exit: limit,", next(m for m in self.sent if m.startswith("TRADE CLOSED")))

    def test_guard_sends_rest_to_market_and_killswitch(self):
        for case in ("guard", "kill"):
            with self.subTest(case=case):
                self.setUp()
                t, r0 = self.open_short()
                s = t["stop"]
                self.fd.books = {"AUSDT": ([(s, 1e9)], [(s, self.half), (round(s * 1.01, 2), 1e9)])}
                with mock.patch.object(self.bot_mod.time, "sleep", lambda x: None):
                    self.assertTrue(self.b.start_limit_exit(t, self.res(t)))
                g = t["pending_exit"]["guard"]
                self.fd.books = {"AUSDT": ([(g, 1e9)], [(round(g + t["tick"], 6), 1e9)])}
                if case == "guard":
                    self.b.manage_open_trades(T0 + 3800)
                    why = "price back to entry - 0.5R"
                else:
                    self.b.close_now(t, "KILLSWITCH")
                    why = "KILLSWITCH"
                self.assertIsNone(self.b.book("1H RSI SHORT")["open_trade"])
                row = self.rows()[-1]
                self.assertEqual(row["exit_type"], "limit+market")
                msg = next(m for m in self.sent if m.startswith("TRADE CLOSED"))
                self.assertIn(f"Exit: limit {self.half:g} + market", msg)
                self.assertIn(why, msg)

    def test_entry_and_market_exit_lines(self):
        t, _ = self.open_short()
        self.assertTrue(any("Entry: limit, 1 order(s)" in m for m in self.sent if "ENTERED" in m))
        self.b.close_trade(t, {"reason": "BREAKEVEN_STOP", "price": t["entry"], "ts": T0 + 900, "estimated": True})
        self.assertEqual(self.rows()[-1]["exit_type"], "market")
        self.assertIn("Exit: market, slippage", next(m for m in self.sent if m.startswith("TRADE CLOSED")))


class ParallelTrades(Runner):
    """26 Nov 2026: up to MAX_OPEN_TRADES (3) per book while it has free margin, each sized from the margin left."""
    def sig_for(self, syms, end=T0 + 300):
        cands, sig = find_signal("short", 300, 45, end)
        self.arm("short", [(s, 80.0 - i) for i, s in enumerate(syms)])
        self.fd.c5 = {s: cands for s in syms}
        px = round(sig["div_open"] * 0.999, 4)
        self.fd.px = {s: px for s in syms}
        return end, px

    def rows(self):
        with open(os.path.join(self.d, "candidates.csv"), newline="") as fh:
            return list(csv.DictReader(fh))

    def test_at_most_three_trades(self):
        end, px = self.sig_for(["AUSDT", "BUSDT", "CUSDT", "DUSDT"])
        bk = self.b.book("1H RSI SHORT")
        self.r.tick(end + 8)
        trades = self.b.book_trades("1H RSI SHORT")
        self.assertEqual(len(trades), 3)
        used = sum(t["margin"] for t in trades)
        self.assertLessEqual(used, bk["balance"] + 1e-9)                    # never more margin than the balance
        rows = self.rows()
        taken = [r["symbol"] for r in rows if r["taken"] == "yes"]
        self.assertEqual(taken, [t["symbol"] for t in trades])
        skipped = [r for r in rows if r["taken"] == "no"]
        self.assertEqual([r["reason"] for r in skipped], ["max open trades"])
        self.assertEqual(len(taken) + len(skipped), 4)
        self.assertAlmostEqual(self.b.free_margin("1H RSI SHORT"), bk["balance"] - used)
        st = self.b.status_text()
        self.assertIn(f"{len(trades)} open, free margin", st)
        for t in trades:
            self.assertIn(t["symbol"], st)
        line = next(x for x in st.splitlines() if trades[0]["symbol"] in x)
        for part in ("size 2.5", f"margin ${trades[0]['margin']:.2f}", "stop ", "liq ", "% of book"):
            self.assertIn(part, line)

    def test_last_trade_sized_to_the_free_margin(self):
        end, px = self.sig_for(["AUSDT"])
        bk = self.b.book("1H RSI SHORT")
        bk["open_trade"] = {"symbol": "XUSDT", "product_id": 99, "side": "short", "margin": bk["balance"] * 0.9}
        self.r.tick(end + 8)
        t = self.b.book_trades("1H RSI SHORT")[-1]
        self.assertEqual(t["symbol"], "AUSDT")
        self.assertLessEqual(t["margin"], bk["balance"] * 0.1 + 1e-6)
        self.assertEqual(t["extra"]["size_binding"], "free margin")

    def test_same_coin_not_twice(self):
        end, px = self.sig_for(["AUSDT"])
        self.r.tick(end + 8)
        n = len(self.b.book_trades("1H RSI SHORT"))
        self.r.done["short"] = (0, set())
        self.r.tick(end + 12)
        self.assertEqual(len(self.b.book_trades("1H RSI SHORT")), n)
        self.assertEqual(self.rows()[-1]["reason"], "already held")

    def test_close_middle_trade_and_restart(self):
        end, px = self.sig_for(["AUSDT", "BUSDT", "CUSDT"])
        bk = self.b.book("1H RSI SHORT")
        self.r.tick(end + 8)
        syms = [t["symbol"] for t in self.b.book_trades("1H RSI SHORT")]
        self.assertEqual(len(syms), 3)
        mid = self.b.book_trades("1H RSI SHORT")[1]
        self.b.close_trade(mid, {"reason": "STOP_LOSS", "price": mid["stop"], "ts": end + 900, "estimated": True})
        self.assertEqual([t["symbol"] for t in self.b.book_trades("1H RSI SHORT")], [syms[0], syms[2]])
        self.b.save_state()
        with open(os.path.join(self.d, "state.json")) as fh:
            st = json.load(fh)
        self.assertEqual(st["books"]["1H RSI SHORT"]["open_trade"]["symbol"], syms[0])
        self.assertEqual([t["symbol"] for t in st["books"]["1H RSI SHORT"]["more_trades"]], [syms[2]])
        first = self.b.book_trades("1H RSI SHORT")[0]
        self.b.close_trade(first, {"reason": "STOP_LOSS", "price": first["stop"], "ts": end + 960, "estimated": True})
        self.assertEqual((bk["open_trade"]["symbol"], bk["more_trades"]), (syms[2], []))
        self.assertEqual([n for n, _ in self.b.open_trades()], ["1H RSI SHORT"])


class SizingV2(Runner):
    """26 Nov 2026: stop too wide, liquidation before the stop, hard stop, trades.csv / candidates.csv headers."""
    go = Execution.go

    def test_stop_too_wide_skipped(self):
        self.r.c["max_stop"] = 0.001                                         # every stop is wider than 0.1%
        px, row = self.go(lambda px: [(px, 1e9)] * 5)
        self.assertEqual((row["taken"], row["reason"]), ("no", "stop too wide"))
        self.assertIsNone(self.b.book("1H RSI SHORT")["open_trade"])
        self.assertIn("% from the entry", self.ev("RSI1H_SKIP")[-1]["reason"])

    def test_liquidation_before_stop_refused(self):
        self.r.c["mmr"] = 0.045                                              # liq ~0.5% above a 20x short
        px, row = self.go(lambda px: [(px, 1e9)] * 5)
        self.assertEqual((row["taken"], row["reason"]), ("no", "liquidation before stop"))
        self.assertTrue(row["liq_price"])

    def test_hard_stop_label(self):
        px, _ = self.go(lambda px: [(px, 1e9)] * 5)
        t = self.b.book("1H RSI SHORT")["open_trade"]
        t["stop_source"], t["stop"] = "hard", t["hard_stop"]
        res = {"reason": "STOP_LOSS", "price": t["stop"], "ts": T0 + 900, "estimated": True}
        with mock.patch.object(self.b, "check_exit_by_candles", lambda t, now: dict(res)):
            self.b.manage_open_trades(T0 + 900)
        self.assertEqual(self.ev("EXIT")[-1]["reason"], "HARD_STOP")

    def test_trades_header_repaired(self):
        path = os.path.join(self.d, "trades.csv")
        head = R.Rsi1h.TRADE_HEAD
        bad = [x for x in head if x != "exit_estimated"] + R.Rsi1h.TRADE_TAIL[:-2] + ["exit_estimated", "exit_type"]
        row = ["x"] * 17 + ["True", "", "1H RSI SHORT", "99.5"]
        with open(path, "w", newline="") as fh:
            csv.writer(fh).writerows([bad, row])
        self.r.ensure_trades_header()
        with open(path, newline="") as fh:
            r = list(csv.reader(fh))
        self.assertEqual(r[0], head + R.Rsi1h.TRADE_TAIL)
        self.assertEqual(r[1], row)                                          # rows untouched
        with open(path, newline="") as fh:
            d = list(csv.DictReader(fh))[0]
        self.assertEqual((d["exit_estimated"], d["strategy"], d["book_balance_usd"]), ("True", "1H RSI SHORT", "99.5"))
        self.r.ensure_trades_header()                                        # idempotent
        with open(path, newline="") as fh:
            self.assertEqual(next(csv.reader(fh)), head + R.Rsi1h.TRADE_TAIL)

    def test_candidates_header_extended(self):
        path = os.path.join(self.d, "candidates.csv")
        old = R.CAND_COLS[:R.CAND_COLS.index("free_margin")]
        with open(path, "w", newline="") as fh:
            csv.writer(fh).writerows([old, ["1"] * len(old)])
        self.r.ensure_cand_header()
        with open(path, newline="") as fh:
            r = list(csv.reader(fh))
        self.assertEqual((r[0], r[1]), (R.CAND_COLS, ["1"] * len(old)))


if __name__ == "__main__":
    unittest.main()
