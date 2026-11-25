"""1H RSI LONG / 1H RSI SHORT (rsi1h.py): divergence rule, stop, sizing, hourly scan, entries, exits, cooldown.
Run: venv/bin/python -m unittest discover -s tests -v   (no network: a fake Binance data client)"""
import csv
import os
import random
import sys
import tempfile
import unittest

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

    def test_size_5pct_risk_20x_cap(self):
        n, info = R.size_for(100, 5, 20, 100.0, 101.0, 0.01, 5)         # $5 / $1 = 5 units = 500 steps
        self.assertEqual((n, info["binding"]), (500, "risk budget"))
        self.assertAlmostEqual(info["risk_usd"], 5.0)
        self.assertAlmostEqual(info["margin"], 500 * 0.01 * 100 / 20)
        n, info = R.size_for(100, 5, 20, 100.0, 100.1, 0.01, 5)         # 0.1%: 50 units but cap 20 units
        self.assertEqual((n, info["binding"]), (2000, "leverage cap"))
        self.assertAlmostEqual(info["notional"], 2000.0)

    def test_min_notional_skip(self):
        n, info = R.size_for(100, 5, 20, 100.0, 120.0, 1, 100)           # step 1 = $100 min, risks $20 > $5
        self.assertEqual(n, 0)
        self.assertIn("min notional $100", info["skip"])
        self.assertIn("> 5% = $5.00", info["skip"])


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
        self.assertEqual(t["entry"], px)                                     # slippage 0 (like POC paper)
        self.assertEqual(self.ev("RSI1H_SKIP")[-1]["reason"], "1H RSI SHORT already has a trade open")
        risk = abs(t["stop"] - t["entry"])
        self.assertGreaterEqual(risk / t["entry"] * 100, 0.5 - 1e-9)
        self.assertLessEqual(t["risk_usd"], 5.0 + 1e-9)
        self.assertGreater(t["risk_usd"], 4.5)
        e = self.ev("ENTRY")[-1]
        self.assertEqual(e["book"], "1H RSI SHORT")
        self.assertIn("RSI gap", e["setup"])
        self.assertIn("SHORT ENTERED [1H RSI SHORT]", self.sent[-1])
        self.assertIn("Target: none - stop to entry at 1R, then trailing in whole R from 3R", self.sent[-1])
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
        self.assertIsNone(self.b.book("1H RSI SHORT")["open_trade"])
        x = self.ev("EXIT")[-1]
        fees = 0.05 / 100 * t["contracts"] * t["cv"] * (e0 + res["price"])
        self.assertAlmostEqual(x["est_fees_usd"], round(fees, 4))           # taker fee both sides
        self.assertGreater(x["net_pnl_usd"], 0)
        with open(os.path.join(self.d, "trades.csv"), newline="") as fh:
            rows = list(csv.reader(fh))
        self.assertEqual(rows[0][-3:], ["session", "shadow_exit_pnl", "setup"])
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
        self.assertIsNone(self.b.book("1H RSI SHORT")["open_trade"])

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
        self.assertTrue(all(res in ("15m", "1m") for _, res in self.fd.calls))

    def test_no_candle_fetch_while_book_busy(self):
        self.arm("short", [("AUSDT", 75.0)])
        self.b.book("1H RSI SHORT")["open_trade"] = {"symbol": "XUSDT", "product_id": 9, "side": "short"}
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
        self.assertEqual((a["symbol"], a["taken"], a["rank"], a["reason"]), ("AUSDT", "no", "2", "ranked lower"))
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
        self.assertEqual(a["ltp"], "")                                         # ranked lower: no price lookup
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
        self.assertEqual((b["symbol"], b["rank"], b["reason"], b["vol24h_usdt"], b["arm_rsi"]),
                         ("BUSDT", "2", "ranked lower", "2000000", "88.0"))
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
