"""BOOK JUMP unit tests: wall detection, paper fill / queue, exits, session tagging, kill switch, recorded stream.
Run: venv/bin/python -m unittest discover -s tests -v"""
import json
import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bookjump as bj  # noqa: E402
from bot import Bot, NY  # noqa: E402

SYM = "TESTUSDT"
META = {SYM: {"tick": 0.01, "step": 0.1, "min_notional": 5.0}}
# Wednesday 2026-11-18, 05:00 New York = outside the US session; 10:00 = inside
T_OUT = datetime(2026, 11, 18, 5, 0, tzinfo=NY).timestamp()
T_US = datetime(2026, 11, 18, 10, 0, tzinfo=NY).timestamp()


class Sink:
    def __init__(self):
        self.events, self.msgs, self.walls, self.trades = [], [], [], []

    def event(self, typ, **kw):
        self.events.append((typ, kw))

    def tg(self, text):
        self.msgs.append(text)

    def wall_row(self, row):
        self.walls.append(dict(zip(bj.WALL_COLS, row)))

    def trade_row(self, row):
        self.trades.append(row)

    def save(self):
        pass

    def names(self):
        return [e[0] for e in self.events]

    def last(self, typ):
        return [kw for t, kw in self.events if t == typ][-1]


def book(best_bid, wall_px=None, wall_q=400, n=20, side="bid", spread_ticks=1):
    bids = [(round(best_bid - i * 0.01, 2), 10.0) for i in range(n)]
    asks = [(round(best_bid + spread_ticks * 0.01 + i * 0.01, 2), 10.0) for i in range(n)]
    if wall_px is not None:
        lv = bids if side == "bid" else asks
        lv[:] = [(p, float(wall_q) if abs(p - wall_px) < 0.005 else q) for p, q in lv]
    return bids, asks


def without(levels_pair, price):
    bids, asks = levels_pair
    return [x for x in bids if abs(x[0] - price) > 0.005], [x for x in asks if abs(x[0] - price) > 0.005]


def coarse_book(best_bid, wall_px=None, n=20):
    """A ~1.00 coin with tick 0.001 (0.1% of price): the tick minimums bind instead of the percentages."""
    bids = [(round(best_bid - i * 0.001, 3), 1000.0) for i in range(n)]
    asks = [(round(best_bid + 0.001 + i * 0.001, 3), 1000.0) for i in range(n)]
    if wall_px is not None:
        bids = [(p, 40000.0 if abs(p - wall_px) < 0.0005 else q) for p, q in bids]
    return bids, asks


def engine(cfg=None, state=None):
    sink = Sink()
    c = bj.load_cfg({}) if cfg is None else cfg
    e = bj.Engine(c, Bot.in_us_session, sink, state or bj.new_state(c["start_balance"]))
    e.set_universe(META, [SYM])
    e.st["summary_day"] = bj.ist_day(T_OUT)
    return e, sink


def place_long(e, t0=T_OUT):
    """Bid wall at 99.97 for 3 s -> resting buy at 99.98 (queue 10)."""
    for k in range(31):
        e.on_depth(SYM, t0 + k * 0.1, *book(100.00, 99.97))
    return e.orders.get(SYM)


def filled_long(e, t0=T_OUT):
    place_long(e, t0)
    e.on_trade(SYM, t0 + 3.5, 99.97, 1, True)              # prints below our price (at the wall) -> filled at 99.98
    return e.pos[SYM]


class WallDetection(unittest.TestCase):
    def setUp(self):
        self.c = bj.load_cfg({})

    def test_wall_found_in_top5(self):
        bids, asks = book(100.00, 99.97)
        w = bj.find_walls(bids, asks, self.c)
        self.assertEqual([(x[0], x[1]) for x in w], [("bid", 99.97)])
        self.assertGreater(w[0][4], 5)                     # multiple of the 10-level average

    def test_only_top5_levels(self):
        bids, asks = book(100.00, 99.94)                   # 7th level: not a candidate
        self.assertEqual(bj.find_walls(bids, asks, self.c), [])

    def test_and_or_rule(self):
        bids, asks = book(100.00, 99.97, wall_q=200)       # ~$20k: 6.9x average but below $25k
        self.assertEqual(bj.find_walls(bids, asks, self.c), [])
        c_or = bj.load_cfg({"BJ_WALL_RULE": "or"})
        self.assertEqual(len(bj.find_walls(bids, asks, c_or)), 1)
        flat_b = [(100 - i * 0.01, 300.0) for i in range(5)]   # every level $30k: big but 1x average
        flat_a = [(100.01 + i * 0.01, 300.0) for i in range(5)]
        self.assertEqual(bj.find_walls(flat_b, flat_a, self.c), [])
        self.assertEqual(len(bj.find_walls(flat_b, flat_a, c_or)), 10)

    def test_ask_wall_mirrored(self):
        bids, asks = book(100.00, 100.03, side="ask")
        self.assertEqual([(x[0], x[1]) for x in bj.find_walls(bids, asks, self.c)], [("ask", 100.03)])

    def test_persistence_before_order(self):
        e, s = engine()
        for k in range(30):                                # 0.0 .. 2.9 s
            e.on_depth(SYM, T_OUT + k * 0.1, *book(100.00, 99.97))
        self.assertNotIn(SYM, e.orders)
        e.on_depth(SYM, T_OUT + 3.0, *book(100.00, 99.97))
        o = e.orders[SYM]
        self.assertAlmostEqual(o["price"], 99.98)
        self.assertEqual(o["queue"], 10.0)
        self.assertEqual(s.last("BJ_ORDER")["side"], "long")

    def test_flicker_resets_persistence(self):
        e, s = engine()
        e.on_depth(SYM, T_OUT, *book(100.00, 99.97))
        e.on_depth(SYM, T_OUT + 2.0, *book(100.00))        # gone (in view) -> forgotten, never "seen"
        e.on_depth(SYM, T_OUT + 2.5, *book(100.00, 99.97))
        e.on_depth(SYM, T_OUT + 4.0, *book(100.00, 99.97))  # only 1.5 s at the price
        self.assertNotIn(SYM, e.orders)
        self.assertEqual(e.wlog, [])

    def test_not_passive_then_retry(self):
        e, s = engine()
        for k in range(31):                                # wall AT best bid, 1-tick spread: can't jump
            e.on_depth(SYM, T_OUT + k * 0.1, *book(99.97, 99.97))
        self.assertNotIn(SYM, e.orders)
        e.on_depth(SYM, T_OUT + 3.2, *book(99.97, 99.97, spread_ticks=3))
        self.assertAlmostEqual(e.orders[SYM]["price"], 99.98)

    def test_ask_wall_short_order(self):
        e, s = engine()
        for k in range(31):
            e.on_depth(SYM, T_OUT + k * 0.1, *book(100.00, 100.03, side="ask"))
        o = e.orders[SYM]
        self.assertEqual((o["side"], round(o["price"], 2)), ("short", 100.02))


class Sizing(unittest.TestCase):
    def test_capped_and_rounded(self):
        # risk 1% of 100 = $1 over 0.02 -> 50 units, but notional cap 3 x 100 = $300 -> 3.0006 -> 3.0 (step 0.1)
        self.assertAlmostEqual(bj.size_order(100, 1, 99.98, 99.96, 3, 0.1, 5), 3.0)
        self.assertAlmostEqual(bj.size_order(100, 1, 99.98, 98.98, 3, 0.1, 5), 1.0)   # risk-bound
        self.assertEqual(bj.size_order(100, 1, 99.98, 99.96, 3, 0.1, 500), 0.0)       # below min notional

    def test_fees(self):
        c = bj.load_cfg({})
        f = bj.fees_usd(100.0, 101.0, 2.0, c)
        self.assertAlmostEqual(f, 100 * 2 * 0.0002 * 1.18 + 101 * 2 * 0.0005 * 1.18)


class PaperFill(unittest.TestCase):
    def test_queue_must_trade_through(self):
        e, s = engine()
        place_long(e)
        e.on_trade(SYM, T_OUT + 3.2, 99.98, 4, True)       # sells at our price: 4 of queue 10
        e.on_trade(SYM, T_OUT + 3.3, 99.98, 50, False)     # buyer-aggressor at our price: doesn't count
        self.assertIn(SYM, e.orders)
        e.on_trade(SYM, T_OUT + 3.4, 99.98, 6, True)       # 10 = queue, not more
        self.assertIn(SYM, e.orders)
        e.on_trade(SYM, T_OUT + 3.5, 99.98, 0.1, True)     # 10.1 > 10 -> filled
        self.assertIn(SYM, e.pos)
        self.assertAlmostEqual(e.pos[SYM]["entry"], 99.98)
        self.assertIn("BJ_FILL", s.names())

    def test_trade_below_fills(self):
        e, s = engine()
        place_long(e)
        e.on_trade(SYM, T_OUT + 3.2, 99.97, 0.1, True)
        self.assertAlmostEqual(e.pos[SYM]["entry"], 99.98)   # filled at our limit, not the print

    def test_short_fill_mirrored(self):
        e, s = engine()
        for k in range(31):
            e.on_depth(SYM, T_OUT + k * 0.1, *book(100.00, 100.03, side="ask"))
        e.on_trade(SYM, T_OUT + 3.2, 100.02, 11, True)     # seller-aggressor at our ask: ignored
        self.assertNotIn(SYM, e.pos)
        e.on_trade(SYM, T_OUT + 3.3, 100.03, 0.1, False)   # prints above our ask -> filled
        self.assertEqual(e.pos[SYM]["side"], "short")


class OrderCancel(unittest.TestCase):
    def test_wall_pulled(self):
        e, s = engine()
        place_long(e)
        e.on_depth(SYM, T_OUT + 3.5, *book(100.00))
        self.assertNotIn(SYM, e.orders)
        self.assertEqual(s.last("BJ_CANCEL")["reason"], "wall_pulled")

    def test_wall_below_threshold(self):
        e, s = engine()
        place_long(e)
        e.on_depth(SYM, T_OUT + 3.5, *book(100.00, 99.97, wall_q=240))   # $24k: under $25k, kept 60%
        self.assertEqual(s.last("BJ_CANCEL")["reason"], "wall_below_threshold")

    def test_timeout(self):
        e, s = engine()
        o = place_long(e)
        e.on_timer(o["placed_ts"] + 59.9)
        self.assertIn(SYM, e.orders)
        e.on_timer(o["placed_ts"] + 60)
        self.assertEqual(s.last("BJ_CANCEL")["reason"], "timeout")

    def test_price_away(self):
        # max(0.5% of 99.98 = 0.4999, 10 ticks = 0.10) -> 0.4999 (50 ticks) here
        e, s = engine()
        place_long(e)
        e.on_depth(SYM, T_OUT + 3.5, *book(100.47, 99.97, n=12))   # 0.49 away: kept (old rule cancelled at 0.11)
        self.assertIn(SYM, e.orders)
        e.on_depth(SYM, T_OUT + 3.6, *book(100.49, 99.97, n=12))   # 0.51 away; wall now out of view
        self.assertEqual(s.last("BJ_CANCEL")["reason"], "price_away")

    def test_price_away_ticks_bind_on_coarse_tick(self):
        # tick 0.001 on a ~1.00 coin: 0.5% = 0.005 (5 ticks) < 10 ticks -> 10 ticks
        e, s = engine()
        e.set_universe({"CTUSDT": {"tick": 0.001, "step": 1, "min_notional": 5}}, ["CTUSDT"])
        for k in range(31):
            e.on_depth("CTUSDT", T_OUT + k * 0.1, *coarse_book(1.000, 0.997))
        self.assertAlmostEqual(e.orders["CTUSDT"]["price"], 0.998)
        e.on_depth("CTUSDT", T_OUT + 3.5, *coarse_book(1.008, 0.997, n=12))   # 10 ticks: kept
        self.assertIn("CTUSDT", e.orders)
        e.on_depth("CTUSDT", T_OUT + 3.6, *coarse_book(1.009, 0.997, n=12))   # 11 ticks
        self.assertEqual(s.last("BJ_CANCEL")["reason"], "price_away")

    def test_max_open(self):
        e, s = engine()
        meta = {f"S{i}USDT": dict(META[SYM]) for i in range(5)}
        e.set_universe(meta, list(meta))
        for k in range(31):
            for sym in meta:
                e.on_depth(sym, T_OUT + k * 0.1, *book(100.00, 99.97))
        self.assertEqual(len(e.orders), 3)


class Exits(unittest.TestCase):
    def exit_of(self, s):
        return s.last("BJ_EXIT")

    # entry 99.98, tick 0.01: hard stop = max(0.4% = 0.39992, 10 ticks) -> 99.58008; size 1% of $100 / 0.39992
    # = 2.5005 -> 2.5 (step 0.1; the 3x notional cap 3.0006 does not bind)
    STOP = 99.98 - 99.98 * 0.004
    QTY = 2.5

    def test_size_and_stop_from_the_same_distance(self):
        e, s = engine()
        o = place_long(e)
        self.assertAlmostEqual(o["stop"], self.STOP)
        self.assertAlmostEqual(o["qty"], self.QTY)
        self.assertAlmostEqual(s.last("BJ_ORDER")["stop"], self.STOP)

    def test_wall_gone_is_shadow_only(self):
        e, s = engine()
        p = filled_long(e)
        e.on_depth(SYM, T_OUT + 4, *without(book(100.00), 99.97))     # level removed entirely
        self.assertEqual(p["exit_pending"], "")                         # no exit any more
        self.assertEqual(p["shadow"]["reason"], "WALL_GONE")
        self.assertNotIn("BJ_WALL_EXIT_SHADOW", s.names())              # priced at the NEXT trade, like the old exit
        e.on_trade(SYM, T_OUT + 4.1, 99.95, 1, True)
        sh = s.last("BJ_WALL_EXIT_SHADOW")
        fees = 99.98 * self.QTY * 0.0002 * 1.18 + 99.95 * self.QTY * 0.0005 * 1.18
        self.assertEqual((sh["reason"], sh["price"], sh["session"]), ("WALL_GONE", 99.95, "outside"))
        self.assertAlmostEqual(sh["pnl_usd"], round(-0.03 * self.QTY - fees, 4))
        self.assertEqual(sh["time"], bj.ist_iso(T_OUT + 4.1))
        self.assertIn(SYM, e.pos)
        self.assertNotIn("BJ_EXIT", s.names())
        e.on_depth(SYM, T_OUT + 4.2, *without(book(100.00), 99.97))
        e.on_trade(SYM, T_OUT + 4.3, 99.94, 1, True)
        self.assertEqual(s.names().count("BJ_WALL_EXIT_SHADOW"), 1)   # only the first time the old rule fires

    def test_wall_shrank_is_shadow_only(self):
        e, s = engine()
        p = filled_long(e)
        e.on_depth(SYM, T_OUT + 4, *book(100.00, 99.97, wall_q=210))   # 52% of 400: still fine
        self.assertIsNone(p["shadow"])
        e.on_depth(SYM, T_OUT + 4.1, *book(100.00, 99.97, wall_q=190))
        self.assertEqual((p["shadow"]["reason"], p["exit_pending"]), ("WALL_SHRANK", ""))

    def test_trade_below_wall_is_shadow_only(self):
        e, s = engine()
        p = filled_long(e)
        e.on_trade(SYM, T_OUT + 4, 99.97, 1, True)          # at the wall: fine
        self.assertIsNone(p["shadow"])
        e.on_trade(SYM, T_OUT + 4.1, 99.96, 1, True)        # through it: shadow, priced at the NEXT trade
        self.assertEqual(p["shadow"]["reason"], "WALL_BROKEN")
        e.on_trade(SYM, T_OUT + 4.2, 99.94, 1, True)
        self.assertEqual((s.last("BJ_WALL_EXIT_SHADOW")["reason"], s.last("BJ_WALL_EXIT_SHADOW")["price"]),
                         ("WALL_BROKEN", 99.94))
        self.assertEqual(s.last("BJ_WALL_EXIT_SHADOW")["traded_at_wall_usdt"], round(99.97, 2))
        self.assertIn(SYM, e.pos)

    def test_hard_stop(self):
        e, s = engine()
        p = filled_long(e)
        e.on_trade(SYM, T_OUT + 4, 99.59, 1, True)          # above 99.58008: holds
        self.assertEqual(p["exit_pending"], "")
        e.on_trade(SYM, T_OUT + 4.1, 99.58, 1, True)        # at/through the stop: exit at the NEXT trade
        self.assertEqual(p["exit_pending"], "HARD_STOP")
        e.on_trade(SYM, T_OUT + 4.2, 99.57, 1, True)
        x = s.last("BJ_EXIT")
        self.assertEqual((x["reason"], x["exit_price"]), ("HARD_STOP", 99.57))
        self.assertAlmostEqual(x["r_multiple"], round(-0.41 / (99.98 * 0.004), 2))
        st = x["settings"]
        self.assertEqual((st["stop_pct"], st["stop_ticks"]), (0.4, 10))
        self.assertAlmostEqual(st["stop_dist"], 99.98 * 0.004)
        self.assertAlmostEqual(x["stop0"], self.STOP)
        self.assertEqual(x["shadow_wall_exit"]["reason"], "WALL_BROKEN")   # 99.59 was already beyond the wall

    def test_hard_stop_ticks_bind_on_coarse_tick(self):
        # entry 0.998, tick 0.001: 0.4% = 0.003992 < 10 ticks = 0.010 -> stop 0.988; size $1 / 0.010 = 100
        e, s = engine()
        e.set_universe({"CTUSDT": {"tick": 0.001, "step": 1, "min_notional": 5}}, ["CTUSDT"])
        for k in range(31):
            e.on_depth("CTUSDT", T_OUT + k * 0.1, *coarse_book(1.000, 0.997))
        o = e.orders["CTUSDT"]
        self.assertEqual((round(o["stop"], 6), o["qty"]), (0.988, 100))

    def test_breakeven_then_trail(self):
        # entry 99.98, tick 0.01: BE = max(0.15% = 0.14997, 5 ticks) -> +0.15; trail = max(0.3% of best, 8 ticks)
        e, s = engine()
        p = filled_long(e)
        e.on_trade(SYM, T_OUT + 5, 100.12, 1, False)        # +0.14: not yet
        self.assertFalse(p["be"])
        e.on_trade(SYM, T_OUT + 6, 100.13, 1, False)        # +0.15: stop to entry
        self.assertTrue(p["be"])
        self.assertAlmostEqual(p["stop"], 99.98)
        e.on_trade(SYM, T_OUT + 7, 100.30, 1, False)        # trail 100.30 - 0.3009 = 99.9991 (just above entry)
        self.assertAlmostEqual(p["stop"], 100.30 - 100.30 * 0.003)
        e.on_trade(SYM, T_OUT + 7.5, 100.60, 1, False)
        self.assertAlmostEqual(p["stop"], 100.60 - 100.60 * 0.003)
        e.on_trade(SYM, T_OUT + 8, 100.40, 1, True)         # pullback: stop never moves down
        self.assertAlmostEqual(p["stop"], 100.60 - 100.60 * 0.003)
        self.assertEqual(p["exit_pending"], "")
        e.on_depth(SYM, T_OUT + 8.5, *book(100.40))         # wall gone after breakeven: no longer an exit
        self.assertEqual(p["exit_pending"], "")
        e.on_trade(SYM, T_OUT + 9, 100.29, 1, True)         # <= 100.2982
        self.assertEqual(p["exit_pending"], "TRAIL_STOP")
        e.on_trade(SYM, T_OUT + 9.1, 100.28, 1, True)
        x = self.exit_of(s)
        self.assertEqual((x["reason"], x["exit_price"]), ("TRAIL_STOP", 100.28))
        st = x["settings"]
        self.assertEqual((st["be_pct"], st["be_ticks"], st["trail_pct"], st["trail_ticks"]), (0.15, 5, 0.3, 8))
        self.assertAlmostEqual(st["be_dist"], 0.14997)
        self.assertAlmostEqual(st["trail_dist"], 100.60 * 0.003)

    def test_tick_minimums_bind_on_coarse_tick(self):
        # entry 0.998, tick 0.001: BE = max(0.15% = 0.0015, 5 ticks = 0.005) -> 0.005; trail 8 ticks = 0.008
        e, s = engine()
        e.set_universe({"CTUSDT": {"tick": 0.001, "step": 1, "min_notional": 5}}, ["CTUSDT"])
        for k in range(31):
            e.on_depth("CTUSDT", T_OUT + k * 0.1, *coarse_book(1.000, 0.997))
        e.on_trade("CTUSDT", T_OUT + 3.2, 0.997, 1, True)   # filled at 0.998
        p = e.pos["CTUSDT"]
        e.on_trade("CTUSDT", T_OUT + 4, 1.002, 1, False)    # +0.004 (0.4%!) but only 4 ticks: no breakeven
        self.assertFalse(p["be"])
        e.on_trade("CTUSDT", T_OUT + 5, 1.003, 1, False)    # 5 ticks
        self.assertTrue(p["be"])
        e.on_trade("CTUSDT", T_OUT + 6, 1.010, 1, False)    # trail = 1.010 - max(0.00303, 0.008) = 1.002
        self.assertAlmostEqual(p["stop"], 1.002)
        e.on_trade("CTUSDT", T_OUT + 7, 1.002, 1, True)
        e.on_trade("CTUSDT", T_OUT + 7.1, 1.001, 1, True)
        self.assertEqual((s.last("BJ_EXIT")["reason"], s.last("BJ_EXIT")["exit_price"]), ("TRAIL_STOP", 1.001))
        self.assertAlmostEqual(s.last("BJ_EXIT")["settings"]["trail_dist"], 0.008)

    def test_breakeven_stop(self):
        e, s = engine()
        filled_long(e)
        e.on_trade(SYM, T_OUT + 5, 100.13, 1, False)
        e.on_trade(SYM, T_OUT + 6, 99.98, 1, True)
        e.on_trade(SYM, T_OUT + 6.1, 99.97, 1, True)
        self.assertEqual((self.exit_of(s)["reason"], self.exit_of(s)["exit_price"]), ("BE_STOP", 99.97))

    def test_time_stop(self):
        e, s = engine()
        p = filled_long(e)
        e.on_timer(p["fill_ts"] + 299)
        self.assertEqual(p["exit_pending"], "")
        e.on_timer(p["fill_ts"] + 300)
        self.assertEqual(p["exit_pending"], "TIME_STOP")
        e.on_trade(SYM, p["fill_ts"] + 301, 100.00, 1, False)
        self.assertEqual(self.exit_of(s)["reason"], "TIME_STOP")

    def test_no_time_stop_after_breakeven(self):
        e, s = engine()
        p = filled_long(e)
        e.on_trade(SYM, T_OUT + 5, 100.13, 1, False)
        e.on_timer(p["fill_ts"] + 400)
        self.assertEqual(p["exit_pending"], "")

    def test_short_mirrored(self):
        e, s = engine()
        for k in range(31):
            e.on_depth(SYM, T_OUT + k * 0.1, *book(100.00, 100.03, side="ask"))
        e.on_trade(SYM, T_OUT + 3.2, 100.03, 1, False)      # filled at 100.02
        p = e.pos[SYM]
        e.on_trade(SYM, T_OUT + 3.9, 99.87, 1, True)        # -0.1499%: not yet
        self.assertFalse(p["be"])
        e.on_trade(SYM, T_OUT + 4, 99.86, 1, True)          # -0.16% -> breakeven
        self.assertTrue(p["be"])
        e.on_trade(SYM, T_OUT + 5, 99.50, 1, True)          # trail 99.50 + 0.2985 = 99.7985
        self.assertAlmostEqual(p["stop"], 99.50 + 99.50 * 0.003)
        e.on_trade(SYM, T_OUT + 6, 99.80, 1, False)         # >= 99.7985 -> stop hit
        e.on_trade(SYM, T_OUT + 6.1, 99.81, 1, False)
        x = self.exit_of(s)
        self.assertEqual((x["reason"], x["exit_price"]), ("TRAIL_STOP", 99.81))
        self.assertGreater(x["gross_pnl_usd"], 0)

    def test_short_hard_stop_and_shadow(self):
        e, s = engine()
        for k in range(31):
            e.on_depth(SYM, T_OUT + k * 0.1, *book(100.00, 100.03, side="ask"))
        e.on_trade(SYM, T_OUT + 3.2, 100.03, 1, False)      # filled at 100.02, stop 100.02 + 0.40008
        p = e.pos[SYM]
        self.assertAlmostEqual(p["stop"], 100.02 * 1.004)
        e.on_trade(SYM, T_OUT + 4, 100.04, 1, False)        # above the ask wall: shadow only
        e.on_trade(SYM, T_OUT + 4.1, 100.05, 1, False)
        self.assertEqual(s.last("BJ_WALL_EXIT_SHADOW")["reason"], "WALL_BROKEN")
        self.assertNotIn("BJ_EXIT", s.names())
        e.on_trade(SYM, T_OUT + 5, 100.43, 1, False)        # >= 100.42008
        e.on_trade(SYM, T_OUT + 5.1, 100.44, 1, False)
        self.assertEqual(s.last("BJ_EXIT")["reason"], "HARD_STOP")

    def test_trades_csv_row(self):
        e, s = engine()
        filled_long(e)
        e.on_depth(SYM, T_OUT + 4, *book(100.00))           # wall gone: shadow
        e.on_trade(SYM, T_OUT + 4.1, 99.95, 1, True)        # shadow priced
        e.on_trade(SYM, T_OUT + 5, 99.50, 1, True)          # hard stop
        e.on_trade(SYM, T_OUT + 5.1, 99.49, 1, True)
        row = s.trades[-1]
        self.assertEqual(len(row), len(bj.TRADE_HEAD + bj.TRADE_TAIL))
        self.assertEqual((row[3], row[4], row[5], row[10], row[19], row[21]),
                         (SYM, "LONG", 25, "HARD_STOP", "BOOK JUMP", "outside"))
        self.assertEqual(row[22], s.last("BJ_WALL_EXIT_SHADOW")["pnl_usd"])
        self.assertLess(row[22], 0)

    def test_trades_csv_row_without_shadow(self):
        e, s = engine()
        p = filled_long(e)
        e.on_timer(p["fill_ts"] + 300)
        e.on_trade(SYM, p["fill_ts"] + 301, 99.99, 1, False)
        self.assertEqual((s.trades[-1][10], s.trades[-1][22]), ("TIME_STOP", ""))
        self.assertIsNone(s.last("BJ_EXIT")["shadow_wall_exit"])


class SuiCase(unittest.TestCase):
    """Live 2026-11-22 15:20:35 IST: SUIUSDT short 462.1 @ 0.6550 under a ~$661k ask wall at 0.6551 was filled
    and exited 0.1 s later with WALL_GONE. Depth and trades come over two sockets, so the fill trade and a book
    snapshot taken BEFORE the fill can arrive in either order; only snapshots after the fill may judge the wall."""
    SUI = "SUIUSDT"
    WALL_Q = 1_009_340.0                                    # x 0.6551 = $661,219 (the size logged at the fill)

    def sui_book(self, wall_q=None, drop_wall=False):
        bids = [(round(0.6549 - i * 0.0001, 4), 5000.0) for i in range(20)]
        asks = [(round(0.6550 + i * 0.0001, 4), 5000.0) for i in range(20)]
        asks = [(p, (self.WALL_Q if wall_q is None else wall_q) if abs(p - 0.6551) < 5e-5 else q) for p, q in asks]
        if drop_wall:
            asks = [x for x in asks if abs(x[0] - 0.6551) > 5e-5]
        return bids, asks

    def filled_short(self):
        e, s = engine()
        e.set_universe({self.SUI: {"tick": 0.0001, "step": 0.1, "min_notional": 5}}, [self.SUI])
        for k in range(31):
            e.on_depth(self.SUI, T_OUT + k * 0.1, *self.sui_book(), xt=1_000_000 + k * 100)
        o = e.orders[self.SUI]
        self.assertEqual((o["side"], o["price"], o["wall_price"]), ("short", 0.655, 0.6551))
        # the fill: an aggressive buy prints at the wall price 0.6551 (through our 0.6550, not beyond the wall)
        e.on_trade(self.SUI, T_OUT + 8.0, 0.6551, 300, False, xt=1_008_000)
        p = e.pos[self.SUI]
        self.assertEqual((p["entry"], p["fill_xt"], p["exit_pending"]), (0.655, 1_008_000, ""))
        return e, s, p

    def test_fill_trade_and_stale_snapshot_do_not_stop(self):
        e, s, p = self.filled_short()
        # a book snapshot taken 50 ms BEFORE the fill arrives after it, without the wall level: ignored
        e.on_depth(self.SUI, T_OUT + 8.05, *self.sui_book(drop_wall=True), xt=1_007_950)
        self.assertEqual(p["exit_pending"], "")
        # trades AT the wall price are not "through" it
        e.on_trade(self.SUI, T_OUT + 8.1, 0.6551, 500, False, xt=1_008_100)
        self.assertEqual(p["exit_pending"], "")
        # post-fill snapshot with the wall still there ($661k): no exit
        e.on_depth(self.SUI, T_OUT + 8.2, *self.sui_book(), xt=1_008_100)
        e.on_trade(self.SUI, T_OUT + 8.3, 0.6550, 10, True, xt=1_008_200)
        self.assertIn(self.SUI, e.pos)
        self.assertNotIn("BJ_EXIT", s.names())

    def test_wall_shrank_after_fill(self):
        e, s, p = self.filled_short()
        e.on_depth(self.SUI, T_OUT + 8.2, *self.sui_book(wall_q=self.WALL_Q * 0.51), xt=1_008_100)   # 51%: hold
        self.assertIsNone(p["shadow"])
        e.on_depth(self.SUI, T_OUT + 8.3, *self.sui_book(wall_q=self.WALL_Q * 0.49), xt=1_008_200)   # 49%
        self.assertEqual((p["shadow"]["reason"], p["exit_pending"]), ("WALL_SHRANK", ""))
        e.on_trade(self.SUI, T_OUT + 8.4, 0.6551, 10, False, xt=1_008_300)
        sh = s.last("BJ_WALL_EXIT_SHADOW")
        self.assertEqual((sh["reason"], sh["price"]), ("WALL_SHRANK", 0.6551))
        self.assertEqual(sh["trigger"]["book_ms_after_fill"], 200)
        self.assertEqual(sh["traded_at_wall_usdt"], round(0.6551 * 10, 2))
        self.assertIn(self.SUI, e.pos)

    def test_wall_gone_after_fill(self):
        e, s, p = self.filled_short()
        e.on_depth(self.SUI, T_OUT + 8.2, *self.sui_book(drop_wall=True), xt=1_008_100)
        self.assertEqual(p["shadow"]["reason"], "WALL_GONE")
        self.assertEqual(p["shadow"]["trigger"]["wall_level_usdt"], 0)

    def test_fill_trade_beyond_wall_does_not_stop_but_next_one_does(self):
        e, s = engine()
        e.set_universe({self.SUI: {"tick": 0.0001, "step": 0.1, "min_notional": 5}}, [self.SUI])
        for k in range(31):
            e.on_depth(self.SUI, T_OUT + k * 0.1, *self.sui_book(), xt=1_000_000 + k * 100)
        e.on_trade(self.SUI, T_OUT + 8.0, 0.6552, 300, False, xt=1_008_000)   # sweeps through: fills us
        p = e.pos[self.SUI]
        self.assertEqual(p["exit_pending"], "")
        e.on_trade(self.SUI, T_OUT + 8.1, 0.6552, 10, False, xt=1_008_100)    # next trade strictly beyond
        self.assertEqual((p["shadow"]["reason"], p["exit_pending"]), ("WALL_BROKEN", ""))


class Sessions(unittest.TestCase):
    def test_in_us_session_helper(self):
        self.assertEqual(engine()[0].session(T_US), "US")
        self.assertEqual(engine()[0].session(T_OUT), "outside")
        sat = datetime(2026, 11, 21, 10, 0, tzinfo=NY).timestamp()
        self.assertEqual(engine()[0].session(sat), "outside")
        close = datetime(2026, 11, 18, 16, 0, tzinfo=NY).timestamp()
        self.assertEqual(engine()[0].session(close), "outside")
        opn = datetime(2026, 11, 18, 9, 30, tzinfo=NY).timestamp()
        self.assertEqual(engine()[0].session(opn), "US")

    def test_everything_tagged(self):
        for t0, tag in ((T_US, "US"), (T_OUT, "outside")):
            e, s = engine()
            e.st["summary_day"] = bj.ist_day(t0)
            filled_long(e, t0)
            e.on_depth(SYM, t0 + 4, *book(100.00))
            e.on_trade(SYM, t0 + 4.1, 99.95, 1, True)
            e.on_trade(SYM, t0 + 4.2, 99.50, 1, True)
            e.on_trade(SYM, t0 + 4.3, 99.50, 1, True)
            for typ in ("BJ_ORDER", "BJ_FILL", "BJ_WALL_EXIT_SHADOW", "BJ_EXIT"):
                self.assertEqual(s.last(typ)["session"], tag)
            self.assertEqual(s.trades[-1][21], tag)
            e.on_timer(t0 + 400)
            self.assertEqual(s.walls[0]["session"], tag)
            self.assertEqual(e.day(t0)["s"][tag]["trades"], 1)


class WallLog(unittest.TestCase):
    def test_pulled_touched_still_there_and_mids(self):
        e, s = engine()
        e.universe = set()                                  # observe only, no orders
        for k in range(31):
            e.on_depth(SYM, T_OUT + k * 0.1, *book(100.00, 99.97))
        e.on_depth(SYM, T_OUT + 13.0, *book(100.00, 99.97))
        e.on_timer(T_OUT + 13.1)                            # +10 s mid
        e.on_depth(SYM, T_OUT + 20, *book(100.00))          # pulled, never touched
        e.on_depth(SYM, T_OUT + 59, *book(100.10))
        e.on_timer(T_OUT + 63.1)                            # +60 s mid from the new book
        e.on_timer(T_OUT + 303.1)
        r = s.walls[-1]
        self.assertEqual((r["outcome"], r["side"]), ("pulled", "bid"))
        self.assertAlmostEqual(r["mid_10s"], 100.005)
        self.assertAlmostEqual(r["mid_60s"], 100.105)
        self.assertAlmostEqual(r["lifetime_s"], 20.0)
        self.assertEqual(r["mid_300s"], "")                 # no fresh book at +300 s

        e2, s2 = engine()
        e2.universe = set()
        for k in range(31):
            e2.on_depth(SYM, T_OUT + k * 0.1, *book(100.00, 99.97))
        e2.on_trade(SYM, T_OUT + 5, 99.97, 1, True)        # touched, held
        e2.on_depth(SYM, T_OUT + 300, *book(100.00, 99.97))
        e2.on_timer(T_OUT + 303.1)
        self.assertEqual((s2.walls[-1]["outcome"], s2.walls[-1]["touched"], s2.walls[-1]["broke_through"]),
                         ("still there", 1, 0))
        d = e2.day(T_OUT)["s"]["outside"]
        self.assertEqual((d["walls"], d["walls_done"], d["touched_held"], d["pulled"]), (1, 1, 1, 0))

        e3, s3 = engine()
        e3.universe = set()
        for k in range(31):
            e3.on_depth(SYM, T_OUT + k * 0.1, *book(100.00, 99.97))
        e3.on_trade(SYM, T_OUT + 5, 99.96, 1, True)        # traded through
        e3.on_depth(SYM, T_OUT + 6, *book(99.96))
        e3.on_timer(T_OUT + 303.1)
        self.assertEqual((s3.walls[-1]["outcome"], s3.walls[-1]["broke_through"]), ("touched", 1))

    def test_bj_order_column(self):
        e, s = engine()
        filled_long(e)
        e.on_timer(T_OUT + 400)
        self.assertEqual(s.walls[-1]["bj_order"], "filled")


class KillSwitch(unittest.TestCase):
    def lose(self, e, t):
        e.walls.clear()
        filled_long(e, t)
        e.on_trade(SYM, t + 4, 98.00, 1, True)              # hard stop
        e.on_trade(SYM, t + 4.1, 98.00, 1, True)            # filled at 98.00: -1.98 x 2.5 = about -5% of the book

    def test_no_pause_on_losses(self):
        """No pause after losses in a row or a big daily loss: only a stale feed pauses."""
        e, s = engine()
        for i in range(7):
            self.lose(e, T_OUT + i * 100)
        self.assertEqual(s.names().count("BJ_EXIT"), 7)
        self.assertLess(e.day(T_OUT)["pnl"], -0.2 * 100)
        self.assertNotIn("BJ_KILL", s.names())
        self.assertTrue(e.can_enter(T_OUT + 800))
        e.walls.clear()
        self.assertIsNotNone(place_long(e, T_OUT + 800))
        self.assertNotIn("day_loss_pct", e.c)
        self.assertNotIn("max_loss_streak", e.c)

    def test_kill_closes_open_position(self):
        e, s = engine()
        filled_long(e)
        e.kill(T_OUT + 5, "test")
        e.on_trade(SYM, T_OUT + 5.1, 99.99, 1, False)
        self.assertEqual(s.last("BJ_EXIT")["reason"], "KILL")

    def test_stale_feed(self):
        e, s = engine()
        place_long(e)
        e.feed_down(T_OUT + 10, "trade stream")
        e.feed_down(T_OUT + 11, "trade stream")             # notified once
        self.assertEqual(s.last("BJ_CANCEL")["reason"], "feed_stale")
        self.assertEqual(sum("no market data" in m for m in s.msgs), 1)
        self.assertFalse(e.can_enter(T_OUT + 12))
        e.on_timer(T_OUT + 69)
        self.assertNotIn("BJ_KILL", s.names())
        e.on_timer(T_OUT + 70)
        self.assertEqual(s.last("BJ_KILL")["state"], "paused")
        e.feed_up(T_OUT + 80)
        self.assertEqual(s.last("BJ_FEED")["state"], "ok")
        self.assertFalse(e.can_enter(T_OUT + 81))           # still in the 6 h pause

    def test_stale_recovers_in_time(self):
        e, s = engine()
        e.feed_down(T_OUT + 10, "depth stream")
        e.feed_up(T_OUT + 40)
        e.on_timer(T_OUT + 80)
        self.assertNotIn("BJ_KILL", s.names())
        self.assertTrue(e.can_enter(T_OUT + 81))

    def test_bot_pause_stands_down(self):
        e, s = engine()
        place_long(e)
        e.bot_paused = lambda: True
        e.on_timer(T_OUT + 5)
        self.assertEqual(s.last("BJ_CANCEL")["reason"], "bot_paused")


class Misc(unittest.TestCase):
    def test_refuse_reason(self):
        self.assertEqual(bj.refuse_reason({"dry_run": True, "exchange": "binance"}), "")
        self.assertIn("DRY_RUN", bj.refuse_reason({"dry_run": False, "exchange": "binance"}))
        self.assertIn("EXCHANGE=delta", bj.refuse_reason({"dry_run": True, "exchange": "delta"}))

    def test_restart_closes_leftover_position(self):
        st = bj.new_state(100)
        st["positions"][SYM] = {"sym": SYM, "side": "long", "entry": 99.98, "qty": 3.0, "stop0": 99.96,
                                "wall_price": 99.97, "wall_ref_usdt": 39988, "fill_ts": T_OUT, "best": 99.98,
                                "be": False, "stop": 99.96, "session": "outside", "exit_pending": "",
                                "tick": 0.01, "step": 0.1, "wall_mult": 8.0}
        e, s = engine(state=st)
        self.assertIn(SYM, e.subscriptions())
        e.on_trade(SYM, T_OUT + 100, 99.99, 1, False)
        self.assertEqual(s.last("BJ_EXIT")["reason"], "RESTART")

    def test_daily_summary_at_ist_midnight(self):
        e, s = engine()
        e.st["summary_day"] = "2026-11-17"
        self.lose = KillSwitch.lose
        e.st["day"]["2026-11-17"] = {"start_balance": 100, "pnl": -0.5, "trades": 2, "wins": 1, "s": {
            "US": {"walls": 10, "walls_done": 10, "pulled": 6, "touched_held": 2, "orders": 3, "fills": 2,
                   "trades": 2, "wins": 1, "pnl": -0.5}}}
        e.on_timer(datetime(2026, 11, 18, 0, 0, 1, tzinfo=bj.IST).timestamp())
        msg = s.msgs[-1]
        self.assertIn("BOOK JUMP day 2026-11-17", msg)
        self.assertIn("US: walls 10 | pulled 60% | touched+held 20% | orders 3 | fills 2 | trades 2 | win 50% | P&L $-0.50", msg)
        self.assertIn("outside: walls 0", msg)
        self.assertEqual(e.st["summary_day"], "2026-11-18")

    def test_universe_keeps_open_symbols(self):
        e, s = engine()
        filled_long(e)
        subs = e.set_universe({}, ["OTHERUSDT"])
        self.assertEqual(subs, {"OTHERUSDT", SYM})


class Files(unittest.TestCase):
    def runner(self, d):
        r = bj.BookJump.__new__(bj.BookJump)               # file helpers only, no threads / network
        r.walls_file = os.path.join(d, "walls.csv")
        return r

    def test_walls_file_created_with_header_once(self):
        import csv
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            r = self.runner(d)
            r.ensure_walls_file()
            with open(r.walls_file) as fh:
                self.assertEqual(list(csv.reader(fh)), [bj.WALL_COLS])
            r.wall_row(["x"] * len(bj.WALL_COLS))             # first real row: no second header
            r.ensure_walls_file()                              # restart: existing rows kept
            with open(r.walls_file) as fh:
                rows = list(csv.reader(fh))
            self.assertEqual((rows[0], len(rows)), (bj.WALL_COLS, 2))


class StartGrace(unittest.TestCase):
    def test_start_resets_the_stale_clock(self):
        import threading
        from unittest import mock
        r = bj.BookJump.__new__(bj.BookJump)
        r.lock, r.subs = threading.RLock(), set()
        r.last_msg = {"depth": 1.0, "trade": 1.0}           # set by prepare(), long before the sockets open
        with mock.patch("threading.Thread"):
            r.start()
        self.assertGreater(min(r.last_msg.values()), 1e9)


class Persistence(unittest.TestCase):
    HEAD = ("open_time_ist,close_time_ist,mode,symbol,side,contracts,entry,stop_loss,take_profit,exit_price,"
            "exit_reason,gross_pnl_usd,est_fees_usd,net_pnl_usd,net_pnl_inr,r_multiple,rsi_4h,exit_estimated,"
            "ob_ratio,strategy,book_balance_usd")

    def runner(self, d, store):
        r = bj.BookJump.__new__(bj.BookJump)
        r.trades_file, r.walls_file = os.path.join(d, "trades.csv"), os.path.join(d, "walls.csv")
        r.old_state_file = os.path.join(d, "bookjump_state.json")
        r.c = bj.load_cfg({})
        r.cleared, r.st_a = [], None
        r.book_get = lambda name="BOOK JUMP": store.get(name)
        r.book_publish = lambda snap, name="BOOK JUMP": store.__setitem__(name, snap)
        return r

    def check_header(self, first_line):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            r = self.runner(d, {})
            rows = ["2026-11-20 10:00 IST,2026-11-20 11:00 IST,PAPER,ETHUSDT,LONG,1,1,1,1,1,X,1,0,1,85,1,,True,,POC RETURN,120\r\n"] * 3
            with open(r.trades_file, "w", newline="") as fh:
                fh.write(first_line)
                fh.writelines(rows)
            r.ensure_trades_header()
            r.ensure_trades_header()                         # idempotent over restarts
            with open(r.trades_file, newline="") as fh:
                got = fh.readlines()
            self.assertEqual(got[0], self.HEAD + ",session,shadow_exit_pnl\r\n")
            self.assertEqual(got[1:], rows)                   # rows kept byte for byte

    def test_header_added_once(self):
        self.check_header(self.HEAD + "\r\n")

    def test_header_duplicate_repaired(self):
        self.check_header(self.HEAD + ",session,session\r\n")

    def test_header_from_session_only(self):            # the header written by the version deployed on 22 Nov
        self.check_header(self.HEAD + ",session\r\n")

    def test_header_with_lf_endings(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            r = self.runner(d, {})
            with open(r.trades_file, "w", newline="") as fh:
                fh.write(self.HEAD + ",session,session\nrow\n")
            r.ensure_trades_header()
            with open(r.trades_file, newline="") as fh:
                self.assertEqual(fh.read(), self.HEAD + ",session,shadow_exit_pnl\nrow\n")

    def test_book_round_trip_through_state(self):
        import tempfile
        store = {}
        with tempfile.TemporaryDirectory() as d:
            r = self.runner(d, store)
            r.st = r.load_state()
            r.st["balance"], r.st["trades"] = 101.25, 4
            r.st["day"]["2026-11-22"] = {"start_balance": 100, "pnl": 1.25, "trades": 4, "wins": 3, "s": {}}
            r.st["positions"][SYM] = {"sym": SYM, "side": "long"}
            r.save()
            snap = store["BOOK JUMP"]
            self.assertIsNot(snap, r.st)                      # a private copy, safe for the main thread's save
            r.st["balance"] = 0
            self.assertEqual(snap["balance"], 101.25)
            self.assertEqual((snap["open_trade"], snap["strategy"]), (None, "BOOK JUMP"))
            r2 = self.runner(d, store)                        # restart: restored from state.json
            st2 = r2.load_state()
            self.assertEqual((st2["balance"], st2["trades"], list(st2["positions"])), (101.25, 4, [SYM]))
            self.assertEqual(st2["day"]["2026-11-22"]["pnl"], 1.25)

    def test_old_loss_pause_cleared_at_start(self):
        import tempfile
        import time as _t
        store = {"BOOK JUMP": dict(bj.new_state(100), paused_until=_t.time() + 3600, loss_streak=5)}
        with tempfile.TemporaryDirectory() as d:
            r = self.runner(d, store)
            st = r.load_state()
            self.assertEqual(st["paused_until"], 0)
            self.assertNotIn("loss_streak", st)
            self.assertEqual(r.cleared[0][0], "BOOK JUMP")

    def test_migrates_old_state_file(self):
        import tempfile
        store = {}
        with tempfile.TemporaryDirectory() as d:
            r = self.runner(d, store)
            with open(r.old_state_file, "w") as fh:
                json.dump({"balance": 99.5, "start": 100, "trades": 2, "wins": 0, "summary_day": "2026-11-22",
                           "days": {"2026-11-22": {"pnl": -0.5}}, "positions": {}}, fh)
            st = r.load_state()
            self.assertEqual((st["balance"], st["day"], st["summary_day"]), (99.5, {"2026-11-22": {"pnl": -0.5}}, "2026-11-22"))
            self.assertNotIn("days", st)


class RecordedStream(unittest.TestCase):
    def test_replay(self):
        """A recorded-format Binance stream: wall -> order -> queue fill -> breakeven -> trail -> exit."""
        e, s = engine()
        path = os.path.join(os.path.dirname(__file__), "fixtures", "bj_stream.jsonl")
        with open(path) as fh:
            for line in fh:
                r = json.loads(line)
                t = T_OUT + r["t"]
                msg = bj.parse_msg(r["raw"])
                if msg[0] == "depth":
                    e.on_depth(msg[1], t, msg[2], msg[3], msg[4])
                else:
                    e.on_trade(msg[1], t, msg[2], msg[3], msg[4], msg[5])
                e.on_timer(t)
        self.assertEqual([n for n in s.names() if n.startswith("BJ_")], ["BJ_ORDER", "BJ_FILL", "BJ_EXIT"])
        o, f, x = s.last("BJ_ORDER"), s.last("BJ_FILL"), s.last("BJ_EXIT")
        self.assertEqual((o["price"], o["queue_ahead"], o["wall_price"]), (99.98, 10.0, 99.97))
        self.assertEqual(f["price"], 99.98)
        self.assertEqual((x["reason"], x["exit_price"], x["breakeven_reached"]), ("TRAIL_STOP", 99.99, True))
        self.assertAlmostEqual(x["stop"], 100.31 - 100.31 * 0.003)
        self.assertAlmostEqual(e.st["balance"], 100 + 0.01 * 2.5 - bj.fees_usd(99.98, 99.99, 2.5, e.c), places=5)
        self.assertEqual(len([m for m in s.msgs if m.startswith("BJ ")]), 2)    # fill + exit to Telegram


if __name__ == "__main__":
    unittest.main()
