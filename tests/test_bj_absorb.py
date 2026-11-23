"""BJ ABSORB unit tests: setup on the first touch, absorption trigger + 2 s confirm, drops, order rules, exits,
walls.csv columns, reporting. Run: venv/bin/python -m unittest discover -s tests -v"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bookjump as bj  # noqa: E402
from bot import Bot  # noqa: E402
from test_bookjump import META, SYM, T_OUT, T_US, Sink, book, without  # noqa: E402

WALL = 99.97
SIZE0 = WALL * 400                      # $39,988 resting at the bid wall in book(..., wall_q=400)


def engines(bj_max_open=0, acfg=None):
    """BOOK JUMP (market engine, kept out of the way with max_open 0 unless asked) + BJ ABSORB on the same feed."""
    c = bj.load_cfg({})
    c["max_open"] = bj_max_open
    sm, sa = Sink(), Sink()
    m = bj.Engine(c, Bot.in_us_session, sm, bj.new_state(100))
    ca = bj.load_absorb_cfg(c, acfg or {})
    a = bj.Absorb(ca, Bot.in_us_session, sa, bj.new_state(100, bj.ABSORB), market=m)
    m.set_universe(META, [SYM])
    m.st["summary_day"] = a.st["summary_day"] = bj.ist_day(T_OUT)
    return m, a, sm, sa


def seen_wall(m, t0=T_OUT, best=100.00):
    for k in range(31):                                   # persists 3 s -> seen
        m.on_depth(SYM, t0 + k * 0.1, *book(best, WALL))


def hit(m, t, qty, px=WALL, aggressive=True):
    """A trade at the bid wall; aggressive=True = seller-aggressor (buyer is maker) = hits the bid wall."""
    m.on_trade(SYM, t, px, qty, aggressive)


class Setup(unittest.TestCase):
    def test_setup_starts_at_first_touch_of_a_seen_wall(self):
        m, a, sm, sa = engines()
        for k in range(20):                                # not yet seen (2 s)
            m.on_depth(SYM, T_OUT + k * 0.1, *book(100.00, WALL))
        hit(m, T_OUT + 2.0, 1)
        self.assertEqual(a.setups, {})                     # touched before it was seen: no setup
        seen_wall(m)
        hit(m, T_OUT + 3.5, 10)
        s = sa.last("BJA_SETUP")
        self.assertEqual((s["wall_side"], s["wall_price"], s["session"]), ("bid", WALL, "outside"))
        self.assertAlmostEqual(s["wall_usdt_at_touch"], round(SIZE0, 2))
        self.assertAlmostEqual(s["need_usdt"], round(SIZE0 * 0.3, 2))
        (st,) = a.setups.values()
        self.assertAlmostEqual(st["vol"], WALL * 10)       # the first touch counts when it hits the wall

    def test_only_trades_hitting_the_wall_count(self):
        m, a, sm, sa = engines()
        seen_wall(m)
        hit(m, T_OUT + 3.5, 10)
        hit(m, T_OUT + 3.6, 500, aggressive=False)         # buyer-aggressor at the bid wall price: not a hit
        hit(m, T_OUT + 3.7, 5, px=99.98)                   # above the wall: not at it
        (st,) = a.setups.values()
        self.assertAlmostEqual(st["vol"], WALL * 10)

    def test_ask_wall_counts_aggressive_buys(self):
        m, a, sm, sa = engines()
        for k in range(31):
            m.on_depth(SYM, T_OUT + k * 0.1, *book(100.00, 100.03, side="ask"))
        m.on_trade(SYM, T_OUT + 3.5, 100.03, 20, False)    # aggressive buy into the ask wall
        m.on_trade(SYM, T_OUT + 3.6, 100.03, 99, True)     # seller-aggressor at the ask: not a hit
        (st,) = a.setups.values()
        self.assertAlmostEqual(st["vol"], 100.03 * 20)


class Trigger(unittest.TestCase):
    def absorb(self, m, t=T_OUT + 3.5):
        seen_wall(m)
        hit(m, t, 50)                                       # 12.5%
        hit(m, t + 1, 75)                                   # 125 x 99.97 = 31.25% of the wall at first touch

    def test_absorbed_after_confirm_then_order(self):
        m, a, sm, sa = engines()
        self.absorb(m)
        self.assertEqual(next(iter(a.setups.values()))["state"], "confirm")
        m.on_timer(T_OUT + 4.5 + 1.9)
        self.assertNotIn("BJA_ABSORBED", sa.names())       # 2 s not over yet
        m.on_depth(SYM, T_OUT + 6.0, *book(100.00, WALL))   # wall still there
        m.on_timer(T_OUT + 4.5 + 2.0)
        x = sa.last("BJA_ABSORBED")
        self.assertAlmostEqual(x["absorbed_pct"], 31.2)
        o = sa.last("BJA_ORDER")
        self.assertEqual((o["side"], o["price"], o["wall_price"], o["queue_ahead"]), ("long", 99.98, WALL, 10.0))
        self.assertAlmostEqual(o["stop"], 99.98 - 99.98 * 0.004)   # same hard stop rule as BOOK JUMP
        self.assertAlmostEqual(o["qty"], 2.5)
        self.assertEqual(sa.names(), ["BJA_SETUP", "BJA_ABSORBED", "BJA_ORDER"])
        self.assertEqual(sm.names(), [])                    # BOOK JUMP (max_open 0 here) untouched

    def test_needs_30pct(self):
        m, a, sm, sa = engines()
        seen_wall(m)
        hit(m, T_OUT + 3.5, 119)                            # 29.75%
        m.on_timer(T_OUT + 10)
        self.assertEqual(next(iter(a.setups.values()))["state"], "watch")
        hit(m, T_OUT + 11, 1)                               # 30.0%
        self.assertEqual(next(iter(a.setups.values()))["state"], "confirm")

    def test_timeout_after_60s(self):
        m, a, sm, sa = engines()
        seen_wall(m)
        hit(m, T_OUT + 3.5, 50)
        m.on_timer(T_OUT + 63.5)
        self.assertTrue(a.setups)
        m.on_timer(T_OUT + 63.6)
        self.assertEqual(sa.last("BJA_DROP")["reason"], "timeout")
        hit(m, T_OUT + 64, 500)                             # too late, and one setup per wall
        self.assertEqual(a.setups, {})

    def test_dropped_pulled(self):
        m, a, sm, sa = engines()
        self.absorb(m)
        m.on_depth(SYM, T_OUT + 5.0, *without(book(100.00), WALL))   # level gone
        d = sa.last("BJA_DROP")
        self.assertEqual(d["reason"], "dropped_pulled")
        m.on_timer(T_OUT + 7)
        self.assertNotIn("BJA_ORDER", sa.names())

    def test_dropped_shrank_below_half(self):
        m, a, sm, sa = engines()
        self.absorb(m)
        m.on_depth(SYM, T_OUT + 5.0, *book(100.00, WALL, wall_q=210))   # 52%: still fine
        self.assertTrue(a.setups)
        m.on_depth(SYM, T_OUT + 5.1, *book(100.00, WALL, wall_q=190))   # 47.5%: 210 gone, 125 of it traded
        self.assertEqual(sa.last("BJA_DROP")["reason"], "dropped_shrank")

    def test_mostly_cancelled_counts_as_pulled(self):
        m, a, sm, sa = engines()
        self.absorb(m)
        m.on_depth(SYM, T_OUT + 5.0, *book(100.00, WALL, wall_q=10))    # 390 gone, only 125 traded
        d = sa.last("BJA_DROP")
        self.assertEqual(d["reason"], "dropped_pulled")
        self.assertAlmostEqual(d["cancelled_usdt"], round(WALL * 265, 2))

    def test_dropped_broke(self):
        m, a, sm, sa = engines()
        seen_wall(m)
        hit(m, T_OUT + 3.5, 50)
        hit(m, T_OUT + 4.0, 1, px=99.96)                    # printed beyond the wall
        self.assertEqual(sa.last("BJA_DROP")["reason"], "dropped_broke")

    def test_book_jump_ending_the_wall_does_not_drop_the_setup(self):
        """At $24k (60% of the touch size) the level fails the 5x/$25k wall test, so BOOK JUMP's tracker ends the
        wall, but BJ ABSORB judges it against its size at the first touch: still >= 50%, setup continues."""
        m, a, sm, sa = engines()
        seen_wall(m)
        hit(m, T_OUT + 3.5, 125)
        m.on_depth(SYM, T_OUT + 4.0, *book(100.00, WALL, wall_q=240))
        self.assertNotIn(("bid", round(WALL / 0.01)), m.walls[SYM])
        m.on_timer(T_OUT + 5.6)
        self.assertEqual(sa.last("BJA_ABSORBED")["wall_usdt_now"], round(WALL * 240, 2))
        self.assertIn("BJA_ORDER", sa.names())

    def test_confirm_s_and_pct_from_env(self):
        m, a, sm, sa = engines(acfg={"BJA_ABSORB_PCT": "10", "BJA_CONFIRM_S": "0"})
        seen_wall(m)
        hit(m, T_OUT + 3.5, 41)                             # 10.25%: absorbed at once with no confirm time
        self.assertIn("BJA_ORDER", sa.names())


class AbsorbOrders(unittest.TestCase):
    def ordered(self):
        m, a, sm, sa = engines()
        Trigger().absorb(m)
        m.on_timer(T_OUT + 6.5)
        return m, a, sm, sa, a.orders[SYM]

    def test_cancel_after_10s(self):
        m, a, sm, sa, o = self.ordered()
        m.on_timer(o["placed_ts"] + 9.9)
        self.assertIn(SYM, a.orders)
        m.on_timer(o["placed_ts"] + 10)
        self.assertEqual(sa.last("BJA_CANCEL")["reason"], "timeout")

    def test_cancel_more_than_3_ticks_away(self):
        m, a, sm, sa, o = self.ordered()
        m.on_depth(SYM, T_OUT + 7, *book(100.01, WALL))     # best bid 3 ticks above 99.98: kept
        self.assertIn(SYM, a.orders)
        m.on_depth(SYM, T_OUT + 7.1, *book(100.02, WALL))   # 4 ticks
        self.assertEqual(sa.last("BJA_CANCEL")["reason"], "price_away")

    def test_not_cancelled_when_book_jump_ends_the_wall(self):
        m, a, sm, sa, o = self.ordered()
        m.on_depth(SYM, T_OUT + 7, *book(100.00))           # wall pulled: BOOK JUMP's rule, not BJ ABSORB's
        self.assertIn(SYM, a.orders)

    def test_queue_fill_then_hard_stop_and_shadow(self):
        m, a, sm, sa, o = self.ordered()
        hit(m, T_OUT + 7, 10, px=99.98)                     # 10 = queue ahead: not yet
        self.assertIn(SYM, a.orders)
        hit(m, T_OUT + 7.1, 0.5, px=99.98)                  # beyond the queue: filled at 99.98
        p = a.pos[SYM]
        self.assertEqual(sa.last("BJA_FILL")["price"], 99.98)
        m.on_depth(SYM, T_OUT + 8, *book(100.00))           # wall down to 10 (2.5%): shadow only
        self.assertEqual(p["shadow"]["reason"], "WALL_SHRANK")
        hit(m, T_OUT + 8.1, 1, px=99.90)
        self.assertEqual(sa.last("BJA_WALL_EXIT_SHADOW")["price"], 99.90)
        hit(m, T_OUT + 9, 1, px=99.55)                      # hard stop 99.58008
        hit(m, T_OUT + 9.1, 1, px=99.54)
        x = sa.last("BJA_EXIT")
        self.assertEqual((x["reason"], x["exit_price"], x["session"]), ("HARD_STOP", 99.54, "outside"))
        row = sa.trades[-1]
        self.assertEqual((row[19], row[10], row[21]), ("BJ ABSORB", "HARD_STOP", "outside"))
        self.assertEqual(row[22], sa.last("BJA_WALL_EXIT_SHADOW")["pnl_usd"])
        self.assertAlmostEqual(a.st["balance"], 100 + x["net_pnl_usd"], places=3)
        self.assertEqual(m.st["balance"], 100)              # BOOK JUMP's book untouched
        self.assertNotIn("BJA_KILL", sa.names())

    def test_one_per_symbol_and_max_open(self):
        m, a, sm, sa, o = self.ordered()
        for k in range(31):                                  # a second wall on the same coin, absorbed too
            m.on_depth(SYM, T_OUT + 10 + k * 0.1, *book(100.00, 99.96))
        m.on_trade(SYM, T_OUT + 13.5, 99.96, 130, True)     # also fills the BJ ABSORB order at 99.98
        self.assertIn(SYM, a.pos)
        m.on_timer(T_OUT + 15.6)
        self.assertEqual(sa.last("BJA_DROP")["reason"], "skipped_busy")


class WallsCsv(unittest.TestCase):
    def test_columns_entered_and_waits_for_result(self):
        m, a, sm, sa = engines()
        Trigger().absorb(m)
        m.on_timer(T_OUT + 6.5)
        m.on_timer(T_OUT + 400)
        r = sm.walls[-1]                                    # rows are written by the market engine
        self.assertEqual((r["absorb_result"], r["absorbed_pct"], r["bj_order"]), ("entered", 31.2, ""))
        self.assertAlmostEqual(r["absorbed_usdt"], round(WALL * 125, 2))

    def test_row_waits_for_running_setup(self):
        m, a, sm, sa = engines()
        seen_wall(m)
        for k in range(29):                                 # keep the book fresh up to +300 s
            m.on_depth(SYM, T_OUT + 10 + k * 10, *book(100.00, WALL))
        hit(m, T_OUT + 290, 50)                             # setup starts 290 s after the wall was seen
        m.on_timer(T_OUT + 303.1)
        self.assertEqual(sm.walls, [])                      # still watching: row not written yet
        m.on_timer(T_OUT + 351)                             # 61 s later: timeout
        self.assertEqual((sm.walls[-1]["absorb_result"], sm.walls[-1]["absorbed_pct"]), ("timeout", 12.5))

    def test_untouched_wall_has_empty_columns(self):
        m, a, sm, sa = engines()
        seen_wall(m)
        m.on_timer(T_OUT + 400)
        r = sm.walls[-1]
        self.assertEqual((r["absorbed_usdt"], r["absorbed_pct"], r["absorb_result"]), ("", "", ""))

    def test_dropped_results(self):
        for result, act in (("dropped_pulled", lambda m: m.on_depth(SYM, T_OUT + 5, *book(100.00))),
                            ("dropped_shrank", lambda m: (hit(m, T_OUT + 4, 150),
                                                          m.on_depth(SYM, T_OUT + 5, *book(100.00, WALL, wall_q=180)))),
                            ("dropped_broke", lambda m: hit(m, T_OUT + 5, 1, px=99.96))):
            m, a, sm, sa = engines()
            seen_wall(m)
            hit(m, T_OUT + 3.5, 50)
            act(m)
            m.on_timer(T_OUT + 400)
            self.assertEqual(sm.walls[-1]["absorb_result"], result)


class SessionsAndReports(unittest.TestCase):
    def test_events_tagged_us(self):
        m, a, sm, sa = engines()
        a.st["summary_day"] = m.st["summary_day"] = bj.ist_day(T_US)
        seen_wall(m, T_US)
        hit(m, T_US + 3.5, 125)
        m.on_timer(T_US + 5.6)
        hit(m, T_US + 6, 11, px=99.98)
        for typ in ("BJA_SETUP", "BJA_ABSORBED", "BJA_ORDER", "BJA_FILL"):
            self.assertEqual(sa.last(typ)["session"], "US", typ)
        m.on_timer(T_US + 400)                              # time stop
        hit(m, T_US + 401, 1, px=99.99)
        self.assertEqual(sa.last("BJA_EXIT")["session"], "US")
        d = a.day(T_US)["s"]["US"]
        self.assertEqual((d["setups"], d["absorbed"], d["orders"], d["fills"], d["trades"]), (1, 1, 1, 1, 1))

    def test_day_summary_and_status(self):
        from datetime import datetime
        m, a, sm, sa = engines()
        a.st["summary_day"] = "2026-11-17"
        a.st["day"]["2026-11-17"] = {"start_balance": 100, "pnl": 0.4, "trades": 1, "wins": 1, "s": {
            "outside": {"setups": 7, "absorbed": 2, "dropped": 5, "orders": 2, "fills": 1, "trades": 1, "wins": 1,
                        "pnl": 0.4}}}
        m.on_timer(datetime(2026, 11, 18, 0, 0, 1, tzinfo=bj.IST).timestamp())
        msg = sa.msgs[-1]
        self.assertIn("BJ ABSORB day 2026-11-17 (IST) [PAPER]", msg)
        self.assertIn("outside: setups 7 | absorbed 2 | dropped 5 | orders 2 | fills 1 | trades 1 | win 100% | "
                      "P&L $+0.40", msg)
        self.assertTrue(a.status_text(T_OUT).startswith("BJ ABSORB [PAPER]: $100.00"))
        self.assertIn("BJ ABSORB (own paper book, not in ALL)", a.report_line())

    def test_stale_feed_pauses_both_and_drops_setups(self):
        m, a, sm, sa = engines()
        seen_wall(m)
        hit(m, T_OUT + 3.5, 50)
        m.feed_down(T_OUT + 10, "depth stream")
        self.assertEqual(sa.last("BJA_DROP")["reason"], "dropped_feed")
        m.on_timer(T_OUT + 70)
        self.assertEqual(sm.last("BJ_KILL")["state"], "paused")
        self.assertEqual(sa.last("BJA_KILL")["state"], "paused")
        self.assertFalse(a.can_enter(T_OUT + 71))

    def test_subscriptions_include_absorb_positions(self):
        m, a, sm, sa, o = AbsorbOrders().ordered()
        self.assertEqual(m.set_universe({}, ["OTHERUSDT"]), {"OTHERUSDT", SYM})

    def test_disabled_by_env(self):
        self.assertFalse(bj.load_absorb_cfg(bj.load_cfg({}), {"BJA_ENABLE": "0"})["enabled"])
        c = bj.load_absorb_cfg(bj.load_cfg({}), {})
        self.assertEqual((c["absorb_pct"], c["confirm_s"], c["window"], c["keep_pct"], c["order_timeout"],
                          c["away_ticks"], c["risk_pct"], c["start_balance"], c["stop_pct"], c["stop_ticks"]),
                         (30, 2, 60, 50, 10, 3, 1, 100, 0.4, 10))


class BothBooksOneWall(unittest.TestCase):
    def test_book_jump_and_absorb_independent(self):
        m, a, sm, sa = engines(bj_max_open=3)
        seen_wall(m)
        self.assertIn(SYM, m.orders)                        # BOOK JUMP jumps the wall as soon as it is seen
        hit(m, T_OUT + 3.5, 125)                            # fills BOOK JUMP (through its 99.98) and absorbs
        self.assertIn(SYM, m.pos)
        m.on_timer(T_OUT + 5.6)
        self.assertIn(SYM, a.orders)                        # BJ ABSORB has its own order on the same coin
        self.assertEqual(sa.last("BJA_ORDER")["wall_price"], WALL)


class RunnerTwoBooks(unittest.TestCase):
    def test_save_publishes_both_books_and_headers(self):
        import csv
        import tempfile
        store = {}
        with tempfile.TemporaryDirectory() as d:
            r = bj.BookJump.__new__(bj.BookJump)
            r.c = bj.load_cfg({})
            r.ca = bj.load_absorb_cfg(r.c, {})
            r.cleared = []
            r.old_state_file = os.path.join(d, "bookjump_state.json")
            r.walls_file, r.trades_file = os.path.join(d, "walls.csv"), os.path.join(d, "trades.csv")
            r.book_get = lambda name="BOOK JUMP": store.get(name)
            r.book_publish = lambda snap, name="BOOK JUMP": store.__setitem__(name, snap)
            r.st_a = None
            r.st = r.load_state()
            r.st_a = r.load_state(bj.ABSORB, 100)
            r.save()
            self.assertEqual((store["BOOK JUMP"]["strategy"], store["BJ ABSORB"]["strategy"]), ("BOOK JUMP", "BJ ABSORB"))
            self.assertIsNot(store["BJ ABSORB"], r.st_a)
            with open(r.walls_file, "w", newline="") as fh:     # the walls.csv header deployed on 22 Nov
                fh.write(",".join(bj.WALL_COLS[:15]) + "\r\n" + ",".join(["x"] * 15) + "\r\n")
            r.ensure_walls_file()
            with open(r.walls_file, newline="") as fh:
                rows = list(csv.reader(fh))
            self.assertEqual(rows[0], bj.WALL_COLS)
            self.assertEqual(rows[1], ["x"] * 15)


if __name__ == "__main__":
    unittest.main()
