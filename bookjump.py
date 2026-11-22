"""
BOOK JUMP - order-book penny-jumping, PAPER ONLY, Binance USDT-M perpetuals
=============================================================================
Started by bot.py when ENABLE_BOOKJUMP=1, and only when the bot is in paper mode (DRY_RUN=1) with EXCHANGE=binance.
Runs 24/7 next to POC RETURN in its own threads with its own book (BJ_START_BALANCE, default 100 USDT paper) and its
own state file (bookjump_state.json), so it never touches the POC book or the bot's state.json.

UNIVERSE  every BJ_UNIVERSE_HOURS (6): up to BJ_MAX_SYMBOLS (20) USDT perpetuals with tick/price between
          BJ_TICK_MIN_PCT (0.01%) and BJ_TICK_MAX_PCT (0.1%) and 24h quote volume > BJ_MIN_VOL_USDT (20M), highest
          volume first. Symbols that drop out keep being watched while BOOK JUMP has an order/position on them.
DATA      Binance futures WebSocket, two connections (Binance serves the streams on separate routes):
          wss://fstream.binance.com/public/stream (<sym>@depth20@100ms) and /market/stream (<sym>@aggTrade).
          Auto-reconnect with backoff. No message for BJ_STALE_SEC (10 s): resting orders cancelled, no new entries
          until data resumes, one Telegram note. Not back within BJ_STALE_KILL_SEC (60 s): kill switch.
WALL      top 5 levels per side; average level size in USDT over those 10 levels. A level among the top 5 is a wall
          if size >= BJ_WALL_MULT (5) x average AND (BJ_WALL_RULE=and|or) size >= BJ_WALL_MIN_USDT (25,000).
          It stays a wall while its level (anywhere in the top 20) still passes the same test. It must persist
          BJ_PERSIST_SEC (3 s) at the same price before it is "seen" (logged to walls.csv) and tradable.
ENTRY     post-only paper order one tick in front of the wall (bid wall -> buy at wall + 1 tick, must be < best ask;
          ask wall -> sell at wall - 1 tick, must be > best bid). One order or position per symbol, at most
          BJ_MAX_OPEN (3) orders + positions, one attempt per wall. Size: BJ_RISK_PCT (1%) of the book balance /
          (entry - (wall - 1 tick)), notional capped at BJ_MAX_NOTIONAL_X (3) x balance, rounded down to the step,
          skipped below the symbol's min notional. Cancelled when the wall is pulled or drops below the wall test,
          after BJ_ORDER_TIMEOUT_SEC (60 s), or when price moves more than BJ_AWAY_TICKS (10) ticks away.
FILL      queue ahead = size resting at our price when placed. Filled when an aggTrade prints through our price,
          or when aggressive trades AT our price (sells for a buy order) exceed that queue.
EXIT      every exit is a paper market order at the NEXT aggTrade price after the trigger:
          - before breakeven: wall level gone or below 50% (BJ_WALL_KEEP_PCT) of its size at the fill, or a trade
            through the wall price
          - at +0.15% (BJ_BE_PCT): stop to entry; then trail 0.15% (BJ_TRAIL_PCT) behind the best price since entry
          - time stop: breakeven not reached after BJ_TIME_STOP_SEC (300 s)
COSTS     entry maker 0.02% (BJ_MAKER_PCT) + 18% GST; exit taker 0.05% (BJ_TAKER_PCT) + 18% GST (BJ_GST_PCT).
KILL      BJ_PAUSE_HOURS (6) pause after a BJ_DAY_LOSS_PCT (3%) loss in one IST day, BJ_MAX_LOSS_STREAK (5) losses
          in a row, or a stale feed. Open positions are closed at the next trade; Telegram on pause and resume.
SESSION   every wall / order / trade is tagged "US" (Mon-Fri 09:30-16:00 New York, the bot's in_us_session) or
          "outside". Daily summary at 00:00 IST split by session.
"""
import copy
import csv
import importlib.util
import json
import logging
import os
import queue
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo

log = logging.getLogger("bot")
IST = ZoneInfo("Asia/Kolkata")
BOOK = "BOOK JUMP"
WS_DEPTH = "wss://fstream.binance.com/public/stream?streams="
WS_TRADE = "wss://fstream.binance.com/market/stream?streams="
SESS_KEYS = ("walls", "walls_done", "pulled", "touched_held", "orders", "fills", "trades", "wins")
WALL_COLS = ["time_ist", "session", "symbol", "side", "price", "size_usdt", "multiple", "lifetime_s", "outcome",
             "mid_10s", "mid_60s", "mid_300s", "touched", "broke_through", "bj_order"]


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
def load_cfg(env=None):
    e = os.environ if env is None else env

    def f(k, d):
        try:
            return float(e.get(k, d))
        except (TypeError, ValueError):
            return float(d)

    rule = str(e.get("BJ_WALL_RULE", "and")).strip().lower()
    return {
        "start_balance": f("BJ_START_BALANCE", 100),
        "max_symbols": int(f("BJ_MAX_SYMBOLS", 20)),
        "min_vol": f("BJ_MIN_VOL_USDT", 20_000_000),
        "tick_min_pct": f("BJ_TICK_MIN_PCT", 0.01),
        "tick_max_pct": f("BJ_TICK_MAX_PCT", 0.1),
        "universe_h": f("BJ_UNIVERSE_HOURS", 6),
        "wall_mult": f("BJ_WALL_MULT", 5),
        "wall_min_usdt": f("BJ_WALL_MIN_USDT", 25_000),
        "wall_rule": rule if rule in ("and", "or") else "and",
        "persist": f("BJ_PERSIST_SEC", 3),
        "max_open": int(f("BJ_MAX_OPEN", 3)),
        "risk_pct": f("BJ_RISK_PCT", 1),
        "max_notional_x": f("BJ_MAX_NOTIONAL_X", 3),
        "order_timeout": f("BJ_ORDER_TIMEOUT_SEC", 60),
        "away_pct": f("BJ_AWAY_PCT", 0.5),
        "away_ticks": f("BJ_AWAY_TICKS", 10),
        "wall_keep_pct": f("BJ_WALL_KEEP_PCT", 50),
        "be_pct": f("BJ_BE_PCT", 0.15),
        "be_ticks": f("BJ_BE_TICKS", 5),
        "trail_pct": f("BJ_TRAIL_PCT", 0.3),
        "trail_ticks": f("BJ_TRAIL_TICKS", 8),
        "time_stop": f("BJ_TIME_STOP_SEC", 300),
        "maker_pct": f("BJ_MAKER_PCT", 0.02),
        "taker_pct": f("BJ_TAKER_PCT", 0.05),
        "gst_pct": f("BJ_GST_PCT", 18),
        "day_loss_pct": f("BJ_DAY_LOSS_PCT", 3),
        "max_loss_streak": int(f("BJ_MAX_LOSS_STREAK", 5)),
        "pause_h": f("BJ_PAUSE_HOURS", 6),
        "stale_sec": f("BJ_STALE_SEC", 10),
        "stale_kill_sec": f("BJ_STALE_KILL_SEC", 60),
    }


def refuse_reason(bot_cfg):
    """'' when BOOK JUMP may run: paper mode on Binance only."""
    why = []
    if not bot_cfg.get("dry_run"):
        why.append("DRY_RUN is off (live trading)")
    if bot_cfg.get("exchange") != "binance":
        why.append(f"EXCHANGE={bot_cfg.get('exchange')} (needs binance)")
    return "; ".join(why)


# --------------------------------------------------------------------------
# Pure helpers (unit-tested)
# --------------------------------------------------------------------------
def is_wall(usdt, avg, c):
    big = avg > 0 and usdt >= c["wall_mult"] * avg
    enough = usdt >= c["wall_min_usdt"]
    return (big and enough) if c["wall_rule"] == "and" else (big or enough)


def top_avg_usdt(bids, asks):
    lv = bids[:5] + asks[:5]
    return sum(p * q for p, q in lv) / len(lv) if lv else 0.0


def find_walls(bids, asks, c):
    """Walls among the top 5 levels of each side: [(side, price, qty, usdt, multiple)]."""
    avg = top_avg_usdt(bids, asks)
    out = []
    for side, levels in (("bid", bids[:5]), ("ask", asks[:5])):
        for p, q in levels:
            if is_wall(p * q, avg, c):
                out.append((side, p, q, p * q, p * q / avg if avg else 0.0))
    return out


def level_qty(levels, price, tick):
    for p, q in levels:
        if abs(p - price) < tick / 2:
            return q
    return None


def out_of_view(side, levels, price, tick):
    """The level lies beyond the deepest visible level of the 20-level book (cannot be judged)."""
    if not levels:
        return True
    return price < levels[-1][0] - tick / 2 if side == "bid" else price > levels[-1][0] + tick / 2


def floor_step(x, step):
    if step <= 0:
        return x
    return int(x / step + 1e-9) * step


def size_order(balance, risk_pct, entry, stop, max_notional_x, step, min_notional):
    """Quantity risking risk_pct of the balance to the stop, notional capped; 0 if below min notional."""
    risk = abs(entry - stop)
    if entry <= 0 or risk <= 0 or balance <= 0:
        return 0.0
    qty = min(balance * risk_pct / 100.0 / risk, balance * max_notional_x / entry)
    qty = floor_step(qty, step)
    return qty if qty > 0 and qty * entry >= min_notional else 0.0


def fees_usd(entry, exit_px, qty, c):
    gst = 1 + c["gst_pct"] / 100.0
    return entry * qty * c["maker_pct"] / 100.0 * gst + exit_px * qty * c["taker_pct"] / 100.0 * gst


def parse_msg(raw):
    """Binance combined-stream message -> ("depth", sym, bids, asks, xt) | ("trade", sym, price, qty, buyer_maker, xt)
    | None. xt = Binance's own event time in ms (None if absent)."""
    m = json.loads(raw)
    d = m.get("data") if isinstance(m, dict) and "data" in m else m
    if not isinstance(d, dict):
        return None
    xt = int(d.get("T") or d.get("E") or 0) or None             # exchange time (ms); orders the two sockets
    if d.get("e") == "aggTrade":
        return ("trade", d["s"], float(d["p"]), float(d["q"]), bool(d["m"]), xt)
    if "b" in d and "a" in d and d.get("s"):
        return ("depth", d["s"], [(float(p), float(q)) for p, q in d["b"]],
                [(float(p), float(q)) for p, q in d["a"]], xt)
    return None


def ist_iso(ts):
    return datetime.fromtimestamp(ts, IST).isoformat(timespec="seconds")


def ist_day(ts):
    return datetime.fromtimestamp(ts, IST).strftime("%Y-%m-%d")


def new_state(start_balance):
    """The BOOK JUMP book, stored in state.json as books["BOOK JUMP"] next to the bot's own books.
    open_trade stays None (positions are under "positions") so the bot's per-book logic never touches it;
    "day" is keyed by IST date (BOOK JUMP's day ends at 00:00 IST) and split by session under "s"."""
    return {"strategy": BOOK, "start": start_balance, "balance": start_balance, "open_trade": None,
            "day": {}, "month": {}, "trades": 0, "wins": 0, "day_tz": "IST", "since": "",
            "positions": {}, "loss_streak": 0, "paused_until": 0, "summary_day": ""}


# --------------------------------------------------------------------------
# Engine: all trading logic, no network/threads; time is passed in (tests drive it with recorded streams)
# --------------------------------------------------------------------------
class Engine:
    def __init__(self, cfg, in_us_session, sink, state, usd_inr=85.0):
        self.c, self.in_us, self.sink, self.st, self.usd_inr = cfg, in_us_session, sink, state, usd_inr
        self.meta = {}              # sym -> {"tick", "step", "min_notional"}
        self.universe = set()
        self.book = {}              # sym -> {"ts", "bids", "asks"}
        self.walls = {}             # sym -> {(side, price_key): wall}
        self.wlog = []              # confirmed walls waiting for their +300 s row
        self.orders = {}            # sym -> resting paper order
        self.pos = state.setdefault("positions", {})
        self.feed_ok, self.down_since, self.stale_killed = True, 0.0, False
        self.bot_paused = lambda: False
        self.next_id = 1
        for p in self.pos.values():  # positions left from before a restart: close at the first trade
            p["exit_pending"] = p.get("exit_pending") or "RESTART"

    # ---- bookkeeping ----
    def session(self, ts):
        return "US" if self.in_us(ts) else "outside"

    def day(self, ts):
        return self.st["day"].setdefault(ist_day(ts), {"start_balance": round(self.st["balance"], 4), "pnl": 0.0,
                                                        "trades": 0, "wins": 0, "s": {}})

    def sess(self, ts, tag):
        s = self.day(ts)["s"].setdefault(tag, {k: 0 for k in SESS_KEYS})
        s.setdefault("pnl", 0.0)
        return s

    def paused(self, ts):
        return self.st.get("paused_until", 0) > ts

    def can_enter(self, ts):
        return self.feed_ok and not self.paused(ts) and not self.bot_paused()

    def mid(self, sym, ts, max_age=5.0):
        b = self.book.get(sym)
        if not b or ts - b["ts"] > max_age or not b["bids"] or not b["asks"]:
            return None
        return (b["bids"][0][0] + b["asks"][0][0]) / 2

    def set_universe(self, meta, syms):
        self.meta.update(meta)
        self.universe = set(syms)
        return self.subscriptions()

    def subscriptions(self):
        return self.universe | set(self.orders) | set(self.pos)

    # ---- market data ----
    def on_depth(self, sym, ts, bids, asks, xt=None):
        if not bids or not asks:
            return
        self.book[sym] = {"ts": ts, "bids": bids, "asks": asks, "xt": xt}
        m = self.meta.get(sym)
        if not m:
            return
        tick = m["tick"]
        avg = top_avg_usdt(bids, asks)
        walls = self.walls.setdefault(sym, {})
        for key, w in list(walls.items()):            # walls already tracked: still a wall?
            levels = bids if w["side"] == "bid" else asks
            q = level_qty(levels, w["price"], tick)
            w["top5"] = False
            if q is None:
                if out_of_view(w["side"], levels, w["price"], tick):
                    continue                          # price moved far away; can't judge, keep as is
                self.end_wall(sym, key, ts, "pulled")
            elif is_wall(w["price"] * q, avg, self.c):
                w.update(qty=q, usdt=w["price"] * q, mult=w["price"] * q / avg if avg else 0.0, last_ts=ts)
            else:                                     # lost more than half its size = pulled; else just under the bar
                shrunk = w["price"] * q < w["usdt"] * self.c["wall_keep_pct"] / 100.0
                self.end_wall(sym, key, ts, "pulled" if shrunk else "below_threshold")
        for side, p, q, usdt, mult in find_walls(bids, asks, self.c):
            key = (side, round(p / tick))
            w = walls.get(key)
            if w is None:
                w = walls[key] = {"sym": sym, "side": side, "price": p, "first_ts": ts, "confirm_ts": None,
                                  "qty": q, "usdt": usdt, "mult": mult, "last_ts": ts, "touched": False,
                                  "broke": False, "used": False, "bj_order": "", "ended_ts": None}
            w["top5"] = True
        for w in walls.values():
            if w["confirm_ts"] is None and ts - w["first_ts"] >= self.c["persist"]:
                w["confirm_ts"] = ts
                w["session"] = self.session(ts)
                w["c_usdt"], w["c_mult"] = w["usdt"], w["mult"]
                w["mids"] = {}
                self.wlog.append(w)
                self.sess(ts, w["session"])["walls"] += 1
        self.check_order_book(sym, ts)
        self.check_pos_book(sym, ts, xt)
        self.try_entry(sym, ts)

    def on_trade(self, sym, ts, px, qty, buyer_maker, xt=None):
        for w in self.walls.get(sym, {}).values():
            d = 1 if w["side"] == "bid" else -1
            tick = self.meta[sym]["tick"]
            if d * (px - w["price"]) <= tick / 2:
                w["touched"] = True
            if d * (px - w["price"]) < -tick / 2:
                w["broke"] = True
        p = self.pos.get(sym)
        if p and p.get("exit_pending"):
            self.exit(sym, ts, px)
            return
        o = self.orders.get(sym)
        if o and self.order_filled(o, px, qty, buyer_maker):
            self.fill(sym, ts, px, xt)
            return                                     # the trade that fills us never triggers an exit
        if p:
            self.pos_trade(p, ts, px)

    # ---- walls ----
    def end_wall(self, sym, key, ts, why):
        w = self.walls[sym].pop(key)
        w["ended_ts"] = ts
        w["outcome"] = "touched" if w["touched"] else "pulled"
        o = self.orders.get(sym)
        if o and o["wall_key"] == key:
            self.cancel(sym, ts, "wall_pulled" if why == "pulled" else "wall_below_threshold")

    def finish_walls(self, ts, force=False):
        keep = []
        for w in self.wlog:
            for h in (10, 60, 300):
                if h not in w["mids"] and ts >= w["confirm_ts"] + h:
                    mid = self.mid(w["sym"], ts)
                    w["mids"][h] = "" if mid is None else mid
            if not force and ts < w["confirm_ts"] + 300:
                keep.append(w)
                continue
            outcome = w["outcome"] if w["ended_ts"] is not None else "still there"
            life = (w["ended_ts"] if w["ended_ts"] is not None else ts) - w["first_ts"]
            s = self.sess(w["confirm_ts"], w["session"])
            s["walls_done"] += 1
            s["pulled"] += int(outcome == "pulled")
            s["touched_held"] += int(w["touched"] and not w["broke"])
            self.sink.wall_row([ist_iso(w["confirm_ts"]), w["session"], w["sym"], w["side"], w["price"],
                                round(w["c_usdt"], 2), round(w["c_mult"], 2), round(life, 1), outcome,
                                w["mids"].get(10, ""), w["mids"].get(60, ""), w["mids"].get(300, ""),
                                int(w["touched"]), int(w["broke"]), w["bj_order"]])
        self.wlog = keep

    # ---- orders ----
    def try_entry(self, sym, ts):
        if (not self.can_enter(ts) or sym not in self.universe or sym in self.orders or sym in self.pos
                or len(self.orders) + len(self.pos) >= self.c["max_open"]):
            return
        cands = [w for w in self.walls.get(sym, {}).values() if w["confirm_ts"] and w["top5"] and not w["used"]]
        if not cands:
            return
        w = max(cands, key=lambda x: x["usdt"])
        m, b = self.meta[sym], self.book[sym]
        tick = m["tick"]
        if w["side"] == "bid":
            side, price, stop = "long", w["price"] + tick, w["price"] - tick
            if price >= b["asks"][0][0] - tick / 2:
                return                                         # would not rest passively (post-only)
            queue = level_qty(b["bids"], price, tick) or 0.0
        else:
            side, price, stop = "short", w["price"] - tick, w["price"] + tick
            if price <= b["bids"][0][0] + tick / 2:
                return
            queue = level_qty(b["asks"], price, tick) or 0.0
        qty = size_order(self.st["balance"], self.c["risk_pct"], price, stop, self.c["max_notional_x"],
                         m["step"], m["min_notional"])
        w["used"] = True                                       # one attempt per wall (placed or too small)
        if qty <= 0:
            log.info("BOOK JUMP %s: size below min notional, wall skipped", sym)
            return
        w["bj_order"] = "order"
        tag = self.session(ts)
        self.orders[sym] = {"id": self.next_id, "sym": sym, "side": side, "price": price, "qty": qty, "stop": stop,
                            "wall_key": (w["side"], round(w["price"] / tick)), "wall_price": w["price"],
                            "wall_usdt": w["usdt"], "wall_mult": w["mult"], "placed_ts": ts, "queue": queue,
                            "at_px": 0.0, "session": tag, "wall": w}
        self.next_id += 1
        self.sess(ts, tag)["orders"] += 1
        self.sink.event("BJ_ORDER", symbol=sym, side=side, price=price, qty=qty, wall_price=w["price"],
                        wall_usdt=round(w["usdt"], 2), wall_multiple=round(w["mult"], 2), queue_ahead=queue,
                        session=tag)

    def cancel(self, sym, ts, reason):
        o = self.orders.pop(sym, None)
        if o:
            self.sink.event("BJ_CANCEL", symbol=sym, side=o["side"], price=o["price"], reason=reason,
                            age_s=round(ts - o["placed_ts"], 1), session=o["session"])

    def cancel_all(self, ts, reason):
        for sym in list(self.orders):
            self.cancel(sym, ts, reason)

    def check_order_book(self, sym, ts):
        o = self.orders.get(sym)
        if not o:
            return
        tick, b = self.meta[sym]["tick"], self.book[sym]
        away = (b["bids"][0][0] - o["price"]) if o["side"] == "long" else (o["price"] - b["asks"][0][0])
        limit = max(o["price"] * self.c["away_pct"] / 100.0, self.c["away_ticks"] * tick)
        if away > limit + tick / 2:
            self.cancel(sym, ts, "price_away")

    @staticmethod
    def order_filled(o, px, qty, buyer_maker):
        eps = abs(o["price"]) * 1e-9
        if o["side"] == "long":
            if px < o["price"] - eps:
                return True
            if abs(px - o["price"]) <= eps and buyer_maker:          # aggressive sell at our bid
                o["at_px"] += qty
        else:
            if px > o["price"] + eps:
                return True
            if abs(px - o["price"]) <= eps and not buyer_maker:      # aggressive buy at our ask
                o["at_px"] += qty
        return o["at_px"] > o["queue"]

    def fill(self, sym, ts, px=None, xt=None):
        o = self.orders.pop(sym)
        w = o["wall"]
        w["bj_order"] = "filled"
        tag = self.session(ts)
        p = {"sym": sym, "side": o["side"], "entry": o["price"], "qty": o["qty"], "stop0": o["stop"],
             "wall_price": o["wall_price"], "wall_ref_usdt": w["usdt"] if w["ended_ts"] is None else o["wall_usdt"],
             "fill_ts": ts, "best": o["price"], "be": False, "stop": o["stop"], "session": tag,
             "exit_pending": "", "tick": self.meta[sym]["tick"], "step": self.meta[sym]["step"],
             "wall_mult": o["wall_mult"], "fill_px": px, "fill_xt": xt, "trigger": {}}
        self.pos[sym] = p
        self.sess(ts, tag)["fills"] += 1
        self.sink.event("BJ_FILL", symbol=sym, side=p["side"], price=p["entry"], qty=p["qty"],
                        wall_price=p["wall_price"], wall_usdt=round(p["wall_ref_usdt"], 2), fill_trade_px=px,
                        wait_s=round(ts - o["placed_ts"], 1), session=tag)
        self.sink.tg(f"BJ FILL {sym} {p['side'].upper()} {p['qty']:g} @ {p['entry']:g} | wall {p['wall_price']:g} "
                     f"(${p['wall_ref_usdt'] / 1000:,.0f}k, {p['wall_mult']:.1f}x) | {tag}")
        self.sink.save()

    # ---- positions ----
    def check_pos_book(self, sym, ts, xt=None):
        """Before breakeven: exit if the wall level is missing (WALL_GONE) or below wall_keep_pct of its size at the
        fill (WALL_SHRANK). Only book snapshots taken after the fill count: depth and trades arrive on separate
        sockets, so a snapshot from before the fill can arrive after it (Binance event time decides)."""
        p = self.pos.get(sym)
        if not p or p["be"] or p["exit_pending"]:
            return
        if p.get("fill_xt") and xt is not None and xt <= p["fill_xt"]:
            return
        b = self.book[sym]
        levels = b["bids"] if p["side"] == "long" else b["asks"]
        wside = "bid" if p["side"] == "long" else "ask"
        q = level_qty(levels, p["wall_price"], p["tick"])
        why = ""
        if q is None:
            if not out_of_view(wside, levels, p["wall_price"], p["tick"]):
                why = "WALL_GONE"
        elif p["wall_price"] * q < p["wall_ref_usdt"] * self.c["wall_keep_pct"] / 100.0:
            why = "WALL_SHRANK"
        if why:
            p["exit_pending"] = why
            p["trigger"] = {"wall_level_usdt": round(p["wall_price"] * (q or 0), 2),
                            "best_level": levels[0][0], "book_ms_after_fill":
                            (xt - p["fill_xt"]) if xt and p.get("fill_xt") else None,
                            "s_after_fill": round(ts - p["fill_ts"], 3)}

    def pos_trade(self, p, ts, px):
        if p["exit_pending"]:
            return
        d = 1 if p["side"] == "long" else -1
        eps = p["tick"] / 2
        if not p["be"] and d * (px - p["wall_price"]) < -eps:     # strictly beyond the wall price
            p["exit_pending"] = "WALL_BROKEN"
            p["trigger"] = {"trade_px": px, "s_after_fill": round(ts - p["fill_ts"], 3)}
            return
        p["best"] = max(p["best"], px) if d > 0 else min(p["best"], px)
        if not p["be"] and d * (p["best"] - p["entry"]) >= self.be_dist(p) - 1e-12:
            p["be"], p["stop"] = True, p["entry"]
        if p["be"]:
            trail = p["best"] - d * self.trail_dist(p)
            p["stop"] = max(p["stop"], trail) if d > 0 else min(p["stop"], trail)
            if d * (px - p["stop"]) <= 0:
                p["exit_pending"] = "BE_STOP" if abs(p["stop"] - p["entry"]) < 1e-12 else "TRAIL_STOP"

    def be_dist(self, p):
        """Breakeven needs max(BJ_BE_PCT % of entry, BJ_BE_TICKS ticks) in profit."""
        return max(p["entry"] * self.c["be_pct"] / 100.0, self.c["be_ticks"] * p["tick"])

    def trail_dist(self, p):
        """The trail sits max(BJ_TRAIL_PCT % of the best price, BJ_TRAIL_TICKS ticks) behind the best price."""
        return max(p["best"] * self.c["trail_pct"] / 100.0, self.c["trail_ticks"] * p["tick"])

    def exit(self, sym, ts, px):
        p = self.pos.pop(sym)
        d = 1 if p["side"] == "long" else -1
        gross = d * (px - p["entry"]) * p["qty"]
        fees = fees_usd(p["entry"], px, p["qty"], self.c)
        net = gross - fees
        risk = abs(p["entry"] - p["stop0"])
        r_mult = d * (px - p["entry"]) / risk if risk else 0.0
        st = self.st
        st["balance"] = round(st["balance"] + net, 6)
        st["trades"] += 1
        st["wins"] += int(net > 0)
        st["loss_streak"] = st["loss_streak"] + 1 if net < 0 else 0
        dy = self.day(ts)
        dy["pnl"] += net
        dy["trades"] += 1
        dy["wins"] += int(net > 0)
        s = self.sess(p["fill_ts"], p["session"])
        s["trades"] += 1
        s["wins"] += int(net > 0)
        s["pnl"] += net
        reason = p["exit_pending"] or "EXIT"
        self.sink.event("BJ_EXIT", symbol=sym, side=p["side"], reason=reason, entry=p["entry"], exit_price=px,
                        qty=p["qty"], gross_pnl_usd=round(gross, 4), fees_usd=round(fees, 4),
                        net_pnl_usd=round(net, 4), r_multiple=round(r_mult, 2), breakeven_reached=p["be"],
                        held_s=round(ts - p["fill_ts"], 1), session=p["session"], balance=round(st["balance"], 4),
                        best_price=p["best"], stop=p["stop"], fill_trade_px=p.get("fill_px"),
                        trigger=p.get("trigger") or {},
                        settings={"be_pct": self.c["be_pct"], "be_ticks": self.c["be_ticks"],
                                  "trail_pct": self.c["trail_pct"], "trail_ticks": self.c["trail_ticks"],
                                  "be_dist": self.be_dist(p), "trail_dist": self.trail_dist(p),
                                  "wall_keep_pct": self.c["wall_keep_pct"]})
        n = int(round(p["qty"] / p["step"])) if p["step"] else p["qty"]
        self.sink.trade_row([ist_iso(p["fill_ts"]).replace("T", " "), ist_iso(ts).replace("T", " "), "PAPER", sym,
                             p["side"].upper(), n, p["entry"], p["stop0"], "trail", px, reason, round(gross, 4),
                             round(fees, 4), round(net, 4), round(net * self.usd_inr, 2), round(r_mult, 2), "",
                             False, "", BOOK, round(st["balance"], 4), p["session"]])
        self.sink.tg(f"BJ EXIT {sym} {p['side'].upper()} {reason} {p['entry']:g} -> {px:g} | net ${net:+.3f} "
                     f"(fees ${fees:.3f}) | bal ${st['balance']:.2f} | {int(ts - p['fill_ts'])}s | {p['session']}")
        self.sink.save()
        if self.c["day_loss_pct"] > 0 and dy["pnl"] <= -self.c["day_loss_pct"] / 100.0 * dy["start_balance"]:
            self.kill(ts, f"day loss ${dy['pnl']:.2f} >= {self.c['day_loss_pct']:g}% of the day's start")
        elif self.c["max_loss_streak"] > 0 and st["loss_streak"] >= self.c["max_loss_streak"]:
            self.kill(ts, f"{st['loss_streak']} losses in a row")

    # ---- kill switch / feed ----
    def kill(self, ts, reason):
        if self.paused(ts):
            return
        self.st["paused_until"] = ts + self.c["pause_h"] * 3600
        self.st["loss_streak"] = 0
        self.cancel_all(ts, "kill_switch")
        for p in self.pos.values():
            p["exit_pending"] = p["exit_pending"] or "KILL"
        self.sink.event("BJ_KILL", state="paused", reason=reason, until=ist_iso(self.st["paused_until"]))
        self.sink.tg(f"BOOK JUMP PAUSED for {self.c['pause_h']:g}h: {reason}. Resting orders cancelled"
                     + (", open positions closing at market" if self.pos else "") + f". Resumes {ist_iso(self.st['paused_until'])[11:16]} IST.")
        self.sink.save()

    def feed_down(self, ts, detail=""):
        if not self.feed_ok:
            return
        self.feed_ok, self.down_since, self.stale_killed = False, ts, False
        self.cancel_all(ts, "feed_stale")
        for sym, walls in self.walls.items():
            for w in walls.values():
                if w["confirm_ts"] is not None:
                    w["ended_ts"], w["outcome"] = ts, "feed lost"
        self.walls, self.book = {}, {}
        self.sink.event("BJ_FEED", state="stale", detail=detail)
        self.sink.tg(f"BOOK JUMP: no market data for {self.c['stale_sec']:g}s ({detail}). Resting orders cancelled; "
                     "standing down until the feed is back.")

    def feed_up(self, ts):
        if self.feed_ok:
            return
        self.feed_ok = True
        self.sink.event("BJ_FEED", state="ok", down_s=round(ts - self.down_since, 1))
        self.sink.tg(f"BOOK JUMP: market data back after {ts - self.down_since:.0f}s.")

    # ---- timer (called about twice a second) ----
    def on_timer(self, ts):
        for sym, o in list(self.orders.items()):
            if ts - o["placed_ts"] >= self.c["order_timeout"]:
                self.cancel(sym, ts, "timeout")
        if self.orders and not self.can_enter(ts):
            self.cancel_all(ts, "bot_paused" if self.bot_paused() else "standing_down")
        for p in self.pos.values():
            if not p["be"] and not p["exit_pending"] and ts - p["fill_ts"] >= self.c["time_stop"]:
                p["exit_pending"] = "TIME_STOP"
        if not self.feed_ok and not self.stale_killed and ts - self.down_since >= self.c["stale_kill_sec"]:
            self.stale_killed = True
            self.kill(ts, f"market data stale for over {self.c['stale_kill_sec']:g}s")
        pu = self.st.get("paused_until", 0)
        if pu and ts >= pu:
            self.st["paused_until"] = 0
            self.sink.event("BJ_KILL", state="resumed")
            self.sink.tg("BOOK JUMP resumed after the kill-switch pause.")
            self.sink.save()
        self.finish_walls(ts)
        today = ist_day(ts)
        if self.st.get("summary_day") != today:
            prev, self.st["summary_day"] = self.st.get("summary_day"), today
            if prev:
                self.sink.tg(self.day_summary(prev))
            self.sink.save()

    # ---- reporting ----
    def day_summary(self, day):
        d = self.st["day"].get(day, {})
        lines = [f"BOOK JUMP day {day} (IST) [PAPER]"]
        for tag in ("US", "outside"):
            s = (d.get("s") or {}).get(tag) or {}
            done = s.get("walls_done", 0)
            tr = s.get("trades", 0)
            lines.append(f"{tag}: walls {s.get('walls', 0)} | pulled {s.get('pulled', 0) / done * 100 if done else 0:.0f}% | "
                         f"touched+held {s.get('touched_held', 0) / done * 100 if done else 0:.0f}% | orders {s.get('orders', 0)} | "
                         f"fills {s.get('fills', 0)} | trades {tr} | win {s.get('wins', 0) / tr * 100 if tr else 0:.0f}% | "
                         f"P&L ${s.get('pnl', 0.0):+.2f}")
        lines.append(f"Day P&L ${d.get('pnl', 0.0):+.2f} | balance ${self.st['balance']:.2f} (start ${self.st['start']:.2f})")
        return "\n".join(lines)

    def status_text(self, ts):
        st = self.st
        d = st["day"].get(ist_day(ts), {})
        lines = [f"{BOOK} [PAPER]: ${st['balance']:.2f} (start ${st['start']:.2f}) | today (IST) {d.get('trades', 0)} "
                 f"trades, ${d.get('pnl', 0.0):+.2f} | feed {'ok' if self.feed_ok else 'STALE'} | "
                 f"universe {len(self.universe)} | walls now {sum(len(x) for x in self.walls.values())}"
                 + (f" | PAUSED until {ist_iso(st['paused_until'])[11:16]} IST" if self.paused(ts) else "")
                 + (" | bot paused: no new entries" if self.bot_paused() else "")]
        for p in self.pos.values():
            lines.append(f"  {p['side'].upper()} {p['sym']} {p['qty']:g} @ {p['entry']:g} wall {p['wall_price']:g} "
                         f"stop {'%g' % p['stop'] if p['be'] else 'wall'}{' (closing)' if p['exit_pending'] else ''}")
        for o in self.orders.values():
            lines.append(f"  resting {o['side']} {o['sym']} {o['qty']:g} @ {o['price']:g} (queue {o['queue']:g})")
        return "\n".join(lines)

    def report_line(self, day=None):
        st = self.st
        if day:
            d = st["day"].get(day, {})
            pnl, base, extra = d.get("pnl", 0.0), d.get("start_balance") or st["balance"], f"{d.get('trades', 0)} trades (IST day)"
        else:
            pnl, base, extra = st["balance"] - st["start"], st["start"], f"{st['trades']} trades, {st['wins']} wins"
        return (f"{BOOK} (own paper book, not in ALL): {extra} | P&L ${pnl:+.2f} "
                f"({pnl / base * 100 if base else 0:+.1f}%) | balance ${st['balance']:.2f}")


# --------------------------------------------------------------------------
# Runner: WebSocket + timer threads, files, universe
# --------------------------------------------------------------------------
class BookJump:
    def __init__(self, bot_cfg, tg, event_fn, in_us_session, rest, data_dir, bot_paused, book_get, book_publish):
        """book_get() -> the stored BOOK JUMP book from state.json (or None); book_publish(snapshot) puts a fresh
        copy into state.json's books (the bot writes it to disk on its next save, every few seconds)."""
        self.bc, self.c = bot_cfg, load_cfg()
        self.tg_client, self.event_fn, self.rest = tg, event_fn, rest
        self.book_get, self.book_publish = book_get, book_publish
        self.old_state_file = os.path.join(data_dir, "bookjump_state.json")   # pre-state.json versions
        self.walls_file = os.path.join(data_dir, "walls.csv")
        self.trades_file = os.path.join(data_dir, "trades.csv")
        self.lock = threading.RLock()
        self.tg_q = queue.Queue()
        self.st = self.load_state()
        self.eng = Engine(self.c, in_us_session, self, self.st, bot_cfg.get("usd_inr", 85.0))
        self.eng.bot_paused = bot_paused
        self.stop = threading.Event()
        self.subs = set()
        self.apps = {}
        self.last_msg = {"depth": 0.0, "trade": 0.0}
        self.got = {"depth": False, "trade": False}
        self.last_kick = {"depth": 0.0, "trade": 0.0}
        self.next_universe = 0.0
        self.last_save = 0.0
        self.last_err_log = 0.0

    # ---- sink used by the engine (always called with self.lock held) ----
    def event(self, typ, **kw):
        try:
            self.event_fn(typ, strategy=BOOK, **kw)
        except Exception:  # noqa: BLE001
            log.exception("BOOK JUMP event write failed")

    def tg(self, text):
        self.tg_q.put(text)                                   # sent by bj-telegram, never blocks the feed

    def tg_loop(self):
        while True:
            text = self.tg_q.get()
            try:
                self.tg_client.send(text)
            except Exception:  # noqa: BLE001
                log.exception("BOOK JUMP telegram failed")

    def wall_row(self, row):
        new = not os.path.exists(self.walls_file)
        with open(self.walls_file, "a", newline="") as fh:
            w = csv.writer(fh)
            if new:
                w.writerow(WALL_COLS)
            w.writerow(row)

    def trade_row(self, row):
        with open(self.trades_file, "a", newline="") as fh:
            csv.writer(fh).writerow(row)

    def save(self):
        """Publish a private copy: the main thread may be writing state.json at this moment, and replacing the
        value of an existing key (never adding one from this thread) is safe while it iterates."""
        self.book_publish(copy.deepcopy(self.st))
        self.last_save = time.time()

    def load_state(self):
        st = new_state(self.c["start_balance"])
        st["since"] = datetime.now(IST).isoformat(timespec="seconds")
        stored = self.book_get()
        if stored:
            st.update(copy.deepcopy(stored))
        else:                                                    # migrate from bookjump_state.json (first version)
            try:
                with open(self.old_state_file) as fh:
                    old = json.load(fh)
                old["day"] = old.pop("days", {})
                st.update(old)
                log.info("BOOK JUMP book migrated from bookjump_state.json into state.json")
            except (OSError, ValueError):
                pass
        st.update(strategy=BOOK, open_trade=None, day_tz="IST")
        cut = ist_day(time.time() - 90 * 86400)
        st["day"] = {k: v for k, v in st["day"].items() if k >= cut}
        return st

    def ensure_walls_file(self):
        """walls.csv exists (header only) from start-up, so the log sync picks it up before the first wall row."""
        if not os.path.exists(self.walls_file):
            with open(self.walls_file, "w", newline="") as fh:
                csv.writer(fh).writerow(WALL_COLS)

    def ensure_trades_header(self):
        """trades.csv gets a trailing 'session' column (POC rows leave it empty). Run before any thread starts."""
        head = ["open_time_ist", "close_time_ist", "mode", "symbol", "side", "contracts", "entry", "stop_loss",
                "take_profit", "exit_price", "exit_reason", "gross_pnl_usd", "est_fees_usd", "net_pnl_usd",
                "net_pnl_inr", "r_multiple", "rsi_4h", "exit_estimated", "ob_ratio", "strategy", "book_balance_usd",
                "session"]
        if not os.path.exists(self.trades_file):
            with open(self.trades_file, "w", newline="") as fh:
                csv.writer(fh).writerow(head)
            return
        with open(self.trades_file, newline="") as fh:                # keep \r\n as written by csv
            lines = fh.readlines()
        if not lines:
            return
        end = lines[0][len(lines[0].rstrip("\r\n")):] or "\n"         # keep the file's own line ending (csv: \r\n)
        cols = [c for c in lines[0].rstrip("\r\n").split(",") if c != "session"] + ["session"]
        fixed = ",".join(cols) + end
        if fixed != lines[0]:                                     # add it once / repair duplicates; rows untouched
            lines[0] = fixed
            tmp = self.trades_file + ".tmp"
            with open(tmp, "w", newline="") as fh:
                fh.writelines(lines)
            os.replace(tmp, self.trades_file)

    # ---- universe ----
    def pick_universe(self):
        prods = self.rest.products(None)
        vol = {t["symbol"]: t for t in self.rest.request("GET", "/fapi/v1/ticker/24hr")}
        meta, cands = {}, []
        for p in prods:
            sym, m = p["symbol"], self.rest.meta.get(p["id"], {})
            t = vol.get(sym)
            if p["state"] != "live" or not t or not m.get("tick"):
                continue
            meta[sym] = {"tick": m["tick"], "step": m["step"], "min_notional": m.get("min_notional", 5)}
            px, qv = float(t.get("lastPrice") or 0), float(t.get("quoteVolume") or 0)
            if px <= 0:
                continue
            tp = m["tick"] / px * 100
            if self.c["tick_min_pct"] <= tp <= self.c["tick_max_pct"] and qv > self.c["min_vol"]:
                cands.append((qv, sym))
        cands.sort(reverse=True)
        return meta, [s for _, s in cands[:self.c["max_symbols"]]]

    def refresh_universe(self):
        try:
            meta, syms = self.pick_universe()
        except Exception as ex:  # noqa: BLE001
            log.warning("BOOK JUMP universe refresh failed: %s", ex)
            self.next_universe = time.time() + 300
            return None
        with self.lock:
            old = set(self.eng.universe)
            subs = self.eng.set_universe(meta, syms)
            self.next_universe = time.time() + self.c["universe_h"] * 3600
            self.event("BJ_UNIVERSE", symbols=syms, added=sorted(set(syms) - old), dropped=sorted(old - set(syms)))
            if subs != self.subs:
                self.subs = subs
                now = time.time()
                self.last_msg = {"depth": now, "trade": now}          # grace period while reconnecting
                for app in list(self.apps.values()):
                    try:
                        app.close()
                    except Exception:  # noqa: BLE001
                        pass
        return syms

    # ---- lifecycle ----
    def prepare(self):
        """Called from the bot's start-up (main thread, before the threads exist). Returns the start-up line."""
        if importlib.util.find_spec("websocket") is None:
            raise RuntimeError("websocket-client is not installed (pip install -r requirements.txt)")
        self.ensure_trades_header()
        self.ensure_walls_file()
        syms = self.refresh_universe()
        with self.lock:
            self.save()
        c = self.c
        uni = f"{len(syms)} symbols" if syms is not None else "universe not loaded yet (retry in 5 min)"
        return (f"{BOOK} [PAPER]: ON | own book ${self.st['balance']:.2f} | {uni} (tick {c['tick_min_pct']:g}-"
                f"{c['tick_max_pct']:g}% of price, 24h vol > ${c['min_vol'] / 1e6:g}M) | wall >= {c['wall_mult']:g}x avg "
                f"{c['wall_rule'].upper()} >= ${c['wall_min_usdt']:,.0f}, {c['persist']:g}s | max {c['max_open']} open, "
                f"risk {c['risk_pct']:g}% | 24/7, tagged US/outside"
                + (f" | PAUSED until {ist_iso(self.st['paused_until'])[11:16]} IST" if self.eng.paused(time.time()) else ""))

    def start(self):
        for kind in ("depth", "trade"):
            threading.Thread(target=self.ws_loop, args=(kind,), name=f"bj-ws-{kind}", daemon=True).start()
        threading.Thread(target=self.timer_loop, name="bj-timer", daemon=True).start()
        threading.Thread(target=self.tg_loop, name="bj-telegram", daemon=True).start()
        log.info("BOOK JUMP threads started (%d symbols)", len(self.subs))

    def ws_loop(self, kind):
        import websocket
        backoff = 1.0
        while not self.stop.is_set():
            syms = sorted(self.subs)
            if not syms:
                self.stop.wait(2)
                continue
            suffix = "@depth20@100ms" if kind == "depth" else "@aggTrade"
            url = (WS_DEPTH if kind == "depth" else WS_TRADE) + "/".join(s.lower() + suffix for s in syms)
            self.got[kind] = False
            app = websocket.WebSocketApp(url, on_message=lambda _a, msg, k=kind: self.on_msg(k, msg),
                                         on_error=lambda _a, err, k=kind: log.warning("BOOK JUMP ws %s: %s", k, err))
            self.apps[kind] = app
            try:
                app.run_forever(ping_interval=60, ping_timeout=20)
            except Exception as ex:  # noqa: BLE001
                log.warning("BOOK JUMP ws %s crashed: %s", kind, ex)
            backoff = 1.0 if self.got[kind] else min(backoff * 2, 60.0)
            log.info("BOOK JUMP ws %s closed; reconnecting in %.0fs", kind, backoff)
            self.stop.wait(backoff)

    def on_msg(self, kind, raw):
        now = time.time()
        self.last_msg[kind] = now
        self.got[kind] = True
        try:
            msg = parse_msg(raw)
            if not msg:
                return
            with self.lock:
                if msg[0] == "depth":
                    self.eng.on_depth(msg[1], now, msg[2], msg[3], msg[4])
                else:
                    self.eng.on_trade(msg[1], now, msg[2], msg[3], msg[4], msg[5])
        except Exception:  # noqa: BLE001
            if now - self.last_err_log > 60:
                self.last_err_log = now
                log.exception("BOOK JUMP message handling failed")

    def timer_loop(self):
        while not self.stop.is_set():
            now = time.time()
            try:
                with self.lock:
                    if self.subs:
                        stale = [k for k, t in self.last_msg.items() if now - t > self.c["stale_sec"]]
                        if stale:
                            self.eng.feed_down(now, ", ".join(f"{k} stream" for k in stale))
                            for k in stale:                       # silent socket: force a reconnect
                                if now - self.last_kick[k] > 15 and k in self.apps:
                                    self.last_kick[k] = now
                                    try:
                                        self.apps[k].close()
                                    except Exception:  # noqa: BLE001
                                        pass
                        else:
                            self.eng.feed_up(now)
                    self.eng.on_timer(now)
                    if now - self.last_save > 60:
                        self.save()
                if now >= self.next_universe:
                    self.refresh_universe()
            except Exception:  # noqa: BLE001
                if now - self.last_err_log > 60:
                    self.last_err_log = now
                    log.exception("BOOK JUMP timer failed")
            self.stop.wait(0.5)

    # ---- called from the bot's main thread ----
    def status_text(self):
        with self.lock:
            return self.eng.status_text(time.time())

    def report_line(self, day=None):
        with self.lock:
            return self.eng.report_line(day)
