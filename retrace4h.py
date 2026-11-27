"""
4H RETRACE LONG / 4H RETRACE SHORT - two $100 books on Binance USDT-M perpetuals (28 Nov 2026). PAPER ONLY (refuses
live, like rsi1h.py; the exchange-side STOP_MARKET for live is not wired yet). Started by bot.py when RETRACE_ENABLED=1
(default) in paper mode on EXCHANGE=binance.

UNIVERSE  the 1H RSI books' universe (TRADING USDT perps, 24h quote volume >= $500k, refreshed hourly).
SETUP     each coin's own closed 4h candles (Binance 4h, UTC-aligned; checked RETRACE_CHECK_DELAY_SEC, 8 s, after
          each 4h close, in background threads): |body| > RETRACE_BIG_X (2) x the average |body| of the previous 24 4h
          candles, and |body| >= RETRACE_BODY_SHARE (80%) of the candle's high-low range. Green -> 4H RETRACE LONG,
          red -> 4H RETRACE SHORT. mid = (high + low) / 2, wicks included. Events 4H_SETUP and 4H_ARMED.
ENTRY     within RETRACE_WINDOW_H (24 h) after that candle closes: when the last trade price comes back to the mid
          (long: at/below it; short: at/above it; watched every RETRACE_POLL_SEC, 3 s, while something is armed and
          that side's book has room), capped IOC limit entry through the bot's executor: the size is limited by the
          order book (largest size whose average fill stays within MAX_IMPACT_PCT, 0.05%, of the best price), the IOC
          children never go beyond RETRACE_ENTRY_CAP_PCT (0.1%) past the mid. One entry per setup candle; a setup
          that could not be entered (no fill within the cap, book full, too small, ...) is retried at most every 60 s
          until it expires (4H_CANCEL "expired"). After a restart, a setup whose mid already traded while it was not
          watched is cancelled (no late entry).
STOP      RETRACE_STOP_PCT (1.5%) from the entry, fixed (= the shared HARD_STOP_PCT, 1.5%). 1R = 1.5% of the entry.
          Breakeven at RETRACE_BE_R (2R); from RETRACE_TRAIL_START_R (3R) the stop trails at (floor(best R) - 1) x R.
          No target. Exits at market (paper: checked on 1-minute candles, like the 1H RSI books).
SIZE      as the other books: free margin x 20 isolated, the price-impact limit, liquidation beyond the stop, up to 3
          trades per book, extra trades only for the leftover of an impact-capped entry, margin >= 25% of the book.
"""
import csv
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from zoneinfo import ZoneInfo

import rsi1h as R

log = logging.getLogger("bot")
IST = ZoneInfo("Asia/Kolkata")
LONG_BOOK, SHORT_BOOK = "4H RETRACE LONG", "4H RETRACE SHORT"
BOOKS = (LONG_BOOK, SHORT_BOOK)
P4H, P15M = 14400, 900
N_AVG = 24
N_CANDLES = 40                      # 24 for the average + up to 6 setup candles still inside the 24 h window (+ spare)


def book_of(side):
    return LONG_BOOK if side == "long" else SHORT_BOOK


def load_cfg(env=None):
    e = os.environ if env is None else env
    c = R.load_cfg(env)

    def f(k, d):
        try:
            return float(e.get(k, d))
        except (TypeError, ValueError):
            return float(d)
    c.update(enabled=str(e.get("RETRACE_ENABLED", "1")).strip().lower() in ("1", "true", "yes", "on"),
             start_balance=f("RETRACE_START_BALANCE", 100), big_x=f("RETRACE_BIG_X", 2),
             body_share=f("RETRACE_BODY_SHARE", 0.8), window_h=f("RETRACE_WINDOW_H", 24),
             stop_pct=f("RETRACE_STOP_PCT", 0.015), entry_cap_pct=f("RETRACE_ENTRY_CAP_PCT", 0.001),
             be_r=f("RETRACE_BE_R", 2), trail_start_r=f("RETRACE_TRAIL_START_R", 3),
             check_delay=f("RETRACE_CHECK_DELAY_SEC", 8), poll_sec=f("RETRACE_POLL_SEC", 3),
             fetch_workers=int(f("RETRACE_FETCH_WORKERS", 4)), retry_sec=f("RETRACE_RETRY_SEC", 60))
    return c


# ---------------- pure rules (unit-tested, also used by the dry run) ----------------
def setup_at(c4, i, big_x=2.0, body_share=0.8, n_avg=N_AVG):
    """The setup of closed 4h candle c4[i], or None. Needs n_avg candles before it."""
    if i < n_avg:
        return None
    c = c4[i]
    body = c["close"] - c["open"]
    rng = c["high"] - c["low"]
    if rng <= 0 or body == 0:
        return None
    avg = sum(abs(x["close"] - x["open"]) for x in c4[i - n_avg:i]) / n_avg
    share = abs(body) / rng
    if avg <= 0 or abs(body) <= big_x * avg or share < body_share:
        return None
    return {"side": "long" if body > 0 else "short", "candle": c["time"], "open": c["open"], "high": c["high"],
            "low": c["low"], "close": c["close"], "body": abs(body), "avg_body": avg, "body_x": abs(body) / avg,
            "body_share": share, "mid": (c["high"] + c["low"]) / 2}


def touched(side, mid, low, high):
    """Did a price range [low, high] reach the mid (long: at/below it, short: at/above it)?"""
    return low <= mid if side == "long" else high >= mid


def ist(ts):
    return datetime.fromtimestamp(ts, IST).strftime("%Y-%m-%d %H:%M")


def hm(ts):
    return datetime.fromtimestamp(ts, IST).strftime("%d %b %H:%M")


class Retrace4h:
    def __init__(self, bot, cfg=None):
        self.b, self.c = bot, cfg or load_cfg()
        self.st = bot.state.setdefault("retrace4h", {})
        for k, v in (("armed", {}), ("last_bar", 0), ("used", {})):
            self.st.setdefault(k, v)
        self.own_prods, self.own_prods_ts = {}, 0.0
        self.job = None
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="retrace4h")
        self.last_poll = 0.0
        self.ready = False              # no price watch until the first candle check after a start has run: armed
                                        # setups from state.json may have been traded / expired while the bot was off
        self.lock = threading.Lock()

    # ---- books ----
    def ensure_books(self):
        books, made = self.b.state["books"], []
        for name in BOOKS:
            if name not in books:
                bk = self.b.new_book(round(self.c["start_balance"], 4))
                bk["since"] = datetime.now(IST).isoformat(timespec="seconds")
                books[name] = bk
                made.append(name)
        if made:
            self.b.save_state()
        return made

    def startup_line(self):
        c = self.c
        return (f"\n4H RETRACE LONG / 4H RETRACE SHORT [PAPER]: ON | ${self.b.book(LONG_BOOK)['balance']:.2f} / "
                f"${self.b.book(SHORT_BOOK)['balance']:.2f} | 4h body > {c['big_x']:g}x the 24-candle average and >= "
                f"{c['body_share'] * 100:g}% of the range; entry back at its 50% within {c['window_h']:g}h (cap "
                f"{c['entry_cap_pct'] * 100:g}% past the mid) | stop {c['stop_pct'] * 100:g}%, 1R = {c['stop_pct'] * 100:g}%"
                f", BE {c['be_r']:g}R, trail from {c['trail_start_r']:g}R, no target | size as the 1H RSI books")

    def prods(self):
        r1 = getattr(self.b, "rsi1h", None)
        if r1 is not None and r1.prods:
            return r1.prods
        if not self.own_prods or time.time() - self.own_prods_ts > 3600:
            try:
                self.own_prods = R.Rsi1h.universe(self)
                self.own_prods_ts = time.time()
            except Exception:  # noqa: BLE001
                log.exception("4H RETRACE universe load failed")
        return self.own_prods

    def has_room(self, side):
        book = book_of(side)
        return (len(self.b.book_trades(book)) < self.c["max_open"]
                and R.avail_margin(self.b, book, self.c["min_trade_margin"])[1] is None)

    # ---- schedule ----
    def tick(self, now):
        bar = int(now) // P4H * P4H
        if bar != self.st["last_bar"] and now >= bar + self.c["check_delay"] and self.job is None:
            prods = self.prods()
            if prods:
                self.job = (bar, self.pool.submit(self.fetch_all, sorted(prods), bar))
        if self.job is not None and self.job[1].done():
            bar_j, fut = self.job
            self.job = None
            try:
                self.on_close(now, bar_j, fut.result())
            except Exception:  # noqa: BLE001
                log.exception("4H RETRACE scan failed")
        self.expire(now)
        if (self.ready and self.st["armed"] and now - self.last_poll >= self.c["poll_sec"]
                and not self.b.state.get("paused")
                and any(self.has_room(sd) for sd in {a["side"] for a in self.st["armed"].values()})):
            self.last_poll = now
            try:
                self.poll(now)
            except Exception:  # noqa: BLE001
                log.exception("4H RETRACE price poll failed")

    def fetch_all(self, syms, bar):
        """Worker thread: per symbol, the setups among the closed 4h candles whose 24 h window is still open, each
        with whether its mid already traded since (later closed candles, and the forming one for older setups)."""
        c = self.c
        win = c["window_h"] * 3600

        def one(sym):
            for _ in range(2):
                try:
                    cand = self.b.data.candles(sym, "4h", N_CANDLES, bar + 1)
                except Exception as ex:  # noqa: BLE001
                    err = str(ex)[:80]
                    time.sleep(1)
                    continue
                closed = [x for x in cand if x["time"] + P4H <= bar]
                forming = [x for x in cand if x["time"] == bar]
                if not closed or closed[-1]["time"] != bar - P4H:
                    err = "last 4h candle not in yet"
                    time.sleep(2)
                    continue
                out = []
                for i in range(max(N_AVG, len(closed) - 7), len(closed)):
                    s = setup_at(closed, i, c["big_x"], c["body_share"])
                    if not s or s["candle"] + P4H + win <= bar:
                        continue
                    later = closed[i + 1:] + (forming if i < len(closed) - 1 else [])
                    hit = next((x for x in later if touched(s["side"], s["mid"], x["low"], x["high"])), None)
                    s["new"] = i == len(closed) - 1
                    s["touched_at"] = hit["time"] if hit else None
                    out.append(s)
                return sym, out, None
            return sym, None, err

        res, errors = {}, 0
        with ThreadPoolExecutor(max_workers=max(1, c["fetch_workers"])) as ex:
            for sym, out, err in ex.map(one, syms):
                if out is None:
                    errors += 1
                elif out:
                    res[sym] = out
        return {"res": res, "errors": errors, "n": len(syms)}

    def on_close(self, now, bar, job):
        armed, used = self.st["armed"], self.st["used"]
        n_setup = n_armed = n_cancel = 0
        win = self.c["window_h"] * 3600
        for sym, sets in job["res"].items():
            for s in sets:
                key = f"{sym}|{s['side']}|{s['candle']}"
                if s["new"]:
                    n_setup += 1
                    self.b.event("4H_SETUP", book=book_of(s["side"]), side=s["side"], symbol=sym,
                                 candle=ist(s["candle"]), open=s["open"], high=s["high"], low=s["low"],
                                 close=s["close"], body_share=round(s["body_share"], 3),
                                 avg_body=round(s["avg_body"], 10), body_x=round(s["body_x"], 2), mid=s["mid"])
                if key in armed or key in used:
                    continue
                if s["touched_at"] is not None:
                    used[key] = s["candle"]
                    self.b.event("4H_CANCEL", book=book_of(s["side"]), side=s["side"], symbol=sym,
                                 candle=ist(s["candle"]), mid=s["mid"],
                                 reason=f"mid traded at {ist(s['touched_at'])} before it was watched: no late entry")
                    n_cancel += 1
                    continue
                a = {"symbol": sym, "side": s["side"], "candle": s["candle"], "mid": s["mid"], "open": s["open"],
                     "high": s["high"], "low": s["low"], "close": s["close"], "body_x": round(s["body_x"], 2),
                     "body_share": round(s["body_share"], 3), "expires": s["candle"] + P4H + int(win),
                     "vol24h": (self.prods().get(sym) or {}).get("vol24h"), "next_try": 0}
                armed[key] = a
                n_armed += 1
                self.b.event("4H_ARMED", book=book_of(s["side"]), side=s["side"], symbol=sym, candle=ist(s["candle"]),
                             mid=s["mid"], expires=ist(a["expires"]))
        cut = now - 3 * 86400
        self.st["used"] = {k: v for k, v in used.items() if v >= cut}
        self.st["last_bar"] = bar
        self.ready = True
        self.b.save_state()
        log.info("4H RETRACE %s close: %d symbols (%d errors) | setups %d, armed %d new / %d now, cancelled %d",
                 hm(bar - P4H), job["n"], job["errors"], n_setup, n_armed, len(armed), n_cancel)

    def cancel(self, key, why):
        a = self.st["armed"].pop(key, None)
        if a:
            self.st["used"][key] = a["candle"]
            self.b.event("4H_CANCEL", book=book_of(a["side"]), side=a["side"], symbol=a["symbol"],
                         candle=ist(a["candle"]), mid=a["mid"], reason=why)

    def expire(self, now):
        gone = [k for k, a in self.st["armed"].items() if now >= a["expires"]]
        for k in gone:
            self.cancel(k, f"expired: {self.c['window_h']:g}h after the setup candle closed without an entry")
        if gone:
            self.b.save_state()

    # ---- live price watch ----
    def poll(self, now):
        px = self.b.all_prices()
        hits = []
        for key, a in self.st["armed"].items():
            p = px.get(a["symbol"]) or 0
            if p > 0 and now >= a.get("next_try", 0) and ((p <= a["mid"]) if a["side"] == "long" else (p >= a["mid"])):
                hits.append((key, a, p))
        if not hits:
            return
        hits.sort(key=lambda h: -(h[1].get("vol24h") or 0))          # highest 24h volume first
        for rank, (key, a, p) in enumerate(hits, 1):
            if not self.has_room(a["side"]):
                continue
            sig = {"side": a["side"], "symbol": a["symbol"], "setup": a, "ltp": p, "cand": {"ltp": p}}
            try:
                self.enter(sig, now)
            except Exception:  # noqa: BLE001
                log.exception("4H RETRACE entry failed")
                sig["skip_reason"] = "error"
            if sig.get("taken"):
                self.st["armed"].pop(key, None)
                self.st["used"][key] = a["candle"]                       # one entry per setup candle
            else:
                a["next_try"] = now + self.c["retry_sec"]
            self.candidate(sig, rank, now)
        self.b.save_state()

    def skip(self, sig, reason, short=None):
        sig["skip_reason"] = short or reason
        self.b.event("4H_SKIP", book=book_of(sig["side"]), side=sig["side"], symbol=sig["symbol"], reason=reason,
                     mid=sig["setup"]["mid"])

    def enter(self, sig, now):
        c, b = self.c, self.b
        side, sym, a = sig["side"], sig["symbol"], sig["setup"]
        BOOK = book_of(side)
        p = self.prods().get(sym)
        cand = sig["cand"]
        if len(b.book_trades(BOOK)) >= c["max_open"]:
            return self.skip(sig, f"{BOOK} already has {len(b.book_trades(BOOK))} trades open", "max open trades")
        avail, why = R.avail_margin(b, BOOK, c["min_trade_margin"])
        if why:
            return self.skip(sig, why[0], why[1])
        if p is None:
            return self.skip(sig, "not in the universe any more", "not in universe")
        held = [n for n, t in b.open_trades() if t["product_id"] == p["id"]]
        if held:
            return self.skip(sig, f"this coin is already held by {held[0]}",
                             "already held" if held[0] == BOOK else "held by another book")
        try:
            ob = b.data.orderbook(sym, c["book_depth"])
        except Exception as ex:  # noqa: BLE001
            return self.skip(sig, f"order book lookup failed: {str(ex)[:120]}", "no order book")
        lv = ob[1] if side == "long" else ob[0]
        if not lv:
            return self.skip(sig, "empty order book", "no order book")
        tick, lev = p["tick"], c["leverage"]
        cap = R.round_tick(a["mid"] * (1 + c["entry_cap_pct"]) if side == "long" else a["mid"] * (1 - c["entry_cap_pct"]),
                           tick, "down" if side == "long" else "up")
        entry = lv[0][0]
        cand.update(level=a["mid"], entry_cap=cap)
        if (side == "long" and entry > cap) or (side == "short" and entry < cap):
            return self.skip(sig, f"best {'ask' if side == 'long' else 'bid'} {entry:g} is beyond the cap {cap:g} "
                                  f"({c['entry_cap_pct'] * 100:g}% past the mid {a['mid']:g})", "beyond cap")
        stop = R.hard_stop_for(side, entry, tick, c["stop_pct"])
        liq = R.liq_price(side, entry, lev, c["mmr"])
        liq_pct = abs(liq - entry) / entry
        cand.update(liq_price=R.round_tick(liq, tick, "nearest"), stop_used=stop, hard_stop=stop,
                    stop_pct=abs(entry - stop) / entry * 100)
        if liq_pct <= c["stop_pct"] + b.cfg.get("entry_slip_cap_pct", 0.001):
            return self.skip(sig, f"liquidation {liq:g} ({liq_pct * 100:.2f}%) is not beyond the stop {stop:g}",
                             "liquidation before stop")
        equity = b.book(BOOK)["balance"]
        free = avail
        impact = R.impact_limit(lv, p["cv"], c["max_impact"])           # order book: average fill within 0.05%
        n, info = R.size_for(free, lev, entry, p["cv"], p["min_notional"], impact)
        cand.update(free_margin=round(free, 4), target_qty=info.get("target_qty"), impact_qty=info.get("impact_qty"))
        if n < 1:
            return self.skip(sig, f"size: {info.get('skip')}", info.get("short", "size (min notional)"))
        need = c["min_trade_margin"] * equity
        if info["margin"] < need:
            return self.skip(sig, f"too small: margin ${info['margin']:.2f} ({info['binding']} binds) < "
                                  f"{c['min_trade_margin'] * 100:g}% of the book's balance ${equity:.2f} (${need:.2f})",
                             "too small")
        cand["size"] = n * p["cv"]
        tx = {"side": side, "symbol": sym, "product_id": p["id"], "contracts": n, "cv": p["cv"], "tick": tick}
        why = b.capped_entry(tx, p["min_notional"], ob, impact_qty=impact,
                             impact_capped=info["binding"] == "price impact", sweep=R.walk_price(lv, n * p["cv"]),
                             cap=cap)
        cand["exec"] = tx.get("exec")
        if why:
            return self.skip(sig, f"{why} (capped IOC entry, cap {cap:g})", why)
        n, entry = tx["contracts"], tx["entry"]
        qty = n * p["cv"]
        cand["size"] = qty
        stop = R.hard_stop_for(side, entry, tick, c["stop_pct"])
        r = entry * c["stop_pct"]
        liq = R.round_tick(R.liq_price(side, entry, lev, c["mmr"]), tick, "nearest")
        loss_usd, loss_pct = R.loss_if_stopped(side, qty, entry, stop, b.cfg["fee_pct_per_side"], equity)
        cand.update(stop_used=stop, stop_pct=abs(entry - stop) / entry * 100, hard_stop=stop, liq_price=liq,
                    loss_pct_if_stopped=round(loss_pct, 2), r_pct=round(c["stop_pct"] * 100, 3))
        info.update(notional=qty * entry, risk_usd=qty * abs(entry - stop), margin=qty * entry / lev)
        x = tx["exec"]
        detail = (f"{'green' if side == 'long' else 'red'} 4h candle {hm(a['candle'])} IST (O {a['open']:g} H "
                  f"{a['high']:g} L {a['low']:g} C {a['close']:g}): body {a['body_x']:.2f}x the 24-candle average, "
                  f"{a['body_share'] * 100:.0f}% of its range | entry back at its 50% {a['mid']:g} (cap {cap:g}, "
                  f"window until {hm(a['expires'])} IST) | stop {stop:g} ({c['stop_pct'] * 100:g}%), 1R "
                  f"{c['stop_pct'] * 100:g}%, BE {c['be_r']:g}R, trail from {c['trail_start_r']:g}R, no target"
                  f" | fill {qty:g}/{x['qty_intended']:g} @ {entry:g} (ref {x['entry_ref']:g}, cap {x['entry_cap']:g}, "
                  f"{x['entry_children']} IOC, slippage {x['entry_slip_pct']:+.3f}% = ${x['entry_slip_usd']:.4f}"
                  + (", price-impact limit" if x["liq_capped"] == "yes" else "") + ")"
                  f" | size: target {info['target_qty']:g} (free ${free:.2f} x {lev:g}), impact limit "
                  f"{info['impact_qty']:g} | liq {liq:g}, loss if stopped ${loss_usd:.2f} = {loss_pct:.1f}% of the book")
        t = {"mode": b.cfg["mode"], "book": BOOK, "side": side, "symbol": sym, "product_id": p["id"],
             "contracts": n, "cv": p["cv"], "tick": tick, "entry": entry, "stop": stop, "tp": None,
             "signal_close": sig["ltp"], "rsi": None, "strategy": "retrace4h",
             "extra": {"detail": detail, "mid": a["mid"], "candle": a["candle"], "cap": cap, "r": r,
                       "last_price": sig["ltp"], "target_qty": info["target_qty"], "impact_qty": info["impact_qty"],
                       "size_binding": info["binding"], "free_margin": round(free, 4)},
             "ob": None, "setup_time": a["candle"], "opened_ts": int(now), "last_checked": int(now),
             "equity": equity, "protected": None, "stop_source": "candle", "exec": tx["exec"],
             "leverage": lev, "margin": info["margin"], "hard_stop": stop, "liq_price": liq,
             "loss_if_stopped_usd": round(loss_usd, 4), "loss_if_stopped_pct": round(loss_pct, 2),
             "trail": True, "trail_r": 0, "stop_set": True, "touch_close": 0, "tc_low": entry, "tc_high": entry,
             "be_r": c["be_r"], "trail_start_r": c["trail_start_r"]}
        sig["taken"] = True
        R.record_leftover(b.book(BOOK), x["liq_capped"], free, info["margin"], sym, now)
        b.finish_entry(t, info, now)
        t["risk0"] = r                     # 1R = 1.5% of the entry (the stop rounds to the tick)
        b.save_state()

    def candidate(self, sig, rank, now):
        try:
            a, c = sig["setup"], sig.get("cand") or {}
            row = {"time": datetime.fromtimestamp(now, IST).strftime("%Y-%m-%d %H:%M:%S"), "bot": "binance-bot",
                   "book": book_of(sig["side"]), "symbol": sig["symbol"], "side": sig["side"].upper(),
                   "check": "price back at the 4h mid", "taken": "yes" if sig.get("taken") else "no",
                   "reason": "" if sig.get("taken") else sig.get("skip_reason", ""), "rank": rank,
                   "ltp": c.get("ltp"), "level": a["mid"], "range_low": a["low"], "range_high": a["high"],
                   "candle_open": a["open"], "candle_colour": "green" if a["side"] == "long" else "red",
                   "distance_pct": round((c["ltp"] / a["mid"] - 1) * 100, 4) if c.get("ltp") else None,
                   "ref_time": ist(a["candle"]), "stop_used": c.get("stop_used"),
                   "stop_pct": round(c["stop_pct"], 3) if c.get("stop_pct") is not None else None,
                   "size": c.get("size"), "vol24h_usdt": round(a["vol24h"]) if a.get("vol24h") else None,
                   "rank_by": "volume", "r_pct": c.get("r_pct")}
            x = c.get("exec") or {}
            row.update({k: x.get(k) for k in ("depth5_qty", "liq_cap_qty", "liq_capped", "entry_ref", "entry_cap",
                                              "entry_avg", "entry_slip_pct", "entry_children", "qty_intended",
                                              "qty_filled")})
            if not x.get("entry_cap") and c.get("entry_cap"):
                row["entry_cap"] = c["entry_cap"]
            row["entry_slip_money"] = x.get("entry_slip_usd")
            row.update({k: c.get(k) for k in ("free_margin", "target_qty", "impact_qty", "hard_stop", "liq_price",
                                              "loss_pct_if_stopped")})
            path = os.path.join(self.b.cfg["data_dir"], "candidates.csv")
            new = not os.path.exists(path)
            with open(path, "a", newline="") as fh:
                w = csv.DictWriter(fh, fieldnames=R.CAND_COLS, extrasaction="ignore")
                if new:
                    w.writeheader()
                w.writerow({k: ("" if v is None else v) for k, v in row.items()})
        except Exception as ex:  # noqa: BLE001
            log.warning("candidates.csv (4H RETRACE): %s", ex)

    def status_lines(self):
        armed = sorted(self.st["armed"].values(), key=lambda a: -(a.get("vol24h") or 0))
        n = {sd: sum(1 for a in armed if a["side"] == sd) for sd in ("long", "short")}
        return ([f"4H RETRACE LONG / SHORT: armed {n['long']} / {n['short']}"
                 + (f" | last 4h check {hm(self.st['last_bar'] - P4H)}" if self.st["last_bar"] else "")]
                + [f"  {a['side'].upper()} {a['symbol']} mid {a['mid']:g} (4h {hm(a['candle'])}), until "
                   f"{hm(a['expires'])}" for a in armed[:6]])
