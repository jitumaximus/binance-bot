"""
EMA20 PULLBACK - one $100 book (both sides) on Binance USDT-M perpetuals. PAPER ONLY (refuses live, like rsi1h.py).
=====================================================================================================================
Started by bot.py when EMA20_ENABLED=1 (default) in paper mode on EXCHANGE=binance. Setup rules: ema20_rules.py.

UNIVERSE  the 1H RSI books' universe (TRADING USDT perps with 24h quote volume >= $500k, refreshed hourly).
CANDLES   after each 5m close (+EMA20_CHECK_DELAY_SEC, 8 s): 5m klines (99, request weight 1) for the whole universe,
          fetched in background threads (4 at a time) so the main loop never waits; the symbols of open EMA20 trades
          first (their exit check runs at once), then armed setups, then the rest. Each symbol's candles are reduced
          to the setup state at the last closed candle in the worker; nothing big is kept.
SETUP     alert -> 2 pullback candles -> armed with a trigger (EMA20_ALERT, EMA20_ARMED, EMA20_CANCEL events).
          Sides: EMA20_SIDES = both | long | short. Armed setups are watched on the live price: one call for all
          symbols (/fapi/v1/ticker/price, weight 2) every EMA20_POLL_SEC (3 s), only while something is armed and
          the book has room for a trade. EMA20_ALERT is one event per 5m close listing that candle's alerts (the
          dry run on 27 Nov 2026 found ~22k alerts / ~6k armed setups a day across ~500 coins).
ENTRY     price beyond the trigger -> capped IOC limit entry through the bot's executor, exactly as the 1H RSI books:
          20x isolated, size = min(free margin x 20, the 0.05% price-impact limit), max MAX_OPEN_TRADES (3) per book,
          extra trades only for the leftover of an impact-capped entry, margin >= MIN_TRADE_MARGIN_PCT (25%) of the
          book, liquidation beyond the stop, a coin held by one book at most. Several breaches at one check: highest
          24h volume first. A setup is used once (a breach that is skipped is not retried). A breach seen only on a
          closed candle (between two price checks, or while the bot was down) cancels the setup: no late entry.
STOP      (28 Nov 2026) the pullback level: the red pullback candle's low (the lower low if both are red); short: the
          green pullback candle's high (the higher high). It works on 5m CLOSES: a 5m candle closing beyond it exits
          at market right after that close (PB_CLOSE); wicks through it don't count. The 3% hard stop always applies
          on the price (the trade's price stop until breakeven).
R         1R = entry - the closer of the pullback level and the 3% hard stop, at least EMA20_MIN_R_PCT (0.2%) of the
          entry. R sets breakeven and the trail.
EXITS     (market) whichever comes first:
          - a 5m candle closes beyond the pullback level (PB_CLOSE), or back through the 20 EMA (EMA_EXIT; below for
            a long, above for a short): exit right after that close, the entry candle included;
          - breakeven at 1R, then from 3R the stop trails at (floor(best R) - 1) x R (checked on 1m candles, like the
            1H RSI books);
          - the 3% hard stop (HARD_STOP_PCT), always: it is the initial stop.
"""
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from zoneinfo import ZoneInfo

import ema20_rules as E
import rsi1h as R

log = logging.getLogger("bot")
IST = ZoneInfo("Asia/Kolkata")
BOOK = "EMA20 PULLBACK"
BOOKS = (BOOK,)
P5M = 300
N_CANDLES = 98                      # +1 in candles() = 99: weight 1 per symbol


def load_cfg(env=None):
    e = os.environ if env is None else env
    c = R.load_cfg(env)             # shared: leverage, impact limit, max open, min margin, hard stop, mmr, min_vol, ...

    def f(k, d):
        try:
            return float(e.get(k, d))
        except (TypeError, ValueError):
            return float(d)
    sides = str(e.get("EMA20_SIDES", "both")).strip().lower()
    c.update(enabled=str(e.get("EMA20_ENABLED", "1")).strip().lower() in ("1", "true", "yes", "on"),
             start_balance=f("EMA20_START_BALANCE", 100),
             sides=("long",) if sides == "long" else ("short",) if sides == "short" else ("long", "short"),
             max_wait=int(f("EMA20_MAX_WAIT", 12)), min_r_pct=f("EMA20_MIN_R_PCT", 0.002),
             check_delay=f("EMA20_CHECK_DELAY_SEC", 8), poll_sec=f("EMA20_POLL_SEC", 3),
             fetch_workers=int(f("EMA20_FETCH_WORKERS", 4)), be_r=f("EMA20_BE_R", 1),
             trail_start_r=f("EMA20_TRAIL_START_R", 3))
    return c


def hm(ts):
    return datetime.fromtimestamp(ts, IST).strftime("%H:%M")


def ist(ts):
    return datetime.fromtimestamp(ts, IST).strftime("%Y-%m-%d %H:%M")


class Ema20:
    def __init__(self, bot, cfg=None):
        self.b, self.c = bot, cfg or load_cfg()
        self.st = bot.state.setdefault("ema20", {})
        for k, v in (("armed", {}), ("last_bar", 0), ("used", {})):
            self.st.setdefault(k, v)
        self.own_prods, self.own_prods_ts = {}, 0.0
        self.job = None                 # (bar, future) of the background candle fetch
        self.lock = threading.Lock()
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ema20")
        self.last_poll = 0.0
        self.exit_bar, self.exit_done = 0, set()     # 5m bar whose EMA exit check is running / trades checked

    # ---- books ----
    def ensure_books(self):
        books = self.b.state["books"]
        if BOOK not in books:
            bk = self.b.new_book(round(self.c["start_balance"], 4))
            bk["since"] = datetime.now(IST).isoformat(timespec="seconds")
            books[BOOK] = bk
            self.b.save_state()
            return [BOOK]
        return []

    def startup_line(self):
        c = self.c
        return (f"\nEMA20 PULLBACK [PAPER]: ON | ${self.b.book(BOOK)['balance']:.2f} | sides {'/'.join(c['sides'])} | "
                f"5m 20 EMA cross + 2-candle pullback, trigger break within {c['max_wait']} candles | stop: 5m close "
                f"beyond the pullback candle or the hard stop, 1R to the closer (>= {c['min_r_pct'] * 100:g}%) | exit "
                f"on a 5m close back through the EMA, BE {c['be_r']:g}R, trail from "
                f"{c['trail_start_r']:g}R, hard stop {c['hard_stop'] * 100:g}% | size as the 1H RSI books")

    # ---- universe ----
    def prods(self):
        r1 = getattr(self.b, "rsi1h", None)
        if r1 is not None and r1.prods:
            return r1.prods
        if not self.own_prods or time.time() - self.own_prods_ts > 3600:
            try:
                self.own_prods = R.Rsi1h.universe(self)        # same selection (uses self.b and self.c["min_vol"])
                self.own_prods_ts = time.time()
            except Exception:  # noqa: BLE001
                log.exception("EMA20 universe load failed")
        return self.own_prods

    # ---- schedule (main loop, every ~2 s) ----
    def tick(self, now):
        bar = int(now) // P5M * P5M
        if bar != self.st["last_bar"] and now >= bar + self.c["check_delay"]:
            if now <= bar + 90 and now - getattr(self, "_exit_try", 0) >= 2:
                self._exit_try = now
                self.check_ema_exits(now, bar)          # open trades first, synchronously (a few symbols)
            if self.job is None or self.job[0] != bar:
                self.start_fetch(now, bar)
        if self.job is not None and self.job[1].done():
            bar_j, fut = self.job
            self.job = None
            try:
                res = fut.result()
            except Exception:  # noqa: BLE001
                log.exception("EMA20 candle fetch failed")
                res = None
            if res is not None:
                self.on_close(now, bar_j, res)
        if (self.st["armed"] and now - self.last_poll >= self.c["poll_sec"] and not self.b.state.get("paused")
                and self.has_room()):
            self.last_poll = now
            try:
                self.poll(now)
            except Exception:  # noqa: BLE001
                log.exception("EMA20 price poll failed")

    def has_room(self):
        """Prices are only watched while the book could take a trade (as the 1H RSI books skip their checks)."""
        return (len(self.b.book_trades(BOOK)) < self.c["max_open"]
                and R.avail_margin(self.b, BOOK, self.c["min_trade_margin"])[1] is None)

    # ---- 5m close: candles for the universe in the background ----
    def start_fetch(self, now, bar):
        prods = self.prods()
        if not prods:
            return
        armed = {k.split("|")[0] for k in self.st["armed"]}
        held = {t["symbol"] for t in self.b.book_trades(BOOK)}
        syms = sorted(held) + sorted(armed - held) + sorted(set(prods) - armed - held)
        self.job = (bar, self.pool.submit(self.fetch_all, syms, bar, now))

    def fetch_all(self, syms, bar, now):
        """Worker thread: every symbol's setups at the last closed candle (small dicts only)."""
        c = self.c
        last = bar - P5M

        def one(sym):
            for _ in range(2):
                try:
                    cand = self.b.data.candles(sym, "5m", N_CANDLES, bar + 1)
                except Exception as ex:  # noqa: BLE001
                    err = str(ex)[:80]
                    time.sleep(1)
                    continue
                cand = [x for x in cand if x["time"] + P5M <= bar]
                if not cand or cand[-1]["time"] != last:
                    err = "last candle not in yet"
                    time.sleep(2)
                    continue
                n = len(cand) - 1
                out = {}
                for side in c["sides"]:
                    sets = E.scan_setups(cand, side, c["max_wait"], c["min_r_pct"])
                    out[side] = [s for s in sets if s.get("end_i", n) >= n - 1 or s["status"] in ("pullback", "armed")]
                    for s in out[side]:
                        s["last_i"] = n
                return sym, out, None
            return sym, None, err

        res, errors = {}, 0
        with ThreadPoolExecutor(max_workers=max(1, c["fetch_workers"])) as ex:
            for sym, out, err in ex.map(one, syms):
                if out is None:
                    errors += 1
                else:
                    res[sym] = out
        return {"res": res, "errors": errors, "n": len(syms), "took": round(time.time() - now, 1)}

    def on_close(self, now, bar, job):
        """Main thread: alerts, newly armed setups and cancels from the candles of the bar that just closed."""
        last_t = bar - P5M
        res, armed, used = job["res"], self.st["armed"], self.st["used"]
        n_alert = n_armed = n_cancel = 0
        seen, alerts = set(), {"long": [], "short": []}
        room = self.has_room()
        for sym, sides in res.items():
            for side, sets in sides.items():
                key = f"{sym}|{side}"
                cur = armed.get(key)
                for s in sets:
                    if s["alert_i"] == s["last_i"]:
                        n_alert += 1
                        alerts[side].append(sym)
                    if s["status"] == "armed" and key not in armed and used.get(key) != s["alert_time"]:
                        n_armed += 1
                        armed[key] = self.arm_record(sym, side, s)
                        a = armed[key]
                        self.b.event("EMA20_ARMED", book=BOOK, side=side, symbol=sym, trigger=a["trigger"],
                                     pullback_stop=a["pb"], low=a["low"], r=a["r"], r_pct=a["r_pct"], ema=a["ema"], alert=ist(a["alert_time"]),
                                     pullback=[ist(x) for x in a["pull"]], expires=ist(a["expires"]))
                    if cur and s["alert_time"] == cur["alert_time"]:
                        seen.add(key)
                        if s["status"] in ("cancelled", "expired", "breached"):
                            why = (s["reason"] if s["status"] != "breached" else
                                   "the trigger traded while the book had no room: no entry" if not room else
                                   "the trigger traded between two price checks (closed-candle high/low): no late entry")
                            self.cancel(key, why)
                            n_cancel += 1
        # armed setups whose symbol had no candles: expire on time; a symbol gone from the universe: cancel
        for key, a in list(armed.items()):
            sym = key.split("|")[0]
            if key in seen:
                continue
            if sym not in res and last_t >= a["expires"]:
                self.cancel(key, f"{self.c['max_wait']} candles without a breach (no candles at the last check)")
                n_cancel += 1
            elif sym in res and key not in seen and not any(
                    s["alert_time"] == a["alert_time"] for s in res[sym].get(a["side"], [])):
                self.cancel(key, "setup no longer found in the candles")
                n_cancel += 1
        if n_alert:                     # one event per 5m close (~20k alerts a day across the universe)
            self.b.event("EMA20_ALERT", book=BOOK, candle=ist(last_t), long=sorted(alerts["long"]),
                         short=sorted(alerts["short"]), count=n_alert)
        cut = now - 86400
        self.st["used"] = {k: v for k, v in used.items() if v >= cut}
        self.st["last_bar"] = bar
        self.b.save_state()
        log.info("EMA20 %s close: %d/%d symbols (%d errors, %.1fs) | alerts %d, armed %d new / %d now, cancelled %d",
                 hm(last_t), len(res), job["n"], job["errors"], job["took"], n_alert, n_armed, len(armed), n_cancel)

    def arm_record(self, sym, side, s):
        return {"symbol": sym, "side": side, "trigger": s["trigger"], "low": s["low"], "pb": s["pb"], "r": s["r"],
                "r_pct": round(s["r"] / s["trigger"] * 100, 3), "ema": round(s["ema"], 10),
                "alert_time": s["alert_time"], "pull": s["pull"][:2], "armed_time": s["armed_time"],
                "expires": s["armed_time"] + self.c["max_wait"] * P5M,
                "vol24h": (self.prods().get(sym) or {}).get("vol24h")}

    def cancel(self, key, why):
        a = self.st["armed"].pop(key, None)
        if a:
            self.st["used"][key] = a["alert_time"]
            self.b.event("EMA20_CANCEL", book=BOOK, side=a["side"], symbol=a["symbol"], reason=why,
                         trigger=a["trigger"], alert=ist(a["alert_time"]))

    # ---- live price watch on armed setups ----
    def poll(self, now):
        d = self.b.data.request("GET", "/fapi/v1/ticker/price")
        px = {x["symbol"]: float(x.get("price") or 0) for x in d or [] if isinstance(x, dict)}
        hits = []
        for key, a in self.st["armed"].items():
            p = px.get(a["symbol"]) or 0
            if p > 0 and ((p > a["trigger"]) if a["side"] == "long" else (p < a["trigger"])):
                hits.append((a, p))
        if not hits:
            return
        hits.sort(key=lambda h: -(h[0].get("vol24h") or 0))          # highest 24h volume first
        for rank, (a, p) in enumerate(hits, 1):
            key = f"{a['symbol']}|{a['side']}"
            self.st["armed"].pop(key, None)
            self.st["used"][key] = a["alert_time"]                       # a setup is used once
            sig = {"side": a["side"], "symbol": a["symbol"], "setup": a, "ltp": p, "cand": {"ltp": p}}
            self.b.event("EMA20_BREACH", book=BOOK, side=a["side"], symbol=a["symbol"], price=p, trigger=a["trigger"],
                         rank=rank)
            try:
                self.enter(sig, now)
            except Exception:  # noqa: BLE001
                log.exception("EMA20 entry failed")
                sig["skip_reason"] = "error"
            self.candidate(sig, rank, now)
        self.b.save_state()

    def skip(self, sig, reason, short=None):
        sig["skip_reason"] = short or reason
        self.b.event("EMA20_SKIP", book=BOOK, side=sig["side"], symbol=sig["symbol"], reason=reason,
                     trigger=sig["setup"]["trigger"])

    def enter(self, sig, now):
        c, b = self.c, self.b
        side, sym, a = sig["side"], sig["symbol"], sig["setup"]
        p = self.prods().get(sym)
        cand = sig["cand"]
        if len(b.book_trades(BOOK)) >= c["max_open"]:
            return self.skip(sig, f"{BOOK} already has {len(b.book_trades(BOOK))} trades open "
                                  f"(MAX_OPEN_TRADES {c['max_open']})", "max open trades")
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
        entry, tick, lev = lv[0][0], p["tick"], c["leverage"]
        hard = R.hard_stop_for(side, entry, tick, c["hard_stop"])
        liq = R.liq_price(side, entry, lev, c["mmr"])
        liq_pct = abs(liq - entry) / entry
        cand.update(liq_price=R.round_tick(liq, tick, "nearest"), hard_stop=hard, stop_used=hard,
                    stop_pct=abs(entry - hard) / entry * 100)
        if liq_pct <= c["hard_stop"] + b.cfg.get("entry_slip_cap_pct", 0.001):
            return self.skip(sig, f"liquidation {liq:g} ({liq_pct * 100:.2f}%) is not beyond the hard stop {hard:g} "
                                  f"at {lev:g}x", "liquidation before stop")
        equity = b.book(BOOK)["balance"]
        free = avail
        impact = R.impact_limit(lv, p["cv"], c["max_impact"])
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
                             impact_capped=info["binding"] == "price impact", sweep=R.walk_price(lv, n * p["cv"]))
        cand["exec"] = tx.get("exec")
        if why:
            return self.skip(sig, f"{why} (capped IOC entry)", why)
        n, entry = tx["contracts"], tx["entry"]
        qty = n * p["cv"]
        cand["size"] = qty
        hard = R.hard_stop_for(side, entry, tick, c["hard_stop"])
        liq = R.round_tick(R.liq_price(side, entry, lev, c["mmr"]), tick, "nearest")
        pb = a["pb"]
        if (side == "long" and pb >= entry) or (side == "short" and pb <= entry):
            return self.skip(sig, f"entry {entry:g} is already beyond the pullback stop {pb:g}", "price past stop")
        r = max(min(abs(entry - pb), abs(entry - hard)), entry * c["min_r_pct"])
        r_src = "pullback" if abs(entry - pb) <= abs(entry - hard) else "hard stop"
        loss_usd, loss_pct = R.loss_if_stopped(side, qty, entry, hard, b.cfg["fee_pct_per_side"], equity)
        cand.update(stop_used=hard, stop_pct=abs(entry - hard) / entry * 100, hard_stop=hard, liq_price=liq,
                    loss_pct_if_stopped=round(loss_pct, 2), r_pct=round(r / entry * 100, 3), stop_candle=pb)
        info.update(notional=qty * entry, risk_usd=qty * abs(entry - hard), margin=qty * entry / lev)
        x = tx["exec"]
        detail = (f"{'green' if side == 'long' else 'red'} alert {hm(a['alert_time'])} crossed the 20 EMA "
                  f"({a['ema']:.6g}); pullback {', '.join(hm(t) for t in a['pull'])}; trigger {a['trigger']:g} "
                  f"(armed {hm(a['armed_time'] + P5M)}, expires {hm(a['expires'] + P5M)}) | stop: a 5m close "
                  f"{'below' if side == 'long' else 'above'} the pullback {'low' if side == 'long' else 'high'} {pb:g} "
                  f"({abs(entry - pb) / entry * 100:.2f}%) or the hard stop {hard:g} ({c['hard_stop'] * 100:g}%) | 1R "
                  f"{r:.6g} = {r / entry * 100:.2f}% (to the {r_src}"
                  f"{', min ' + format(c['min_r_pct'] * 100, 'g') + '%' if r > min(abs(entry - pb), abs(entry - hard)) + 1e-12 else ''})"
                  f" | also exits on a 5m close {'below' if side == 'long' else 'above'} the 20 EMA, BE {c['be_r']:g}R, "
                  f"trail from {c['trail_start_r']:g}R"
                  f" | fill {qty:g}/{x['qty_intended']:g} @ {entry:g} (ref {x['entry_ref']:g}, cap {x['entry_cap']:g}, "
                  f"{x['entry_children']} IOC, slippage {x['entry_slip_pct']:+.3f}% = ${x['entry_slip_usd']:.4f}"
                  + (", price-impact limit" if x["liq_capped"] == "yes" else "") + ")"
                  f" | size: target {info['target_qty']:g} (free ${free:.2f} x {lev:g}), impact limit "
                  f"{info['impact_qty']:g} | liq {liq:g}, loss if stopped ${loss_usd:.2f} = {loss_pct:.1f}% of the book")
        t = {"mode": b.cfg["mode"], "book": BOOK, "side": side, "symbol": sym, "product_id": p["id"],
             "contracts": n, "cv": p["cv"], "tick": tick, "entry": entry, "stop": hard, "tp": None,
             "signal_close": sig["ltp"], "rsi": None, "strategy": "ema20",
             "extra": {"detail": detail, "trigger": a["trigger"], "low": a["low"], "pb": pb, "r": r, "ema": a["ema"],
                       "alert_time": a["alert_time"], "pull": a["pull"], "last_price": sig["ltp"],
                       "target_qty": info["target_qty"], "impact_qty": info["impact_qty"],
                       "size_binding": info["binding"], "free_margin": round(free, 4)},
             "ob": None, "setup_time": a["alert_time"], "opened_ts": int(now), "last_checked": int(now),
             "equity": equity, "protected": None, "stop_source": "hard", "exec": tx["exec"],
             "leverage": lev, "margin": info["margin"], "hard_stop": hard, "liq_price": liq,
             "loss_if_stopped_usd": round(loss_usd, 4), "loss_if_stopped_pct": round(loss_pct, 2),
             "trail": True, "trail_r": 0, "stop_set": True, "touch_close": 0, "tc_low": entry, "tc_high": entry,
             "be_r": c["be_r"], "trail_start_r": c["trail_start_r"], "r_ema": r}
        sig["taken"] = True
        R.record_leftover(b.book(BOOK), x["liq_capped"], free, info["margin"], sym, now)
        b.finish_entry(t, info, now)
        t["risk0"] = r                     # BE / trail work in setup R (finish_entry set the hard-stop distance)
        b.save_state()

    # ---- exits: a 5m close back through the EMA ----
    def check_ema_exits(self, now, bar):
        if self.exit_bar != bar:
            self.exit_bar, self.exit_done = bar, set()
        for t in list(self.b.book_trades(BOOK)):
            tid = (t["symbol"], t.get("opened_ts"))
            if tid in self.exit_done or t.get("pending_exit"):
                continue
            try:
                cand = self.b.data.candles(t["symbol"], "5m", N_CANDLES, bar + 1)
            except Exception as ex:  # noqa: BLE001
                log.warning("EMA20 exit check %s: %s", t["symbol"], ex)
                continue
            cand = [x for x in cand if x["time"] + P5M <= bar]
            if not cand or cand[-1]["time"] != bar - P5M:
                continue                            # candle not in yet: retried every 2 s until 90 s after the close
            self.exit_done.add(tid)
            if cand[-1]["time"] + P5M <= (t.get("opened_ts") or 0):
                continue                            # the trade opened after this candle closed
            e = E.ema_at_last(cand)
            last, long_ = cand[-1], t["side"] == "long"
            pb = (t.get("extra") or {}).get("pb")
            below = "below" if long_ else "above"
            if pb is not None and ((last["close"] < pb) if long_ else (last["close"] > pb)):
                reason, why = "PB_CLOSE", (f"5m {hm(last['time'])} closed {last['close']:g} {below} the pullback "
                                           f"{'low' if long_ else 'high'} {pb:g}")
            elif e is not None and E.closes_against(last, e, t["side"]):
                reason, why = "EMA_EXIT", f"5m {hm(last['time'])} closed {last['close']:g} {below} the 20 EMA {e:.6g}"
            else:
                continue
            px = self.b.last_price(t["symbol"])
            t.setdefault("extra", {})["ema_exit"] = why
            self.b.close_trade(t, {"reason": reason, "price": px, "ts": int(now), "estimated": True})

    # ---- candidates.csv ----
    def candidate(self, sig, rank, now):
        try:
            a, c = sig["setup"], sig.get("cand") or {}
            row = {"time": datetime.fromtimestamp(now, IST).strftime("%Y-%m-%d %H:%M:%S"), "bot": "binance-bot",
                   "book": BOOK, "symbol": sig["symbol"], "side": sig["side"].upper(), "check": "price beyond trigger",
                   "taken": "yes" if sig.get("taken") else "no",
                   "reason": "" if sig.get("taken") else sig.get("skip_reason", ""), "rank": rank,
                   "ltp": c.get("ltp"), "level": a["trigger"],
                   "distance_pct": round((c["ltp"] / a["trigger"] - 1) * 100, 4) if c.get("ltp") else None,
                   "range_low" if sig["side"] == "long" else "range_high": a["low"],
                   "ref_time": ist(a["alert_time"]), "stop_used": c.get("stop_used"),
                   "stop_pct": round(c["stop_pct"], 3) if c.get("stop_pct") is not None else None,
                   "size": c.get("size"), "vol24h_usdt": round(a["vol24h"]) if a.get("vol24h") else None,
                   "rank_by": "volume", "ema20": a["ema"], "r_pct": c.get("r_pct", a["r_pct"]),
                   "stop_candle": a.get("pb")}
            x = c.get("exec") or {}
            row.update({k: x.get(k) for k in ("depth5_qty", "liq_cap_qty", "liq_capped", "entry_ref", "entry_cap",
                                              "entry_avg", "entry_slip_pct", "entry_children", "qty_intended",
                                              "qty_filled")})
            row["entry_slip_money"] = x.get("entry_slip_usd")
            row.update({k: c.get(k) for k in ("free_margin", "target_qty", "impact_qty", "hard_stop", "liq_price",
                                              "loss_pct_if_stopped")})
            import csv
            path = os.path.join(self.b.cfg["data_dir"], "candidates.csv")
            new = not os.path.exists(path)
            with open(path, "a", newline="") as fh:
                w = csv.DictWriter(fh, fieldnames=R.CAND_COLS, extrasaction="ignore")
                if new:
                    w.writeheader()
                w.writerow({k: ("" if v is None else v) for k, v in row.items()})
        except Exception as ex:  # noqa: BLE001
            log.warning("candidates.csv (EMA20): %s", ex)

    # ---- reporting ----
    def status_lines(self):
        armed = sorted(self.st["armed"].values(), key=lambda a: -(a.get("vol24h") or 0))
        return ([f"EMA20 PULLBACK: armed {len(armed)}"
                 + (f" | last 5m check {hm(self.st['last_bar'] - P5M)}" if self.st["last_bar"] else "")]
                + [f"  {a['side'].upper()} {a['symbol']} trigger {a['trigger']:g}, pullback stop {a['pb']:g} "
                   f"(1R {a['r_pct']:.2f}%), "
                   f"until {hm(a['expires'] + P5M)}" for a in armed[:6]])
