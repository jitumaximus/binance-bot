"""
1H RSI LONG / 1H RSI SHORT - the Fyers 2H RSI divergence strategy ported to Binance USDT-M perpetuals. PAPER ONLY.
=====================================================================================================================
Two books of their own (RSI1H_START_BALANCE, $100 each) in state.json next to POC RETURN. Started by bot.py when
RSI1H_ENABLED=1 (default) and only in paper mode (DRY_RUN=1) on EXCHANGE=binance. Crypto is 24/7: no session, no
"today only" candles, no square-off, no daily reset; every window simply rolls back from now.

UNIVERSE  every hour: every TRADING USDT perpetual on Binance (crypto, stocks, commodities) with 24h quote volume
          >= RSI1H_MIN_VOL_USDT ($500k). Production market data. 1H candles are fetched once an hour per symbol
          (98 candles = request weight 1); 5m / 15m candles only for armed symbols.
ARMING    after each 1H close: closed 1H RSI(14) > RSI1H_OVERBOUGHT (70) arms SHORT, < RSI1H_OVERSOLD (30) arms LONG.
          A coin stays armed only while the latest closed 1H RSI is beyond the threshold (re-checked every hour).
SHORT     5m candles, checked ~8 s after each 5m close. First peak = RED candle with close > EMA5 and low > EMA5;
          the highest such peak within RSI1H_SHORT_WINDOW_MIN (45) before the candle just closed, with at least
          RSI1H_MIN_GAP (2) candles between them. Divergence = the candle just closed: red, higher high than the peak,
          RSI(14, 5m) at least RSI1H_MIN_RSI_DIFF (3) points lower.
LONG      15m candles, ~8 s after each 15m close, RSI1H_LONG_WINDOW_MIN (120). First bottom = GREEN candle with
          high < EMA5 (the lowest such); divergence = the candle just closed: green, lower low, RSI(14, 15m) at least
          3 points higher, at least 2 candles between them.
ENTRY     market at the start of the next candle (the check ~8 s after the close), only if the last trade price is
          still below the red divergence candle's open (short) / above the green one's open (long); else skipped
          and logged. Paper fill = last trade price, RSI1H_SLIPPAGE_PCT against us (0 = like the POC paper fills),
          taker fee FEE_PCT_PER_SIDE on both sides (in bot.close_trade). Several coins at once: highest 24h quote
          volume first (RSI1H_RANK_BY=volume, default since 25 Nov 2026; =rsi: most extreme 1H RSI first); if one is
          skipped, the next is tried. One open trade per book. 30-minute cooldown (RSI1H_COOLDOWN_MIN) on a coin after a short on it closes.
STOP      1 tick (STOP_BUFFER_TICKS) beyond the divergence candle's high (short) / low (long), at least
          RSI1H_MIN_STOP_PCT (0.5%) from entry (widened and logged if closer).
EXIT      no target. At 1R the stop moves to entry; from 3R it trails in whole-R steps 1R behind (3R -> 2R, 4R -> 3R
          ...). Exit only on that stop (paper: checked on 1-minute candles, like POC).
SIZE      RSI1H_RISK_PCT (5%) of the book at the stop, notional capped at RSI1H_LEVERAGE (20x isolated) x the book,
          rounded down to the symbol's step; skipped and logged when Binance's min notional can't be met within that.
"""
import csv
import logging
import math
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from zoneinfo import ZoneInfo

log = logging.getLogger("bot")
IST = ZoneInfo("Asia/Kolkata")
LONG_BOOK, SHORT_BOOK = "1H RSI LONG", "1H RSI SHORT"
BOOKS = (LONG_BOOK, SHORT_BOOK)
P1H, P5M, P15M = 3600, 300, 900
N_CANDLES = 98                       # Binance klines: limit < 100 = request weight 1 (candles() asks for count + 1)
# candidates.csv (next to trades.csv): one row per divergence signal at each 5m / 15m check, taken or not. Same columns
# as the fyers-bot's data/candidates.csv; *_pct are percentages, blank = not applicable. No extra API calls.
CAND_COLS = ["time", "bot", "book", "symbol", "side", "check", "taken", "reason", "rank",
             "range_low", "range_high", "candle_open", "ltp", "move_pct", "candle_colour", "gap_pct", "beyond_pct",
             "arm_rsi", "ref_time", "ref_price", "ref_rsi", "div_time", "div_price", "div_rsi", "rsi_gap",
             "div_open", "entry_check", "level", "distance_pct", "stop_candle", "stop_used", "stop_pct", "size",
             "vol24h_usdt", "rank_by",   # binance-bot only
             # execution (25 Nov 2026), as in the fyers-bot (by name); qty_* in coins, size = coins traded
             "depth5_qty", "liq_cap_qty", "liq_capped", "entry_ref", "entry_cap", "entry_avg", "entry_slip_pct",
             "entry_slip_money", "entry_children", "qty_intended", "qty_filled"]


def load_cfg(env=None):
    e = os.environ if env is None else env

    def f(k, d):
        try:
            return float(e.get(k, d))
        except (TypeError, ValueError):
            return float(d)

    return {
        "enabled": str(e.get("RSI1H_ENABLED", "1")).strip().lower() in ("1", "true", "yes", "on"),
        "start_balance": f("RSI1H_START_BALANCE", 100),
        "risk_pct": f("RSI1H_RISK_PCT", 5),
        "leverage": f("RSI1H_LEVERAGE", 20),
        "overbought": f("RSI1H_OVERBOUGHT", 70),
        "oversold": f("RSI1H_OVERSOLD", 30),
        "rsi_len": int(f("RSI1H_RSI_LEN", 14)),
        "ema_len": int(f("RSI1H_EMA_LEN", 5)),
        "short_window": f("RSI1H_SHORT_WINDOW_MIN", 45),
        "long_window": f("RSI1H_LONG_WINDOW_MIN", 120),
        "min_gap": int(f("RSI1H_MIN_GAP", 2)),
        "min_rsi_diff": f("RSI1H_MIN_RSI_DIFF", 3),
        "min_stop_pct": f("RSI1H_MIN_STOP_PCT", 0.5),
        "cooldown_min": f("RSI1H_COOLDOWN_MIN", 30),
        "min_vol": f("RSI1H_MIN_VOL_USDT", 500_000),
        "slippage_pct": f("RSI1H_SLIPPAGE_PCT", 0),
        "be_r": f("RSI1H_BE_R", 1),
        "trail_start_r": f("RSI1H_TRAIL_START_R", 3),
        "check_delay": f("RSI1H_CHECK_DELAY_SEC", 8),
        # several divergences at one check: "volume" = highest 24h quote volume first (since 25 Nov 2026),
        # "rsi" = most extreme 1H RSI first (before)
        "rank_by": "rsi" if str(e.get("RSI1H_RANK_BY", "volume")).strip().lower() == "rsi" else "volume",
    }


def refuse_reason(bot_cfg):
    why = []
    if not bot_cfg.get("dry_run"):
        why.append("DRY_RUN is off (live trading)")
    if bot_cfg.get("exchange") != "binance":
        why.append(f"EXCHANGE={bot_cfg.get('exchange')} (needs binance)")
    return "; ".join(why)


# --------------------------------------------------------------------------
# Pure helpers (unit-tested)
# --------------------------------------------------------------------------
def rsi_series(closes, n=14):
    """Wilder RSI at every index (None before n). The last value equals bot.rsi_wilder(closes, n)."""
    out = [None] * len(closes)
    if len(closes) < n + 1:
        return out
    g = l_ = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        g += max(d, 0.0)
        l_ += max(-d, 0.0)
    ag, al = g / n, l_ / n

    def val(a, b):
        return (100.0 if a > 0 else 50.0) if b == 0 else 100.0 - 100.0 / (1.0 + a / b)
    out[n] = val(ag, al)
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        ag = (ag * (n - 1) + max(d, 0.0)) / n
        al = (al * (n - 1) + max(-d, 0.0)) / n
        out[i] = val(ag, al)
    return out


def ema_series(values, n):
    k, out, e = 2.0 / (n + 1), [], None
    for v in values:
        e = v if e is None else v * k + e * (1 - k)
        out.append(e)
    return out


def hm(ts):
    return datetime.fromtimestamp(ts, IST).strftime("%H:%M")


def div_trigger(cands, side, window_min, min_gap=2, min_diff=3.0, ema_len=5, rsi_len=14):
    """Fyers strategy._div_trigger without the session/day rules. cands: ascending CLOSED candles; the last one is
    the divergence candidate. Reference (short) = RED candle with close > EMA5 and low > EMA5 (the highest);
    (long) = GREEN candle with high < EMA5 (the lowest); within window_min of the candidate (candle start times)
    and at least min_gap candles between them. Returns the signal dict or None."""
    if len(cands) < max(ema_len, rsi_len) + 3:
        return None
    closes = [c["close"] for c in cands]
    e, r = ema_series(closes, ema_len), rsi_series(closes, rsi_len)
    n = len(cands) - 1
    d = cands[n]
    ref = None
    for k in range(n - 1 - min_gap, -1, -1):
        x = cands[k]
        if (d["time"] - x["time"]) / 60 > window_min:
            break
        if r[k] is None:
            continue
        if side == "short":
            ok = x["close"] < x["open"] and x["close"] > e[k] and x["low"] > e[k]
            better = ref is None or x["high"] > cands[ref]["high"]
        else:
            ok = x["close"] > x["open"] and x["high"] < e[k]
            better = ref is None or x["low"] < cands[ref]["low"]
        if ok and better:
            ref = k
    if ref is None or r[n] is None:
        return None
    p = cands[ref]
    between = n - ref - 1
    diff = (r[ref] - r[n]) if side == "short" else (r[n] - r[ref])
    ra, rb, rg = round(r[ref], 1), round(r[n], 1), round(diff, 1)          # as logged (the test uses the exact values)
    if side == "short":
        if not (d["close"] < d["open"] and d["high"] > p["high"] and diff >= min_diff):
            return None
        detail = (f"red peak {hm(p['time'])} high {p['high']:g} RSI {ra} (above EMA5, {between} candles between); "
                  f"red {hm(d['time'])} high {d['high']:g} RSI {rb}; RSI gap {rg} (>= {min_diff:g}) "
                  f"-> bearish divergence")
        stop_ref = d["high"]
    else:
        if not (d["close"] > d["open"] and d["low"] < p["low"] and diff >= min_diff):
            return None
        detail = (f"green bottom {hm(p['time'])} low {p['low']:g} RSI {ra} (high < EMA5, {between} candles between); "
                  f"green {hm(d['time'])} low {d['low']:g} RSI {rb}; RSI gap {rg} (>= {min_diff:g}) "
                  f"-> bullish divergence")
        stop_ref = d["low"]
    key = "high" if side == "short" else "low"
    return {"side": side, "stop_ref": stop_ref, "div_open": d["open"], "div_time": d["time"], "ref_time": p["time"],
            "ref_price": p[key], "div_price": d[key],
            "rsi_ref": ra, "rsi_div": rb, "rsi_gap": rg, "rsi_gap_exact": diff, "between": between, "detail": detail}


def rank_signals(sigs, side, rank_by):
    """Order of entry tries. volume: highest 24h quote volume first (unknown volume last), ties by 1H RSI;
    rsi: most extreme 1H RSI first (shorts highest, longs lowest)."""
    def by_rsi(s):
        return -s["rsi1h"] if side == "short" else s["rsi1h"]
    if rank_by == "volume":
        return sorted(sigs, key=lambda s: (-(s.get("vol24h") or 0), by_rsi(s)))
    return sorted(sigs, key=by_rsi)


def round_tick(price, tick, mode):
    if not tick or tick <= 0:
        return price
    q = price / tick
    q = math.ceil(q - 1e-9) if mode == "up" else math.floor(q + 1e-9) if mode == "down" else round(q)
    return q * tick


def stop_for(side, stop_ref, entry, tick, buffer_ticks, min_pct):
    """1 tick beyond the divergence candle; at least min_pct % from entry. Returns (stop, candle_stop, widened)."""
    if side == "short":
        candle = round_tick(stop_ref + buffer_ticks * tick, tick, "up")
        need = round_tick(entry * (1 + min_pct / 100.0), tick, "up")
        return (need, candle, True) if candle < need else (candle, candle, False)
    candle = round_tick(stop_ref - buffer_ticks * tick, tick, "down")
    need = round_tick(entry * (1 - min_pct / 100.0), tick, "down")
    return (need, candle, True) if candle > need else (candle, candle, False)


def size_for(balance, risk_pct, leverage, entry, stop, step, min_notional):
    """Contracts (x step = quantity) risking risk_pct of the balance at the stop, notional <= leverage x balance.
    Returns (contracts, info); contracts 0 with info["skip"] when it can't be done."""
    dist = abs(entry - stop)
    if entry <= 0 or dist <= 0 or step <= 0 or balance <= 0:
        return 0, {"skip": "invalid entry/stop/step/balance"}
    budget = balance * risk_pct / 100.0
    by_risk = math.floor(budget / (dist * step) + 1e-9)
    by_lev = math.floor(balance * leverage / (entry * step) + 1e-9)
    n = max(0, min(by_risk, by_lev))
    info = {"risk_budget": round(budget, 4), "by_risk": by_risk, "by_leverage": by_lev,
            "binding": "leverage cap" if by_lev < by_risk else "risk budget"}
    min_n = math.ceil(min_notional / (entry * step) - 1e-9)
    if n < max(1, min_n):
        need = max(1, min_n)
        info["skip"] = (f"min notional ${min_notional:g} needs {need * step:g} units = ${need * step * dist:.2f} risk "
                        f"(> {risk_pct:g}% = ${budget:.2f})" if need * step * dist > budget + 1e-12 else
                        f"min notional ${min_notional:g} exceeds the {leverage:g}x leverage cap")
        return 0, info
    info.update(notional=n * step * entry, risk_usd=n * step * dist, margin=n * step * entry / leverage)
    return n, info


# --------------------------------------------------------------------------
# Runner (uses the bot's data client, books, entry/exit bookkeeping and Telegram)
# --------------------------------------------------------------------------
class Rsi1h:
    def __init__(self, bot, cfg=None):
        self.b, self.c = bot, cfg or load_cfg()
        self.st = bot.state.setdefault("rsi1h", {})
        for k, v in (("armed_short", {}), ("armed_long", {}), ("last_scan", 0), ("universe", 0), ("cooldown", {}),
                     ("scan_ist", "")):
            self.st.setdefault(k, v)
        self.prods = {}                  # symbol -> {"id", "symbol", "tick", "cv", "min_notional"}
        self.next_scan_try = 0.0
        self.done = {"short": (0, set()), "long": (0, set())}
        self.last_try = {"short": 0.0, "long": 0.0}

    # ---- books ----
    def ensure_books(self):
        """Each book starts with its own RSI1H_START_BALANCE (never funded from POC)."""
        books = self.b.state["books"]
        made = []
        for name in BOOKS:
            if name not in books:
                bk = self.b.new_book(round(self.c["start_balance"], 4))
                bk["since"] = datetime.now(IST).isoformat(timespec="seconds")
                books[name] = bk
                made.append(name)
        if made:
            self.b.save_state()
        return made

    TRADE_HEAD = ["open_time_ist", "close_time_ist", "mode", "symbol", "side", "contracts", "entry", "stop_loss",
                  "take_profit", "exit_price", "exit_reason", "gross_pnl_usd", "est_fees_usd", "net_pnl_usd",
                  "net_pnl_inr", "r_multiple", "rsi_4h", "exit_estimated", "ob_ratio", "strategy", "book_balance_usd"]
    TRADE_TAIL = ["session", "shadow_exit_pnl", "setup",   # session/shadow: BOOK JUMP's (kept); setup: 1H RSI
                  # execution (25 Nov 2026), every book: entry = capped IOC children, exit = market (paper: estimated
                  # on the live depth); slip: positive = worse than the reference; qty in coins
                  "entry_ref", "entry_avg", "entry_slip_pct", "entry_slip_usd", "entry_children", "qty_intended",
                  "qty_filled", "liq_capped", "exit_ref", "exit_avg", "exit_slip_pct", "exit_slip_usd", "exit_children",
                  "exit_estimated"]

    def ensure_trades_header(self):
        """trades.csv ends with ...,session,shadow_exit_pnl,setup. Only the header line is touched (older rows simply
        have fewer columns); 1H RSI rows fill 'setup' with the divergence detail (both RSI values and the gap)."""
        path = self.b.trades_file
        if not os.path.exists(path):
            with open(path, "w", newline="") as fh:
                csv.writer(fh).writerow(self.TRADE_HEAD + self.TRADE_TAIL)
            return
        with open(path, newline="") as fh:
            lines = fh.readlines()
        if not lines:
            return
        end = lines[0][len(lines[0].rstrip("\r\n")):] or "\n"
        cols = lines[0].rstrip("\r\n").split(",")
        fixed = ",".join([x for x in cols if x not in self.TRADE_TAIL] + self.TRADE_TAIL) + end
        if fixed != lines[0]:
            lines[0] = fixed
            tmp = path + ".tmp"
            with open(tmp, "w", newline="") as fh:
                fh.writelines(lines)
            os.replace(tmp, path)

    def startup_line(self):
        c = self.c
        return (f"\n1H RSI LONG / 1H RSI SHORT [PAPER]: ON | ${self.b.book(LONG_BOOK)['balance']:.2f} / "
                f"${self.b.book(SHORT_BOOK)['balance']:.2f} | 1H RSI(14) >{c['overbought']:g} short / "
                f"<{c['oversold']:g} long, divergence on 5m ({c['short_window']:g} min) / 15m ({c['long_window']:g} min), "
                f"RSI gap >= {c['min_rsi_diff']:g} | stop beyond the divergence candle, min {c['min_stop_pct']:g}% | "
                f"BE {c['be_r']:g}R, trail from {c['trail_start_r']:g}R, no target | risk {c['risk_pct']:g}%, "
                f"{c['leverage']:g}x isolated | universe: USDT perps with 24h vol >= ${c['min_vol'] / 1e3:,.0f}k, hourly | "
                f"several signals: {'highest 24h volume' if c['rank_by'] == 'volume' else 'most extreme 1H RSI'} first")

    # ---- schedule (called from the bot's main loop every ~2 s) ----
    def tick(self, now):
        b1 = int(now) // P1H * P1H
        if b1 != self.st["last_scan"] and now >= b1 + self.c["check_delay"] and now >= self.next_scan_try:
            try:
                self.scan(now, b1)
            except Exception:  # noqa: BLE001
                self.next_scan_try = now + 60
                log.exception("1H RSI scan failed")
                return
        if not self.prods and now >= self.next_scan_try:
            # after a restart the armed coins come from state.json but the universe (ids, ticks, 24h volumes) is only
            # in memory: load it now (2 light requests, no candles) instead of skipping every signal as "not in the
            # universe" until the next hourly scan
            try:
                self.prods = self.universe()
                log.info("1H RSI: universe loaded at start: %d symbols", len(self.prods))
            except Exception:  # noqa: BLE001
                log.exception("1H RSI universe load failed")
            if not self.prods:
                self.next_scan_try = now + 60
        paused = self.b.state.get("paused")
        for side, per, book in (("short", P5M, SHORT_BOOK), ("long", P15M, LONG_BOOK)):
            bnd = int(now) // per * per
            armed = self.st["armed_" + side]
            if (armed and not paused and self.b.book_free(book) and bnd + self.c["check_delay"] <= now <= bnd + 60
                    and now - self.last_try[side] >= 3):
                if self.done[side][0] != bnd:
                    self.done[side] = (bnd, set())
                pend = [s for s in armed if s not in self.done[side][1]]
                if pend:
                    self.last_try[side] = now
                    self.evaluate(now, bnd, side, pend)

    # ---- hourly universe + arming ----
    def universe(self):
        """Every TRADING USDT perpetual with 24h quote volume >= min_vol (not the bot's MIN_TURNOVER_USD list)."""
        data = self.b.data
        raw = data.products(None)
        vol = {t["symbol"]: t.get("turnover_usd", -1) for t in data.tickers(None)}
        prods = {}
        for p in raw:
            if p.get("state") != "live" or vol.get(p["symbol"], -1) < self.c["min_vol"]:
                continue
            m = getattr(data, "meta", {}).get(p["id"], {})
            prods[p["symbol"]] = {"id": p["id"], "symbol": p["symbol"], "tick": float(p["tick_size"]),
                                  "cv": float(p["contract_value"]), "min_notional": float(m.get("min_notional") or 5),
                                  "vol24h": float(vol[p["symbol"]])}
        return prods

    def scan(self, now, b1):
        c = self.c
        self.prods = self.universe()
        expected = b1 - P1H

        def work(sym):
            try:
                return sym, self.b.data.candles(sym, "1h", N_CANDLES, now), None
            except Exception as ex:  # noqa: BLE001
                return sym, None, str(ex)

        old_s, old_l = self.st["armed_short"], self.st["armed_long"]
        new_s, new_l = {}, {}
        checked = stale = errors = 0
        with ThreadPoolExecutor(max_workers=max(1, min(6, self.b.cfg.get("workers", 6)))) as ex:
            for sym, cand, err in ex.map(work, sorted(self.prods)):
                if err or cand is None:
                    errors += 1
                    continue
                cand = [x for x in cand if x["time"] + P1H <= now]
                if len(cand) < c["rsi_len"] + 20 or cand[-1]["time"] != expected:
                    stale += 1
                    continue
                checked += 1
                r = rsi_series([x["close"] for x in cand], c["rsi_len"])[-1]
                if r is None:
                    continue
                if r > c["overbought"]:
                    new_s[sym] = {"rsi": round(r, 2), "candle": expected}
                elif r < c["oversold"]:
                    new_l[sym] = {"rsi": round(r, 2), "candle": expected}
        self.st.update(armed_short=new_s, armed_long=new_l, last_scan=b1, universe=len(self.prods),
                       scan_ist=datetime.fromtimestamp(b1, IST).strftime("%Y-%m-%d %H:%M IST"))
        self.b.save_state()
        self.b.event("RSI1H_SCAN", candle=datetime.fromtimestamp(expected, IST).strftime("%Y-%m-%d %H:%M IST"),
                     universe=len(self.prods), checked=checked, stale=stale, errors=errors,
                     armed_short=len(new_s), armed_long=len(new_l),
                     newly_short=sorted(set(new_s) - set(old_s)), newly_long=sorted(set(new_l) - set(old_l)),
                     dropped_short=sorted(set(old_s) - set(new_s)), dropped_long=sorted(set(old_l) - set(new_l)))
        log.info("1H RSI scan %s: universe %d (24h vol >= $%s), checked %d, stale %d, errors %d | armed short %d, "
                 "long %d", self.st["scan_ist"], len(self.prods), f"{c['min_vol']:,.0f}", checked, stale, errors,
                 len(new_s), len(new_l))

    # ---- 5m / 15m divergence check ----
    def evaluate(self, now, bnd, side, pend):
        c = self.c
        tf, per = ("5m", P5M) if side == "short" else ("15m", P15M)
        window = c["short_window"] if side == "short" else c["long_window"]
        armed = self.st["armed_" + side]

        def work(sym):
            try:
                return sym, self.b.data.candles(sym, tf, N_CANDLES, now), None
            except Exception as ex:  # noqa: BLE001
                return sym, None, str(ex)

        sigs = []
        with ThreadPoolExecutor(max_workers=max(1, min(6, self.b.cfg.get("workers", 6)))) as ex:
            for sym, cand, err in ex.map(work, pend):
                if err or cand is None:
                    continue
                cand = [x for x in cand if x["time"] + per <= now]
                if not cand or cand[-1]["time"] != bnd - per:
                    continue                                 # candle not in yet: retried in a few seconds
                self.done[side][1].add(sym)
                sig = div_trigger(cand, side, window, c["min_gap"], c["min_rsi_diff"], c["ema_len"], c["rsi_len"])
                if sig and sym in armed:
                    sig.update(symbol=sym, rsi1h=armed[sym]["rsi"], vol24h=(self.prods.get(sym) or {}).get("vol24h"))
                    sigs.append(sig)
        sigs = rank_signals(sigs, side, c["rank_by"])    # RSI1H_RANK_BY: volume (default) or rsi
        book = SHORT_BOOK if side == "short" else LONG_BOOK
        taken = None
        for rank, sig in enumerate(sigs, 1):
            self.b.event("RSI1H_SIGNAL", book=book, side=side, symbol=sig["symbol"], rsi_1h=sig["rsi1h"],
                         vol24h_usdt=round(sig["vol24h"]) if sig.get("vol24h") else None, rank=rank,
                         rank_by=c["rank_by"],
                         rsi_ref=sig["rsi_ref"], rsi_div=sig["rsi_div"], rsi_gap=sig["rsi_gap"],
                         div_candle_open=sig["div_open"], stop_ref=sig["stop_ref"], detail=sig["detail"])
            self.enter(sig, now)
            if "skip_reason" not in sig and not self.b.book_free(book):
                taken = sig["symbol"]
                sig["taken"] = True
            elif taken and sig.get("skip_reason", "").startswith("book busy"):
                sig["skip_reason"] = "ranked lower"
            self.candidate(sig, book, rank, f"{tf} close", now, c["rank_by"])

    def candidate(self, sig, book, rank, check, now, rank_by=""):
        """One candidates.csv row. Never raises: a logging problem must not affect trading."""
        try:
            c = sig.get("cand") or {}
            px = c.get("ltp")
            row = {"time": datetime.fromtimestamp(now, IST).strftime("%Y-%m-%d %H:%M:%S"), "bot": "binance-bot",
                   "book": book, "symbol": sig["symbol"], "side": sig["side"].upper(), "check": check,
                   "taken": "yes" if sig.get("taken") else "no",
                   "reason": "" if sig.get("taken") else sig.get("skip_reason", ""), "rank": rank,
                   "ltp": px, "arm_rsi": sig["rsi1h"],
                   "ref_time": datetime.fromtimestamp(sig["ref_time"], IST).strftime("%Y-%m-%d %H:%M"),
                   "ref_price": sig["ref_price"], "ref_rsi": sig["rsi_ref"],
                   "div_time": datetime.fromtimestamp(sig["div_time"], IST).strftime("%Y-%m-%d %H:%M"),
                   "div_price": sig["div_price"], "div_rsi": sig["rsi_div"], "rsi_gap": sig["rsi_gap"],
                   "div_open": sig["div_open"],
                   "entry_check": (f"{px:g} {'<' if px < sig['div_open'] else '>='} open {sig['div_open']:g}"
                                   if px and sig["side"] == "short" else
                                   f"{px:g} {'>' if px > sig['div_open'] else '<='} open {sig['div_open']:g}" if px else ""),
                   "stop_candle": c.get("stop_candle"), "stop_used": c.get("stop_used"),
                   "stop_pct": round(c["stop_pct"], 3) if c.get("stop_pct") is not None else None,
                   "size": c.get("size"), "vol24h_usdt": round(sig["vol24h"]) if sig.get("vol24h") else None,
                   "rank_by": rank_by}
            x = c.get("exec") or {}
            row.update({k: x.get(k) for k in ("depth5_qty", "liq_cap_qty", "liq_capped", "entry_ref", "entry_cap",
                                              "entry_avg", "entry_slip_pct", "entry_children", "qty_intended",
                                              "qty_filled")})
            row["entry_slip_money"] = x.get("entry_slip_usd")
            path = os.path.join(self.b.cfg["data_dir"], "candidates.csv")
            new = not os.path.exists(path)
            with open(path, "a", newline="") as fh:
                w = csv.DictWriter(fh, fieldnames=CAND_COLS, extrasaction="ignore")
                if new:
                    w.writeheader()
                w.writerow({k: ("" if v is None else v) for k, v in row.items()})
        except Exception as ex:  # noqa: BLE001
            log.warning("candidates.csv: %s", ex)

    def skip(self, sig, book, reason, short=None):
        """short: the reason as written in candidates.csv (book busy, cooldown, entry check, size, ...)."""
        sig["skip_reason"] = short or reason
        self.b.event("RSI1H_SKIP", book=book, side=sig["side"], symbol=sig["symbol"], reason=reason,
                     rsi_1h=sig["rsi1h"], rsi_gap=sig["rsi_gap"])

    def last_trade_price(self, sym):
        d = self.b.data.request("GET", "/fapi/v1/ticker/price", params={"symbol": sym})
        return float(d.get("price") or 0)

    def enter(self, sig, now):
        c, b = self.c, self.b
        side, sym = sig["side"], sig["symbol"]
        book = SHORT_BOOK if side == "short" else LONG_BOOK
        p = self.prods.get(sym)
        cand = sig.setdefault("cand", {})          # numbers for candidates.csv
        if b.state.get("paused"):
            return self.skip(sig, book, "bot is paused", "paused")
        if not b.book_free(book):
            return self.skip(sig, book, f"{book} already has a trade open", "book busy")
        if p is None:
            return self.skip(sig, book, "not in the universe any more", "not in universe")
        if any(t["product_id"] == p["id"] for _, t in b.open_trades()):
            return self.skip(sig, book, "this coin is already held by another book", "held by another book")
        last = self.st["cooldown"].get(sym)
        if side == "short" and last and now - last < c["cooldown_min"] * 60:
            return self.skip(sig, book, f"cooldown: last {sym} short closed {(now - last) / 60:.0f} min ago "
                                        f"(< {c['cooldown_min']:g} min)", "cooldown")
        try:
            px = self.last_trade_price(sym)
        except Exception as ex:  # noqa: BLE001
            return self.skip(sig, book, f"price lookup failed: {str(ex)[:120]}", "no price")
        if px <= 0:
            return self.skip(sig, book, "no price")
        cand["ltp"] = px
        if side == "short" and not px < sig["div_open"]:
            return self.skip(sig, book, f"price {px:g} not below the red divergence candle's open {sig['div_open']:g}",
                             "entry check")
        if side == "long" and not px > sig["div_open"]:
            return self.skip(sig, book, f"price {px:g} not above the green divergence candle's open {sig['div_open']:g}",
                             "entry check")
        # reference = best ask (long) / best bid (short) now: it sizes the trade; the capped IOC entry then fills
        try:
            ob = b.data.orderbook(sym, 20)
        except Exception as ex:  # noqa: BLE001
            return self.skip(sig, book, f"order book lookup failed: {str(ex)[:120]}", "no order book")
        lv = ob[1] if side == "long" else ob[0]
        if not lv:
            return self.skip(sig, book, "empty order book", "no order book")
        entry = lv[0][0]
        stop, candle_stop, widened = stop_for(side, sig["stop_ref"], entry, p["tick"],
                                              b.cfg.get("stop_buffer_ticks", 1), c["min_stop_pct"])
        cand.update(stop_candle=candle_stop, stop_used=stop, stop_pct=abs(entry - stop) / entry * 100)
        if (side == "short" and stop <= entry) or (side == "long" and (stop >= entry or stop <= 0)):
            return self.skip(sig, book, "entry is already beyond the stop", "price past stop")
        equity = b.book(book)["balance"]
        n, info = size_for(equity, c["risk_pct"], c["leverage"], entry, stop, p["cv"], p["min_notional"])
        if n < 1:
            return self.skip(sig, book, f"size: {info.get('skip')}", "size (min notional)")
        cand["size"] = n * p["cv"]
        tx = {"side": side, "symbol": sym, "product_id": p["id"], "contracts": n, "cv": p["cv"], "tick": p["tick"]}
        why = b.capped_entry(tx, p["min_notional"], ob)
        cand["exec"] = tx.get("exec")
        if why:
            return self.skip(sig, book, f"{why} (liquidity cap / capped IOC entry)", why)
        n, entry = tx["contracts"], tx["entry"]
        cand["size"] = n * p["cv"]
        stop, candle_stop, widened = stop_for(side, sig["stop_ref"], entry, p["tick"],
                                              b.cfg.get("stop_buffer_ticks", 1), c["min_stop_pct"])
        cand.update(stop_candle=candle_stop, stop_used=stop, stop_pct=abs(entry - stop) / entry * 100)
        if (side == "short" and stop <= entry) or (side == "long" and (stop >= entry or stop <= 0)):
            return self.skip(sig, book, "average fill is already beyond the stop", "fill past stop")
        info.update(notional=n * p["cv"] * entry, risk_usd=n * p["cv"] * abs(entry - stop),
                    margin=n * p["cv"] * entry / c["leverage"])
        stop_pct = abs(entry - stop) / entry * 100
        detail = sig["detail"] + (f" | stop {stop:g} widened to {c['min_stop_pct']:g}% (candle stop {candle_stop:g} "
                                  f"was {abs(entry - candle_stop) / entry * 100:.2f}%)" if widened else
                                  f" | stop {stop:g} ({stop_pct:.2f}%, 1 tick beyond the divergence candle)")
        x = tx["exec"]
        detail += (f" | fill {n * p['cv']:g}/{x['qty_intended']:g} @ {entry:g} (ref {x['entry_ref']:g}, cap {x['entry_cap']:g}, "
                   f"{x['entry_children']} IOC, slippage {x['entry_slip_pct']:+.3f}% = ${x['entry_slip_usd']:.4f}"
                   + (", liquidity cap" if x["liq_capped"] == "yes" else "") + ")")
        if widened:
            b.event("RSI1H_STOP_WIDENED", book=book, symbol=sym, candle_stop=candle_stop, stop=stop,
                    candle_stop_pct=round(abs(entry - candle_stop) / entry * 100, 3), min_pct=c["min_stop_pct"])
        t = {"mode": b.cfg["mode"], "book": book, "side": side, "symbol": sym, "product_id": p["id"],
             "contracts": n, "cv": p["cv"], "tick": p["tick"], "entry": entry, "stop": stop, "tp": None,
             "signal_close": px, "rsi": sig["rsi1h"], "strategy": "rsi1h",
             "extra": {"detail": detail, "rsi_ref": sig["rsi_ref"], "rsi_div": sig["rsi_div"], "rsi_gap": sig["rsi_gap"],
                       "div_candle": sig["div_time"], "ref_candle": sig["ref_time"], "div_open": sig["div_open"],
                       "candle_stop": candle_stop, "stop_widened": widened, "last_price": px,
                       "slippage_pct": c["slippage_pct"]},
             "ob": None, "setup_time": sig["div_time"], "opened_ts": int(now), "last_checked": int(now),
             "equity": equity, "protected": None, "stop_source": "widened" if widened else "candle", "exec": tx["exec"],
             "leverage": c["leverage"], "margin": info["margin"],
             # exits: no target, BE at 1R, whole-R trail from 3R (read per trade by bot.check_exit_by_candles)
             "trail": True, "trail_r": 0, "stop_set": True, "touch_close": 0, "tc_low": entry, "tc_high": entry,
             "be_r": c["be_r"], "trail_start_r": c["trail_start_r"]}
        b.finish_entry(t, info, now)

    # ---- hooks called from bot.close_trade ----
    def exit_price(self, t, px):
        slip = (t.get("extra") or {}).get("slippage_pct", 0) / 100.0
        return px * (1 + slip) if t["side"] == "short" else px * (1 - slip)

    def on_close(self, t, res):
        if t["side"] == "short":
            self.st["cooldown"][t["symbol"]] = int(res["ts"])
            cut = res["ts"] - 86400
            self.st["cooldown"] = {k: v for k, v in self.st["cooldown"].items() if v >= cut}

    # ---- reporting ----
    def status_lines(self):
        st = self.st
        top_s = sorted(st["armed_short"].items(), key=lambda kv: -kv[1]["rsi"])[:5]
        top_l = sorted(st["armed_long"].items(), key=lambda kv: kv[1]["rsi"])[:5]
        return ([f"1H RSI: universe {st['universe']} (24h vol >= ${self.c['min_vol'] / 1e3:,.0f}k), last scan "
                 f"{st['scan_ist'] or '-'} | armed short {len(st['armed_short'])}, long {len(st['armed_long'])}"]
                + [f"  SHORT {s} 1H RSI {v['rsi']}" for s, v in top_s]
                + [f"  LONG {s} 1H RSI {v['rsi']}" for s, v in top_l])
