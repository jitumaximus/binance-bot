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
          skipped, the next is tried. Up to MAX_OPEN_TRADES (3) open trades per book (26 Nov 2026). The first trade
          of an empty book is sized from its whole free margin; while trades are open, a new one is allowed ONLY for
          the leftover of the previous entry when that entry was cut down by the price-impact limit (liq_capped =
          yes), and only for that remainder (27 Nov 2026): margin freed by a trade closing never starts a new small
          trade while others are open. Any trade whose margin would be below MIN_TRADE_MARGIN_PCT (25%) of the
          book's balance is skipped ("too small"). A coin is never held twice. 30-minute cooldown (RSI1H_COOLDOWN_MIN) on
          a coin after a short on it closes; 60-minute cooldown (RSI1H_COOLDOWN_LONG_MIN, since 26 Nov 2026) after
          a long on it closes.
STOP      1 tick (STOP_BUFFER_TICKS) beyond the divergence candle's high (short) / low (long), at least
          RSI1H_MIN_STOP_PCT (0.5%) from entry (widened and logged if closer). Signals whose stop is more than
          MAX_STOP_PCT (3%) from the entry are skipped ("stop too wide"). Every trade also has a hard stop HARD_STOP_PCT
          (3%) against the entry; the active stop is whichever is closer to the entry (paper: simulated; exit reason
          HARD_STOP when the hard stop was the active one). The books refuse to run live, so no exchange STOP_MARKET is
          placed yet: wire one in before any live switch.
LIQ       isolated 20x: liquidation = entry x (1 - 1/lev) / (1 - mmr) long, entry x (1 + 1/lev) / (1 + mmr) short,
          mmr = RSI1H_MMR (1%; Binance's per-coin rate needs a signed call). A trade is refused unless the
          liquidation price is beyond the stop, with ENTRY_SLIP_CAP_PCT to spare for the fill.
EXIT      no target. At 1R the stop moves to entry; from 3R it trails in whole-R steps 1R behind (3R -> 2R, 4R -> 3R
          ...). Exit only on that stop (paper: checked on 1-minute candles, like POC).
SIZE      (26 Nov 2026, replaces 5% risk sizing and the 30%-of-top-5 cap for these books) target = the book's free
          margin x RSI1H_LEVERAGE (20x isolated); price-impact limit = the largest size whose average fill, walking our
          side of the live order book (50 levels), stays within MAX_IMPACT_PCT (0.05%) of the best price. Position =
          min(target, impact limit), rounded down to the symbol's step; then the capped IOC entry as before. No
          max-loss cap: the loss if the stop hits (incl. fees) is logged as % of the book.
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
             "entry_slip_money", "entry_children", "qty_intended", "qty_filled",
             # sizing v2 (26 Nov 2026, binance-bot): free margin x leverage vs the 0.05% price-impact limit (coins)
             "free_margin", "target_qty", "impact_qty", "hard_stop", "liq_price", "loss_pct_if_stopped"]


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
        "leverage": f("RSI1H_LEVERAGE", 20),
        # sizing v2 (26 Nov 2026); fractions, not percent
        "max_impact": f("MAX_IMPACT_PCT", 0.0005),       # average fill within 0.05% of the best price
        "max_open": int(f("MAX_OPEN_TRADES", 3)),         # open trades per book
        "min_trade_margin": f("MIN_TRADE_MARGIN_PCT", 0.25),  # 27 Nov 2026: skip trades with margin < 25% of the book
        "max_stop": f("MAX_STOP_PCT", 0.03),              # skip signals whose stop is further than 3%
        "hard_stop": f("HARD_STOP_PCT", 0.03),            # protective stop 3% against the entry
        "mmr": f("RSI1H_MMR", 0.01),                      # maintenance margin rate for the liquidation price
        "book_depth": int(f("RSI1H_BOOK_DEPTH", 50)),     # order-book levels walked (50 = request weight 2)
        "overbought": f("RSI1H_OVERBOUGHT", 70),
        "oversold": f("RSI1H_OVERSOLD", 30),
        "rsi_len": int(f("RSI1H_RSI_LEN", 14)),
        "ema_len": int(f("RSI1H_EMA_LEN", 5)),
        "short_window": f("RSI1H_SHORT_WINDOW_MIN", 45),
        "long_window": f("RSI1H_LONG_WINDOW_MIN", 120),
        "min_gap": int(f("RSI1H_MIN_GAP", 2)),
        "min_rsi_diff": f("RSI1H_MIN_RSI_DIFF", 3),
        "min_stop_pct": f("RSI1H_MIN_STOP_PCT", 0.5),
        "cooldown_min": f("RSI1H_COOLDOWN_MIN", 30),              # after a short closes
        "cooldown_long_min": f("RSI1H_COOLDOWN_LONG_MIN", 60),    # after a long closes (since 26 Nov 2026)
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


def impact_limit(levels, step, max_pct):
    """Largest quantity (a multiple of step) whose average fill, walking levels (our side, best first), stays within
    max_pct (a fraction) of the best price. Levels inside the band give room that pays for part of the first level
    beyond it. Returns 0 for an empty book. Limited to the levels given (a deep book counts only what was fetched)."""
    if not levels or step <= 0:
        return 0.0
    best = levels[0][0]
    band = best * max_pct
    room = q = 0.0
    for px, qty in levels:
        over = abs(px - best) - band
        if over <= 0:
            room -= over * qty
            q += qty
            continue
        take = min(qty, room / over)
        q += take
        if take < qty:
            break
        room -= take * over
    return round(math.floor(q / step + 1e-9) * step, 10)


def walk_price(levels, qty):
    """The price of the last level a market order for qty reaches (None for an empty book)."""
    px, left = None, qty
    for px, q in levels:
        left -= q
        if left <= 1e-12:
            break
    return px


def size_for(free, leverage, entry, step, min_notional, impact_qty):
    """Contracts (x step = quantity): min(free margin x leverage, the price-impact limit), rounded down to the step.
    Returns (contracts, info); contracts 0 with info["skip"] when Binance's min notional can't be met."""
    if entry <= 0 or step <= 0:
        return 0, {"skip": "invalid entry/step"}
    free = max(0.0, free)
    by_margin = math.floor(free * leverage / (entry * step) + 1e-9)
    by_impact = math.floor(max(0.0, impact_qty) / step + 1e-9)
    n = min(by_margin, by_impact)
    info = {"free_margin": round(free, 4), "target_qty": round(by_margin * step, 10),
            "impact_qty": round(by_impact * step, 10), "binding": "price impact" if by_impact < by_margin else "free margin"}
    min_n = max(1, math.ceil(min_notional / (entry * step) - 1e-9))
    if n < min_n:
        info["skip"] = (f"too little liquidity: the 0.05% price-impact limit is ${by_impact * step * entry:.2f} "
                        f"(< min notional ${min_notional:g})" if by_impact < by_margin else
                        f"no free margin: ${free:.2f} free x {leverage:g}x < min notional ${min_notional:g}")
        info["short"] = "too little liquidity" if by_impact < by_margin else "no free margin"
        return 0, info
    info.update(notional=n * step * entry, margin=n * step * entry / leverage)
    return n, info


def liq_price(side, entry, leverage, mmr):
    """Isolated-margin liquidation price (one position, first maintenance bracket, fees ignored)."""
    if side == "short":
        return entry * (1 + 1 / leverage) / (1 + mmr)
    return entry * (1 - 1 / leverage) / (1 - mmr)


def hard_stop_for(side, entry, tick, pct):
    """pct (a fraction) against the entry, rounded to the tick away from the entry."""
    return round_tick(entry * (1 + pct), tick, "up") if side == "short" else round_tick(entry * (1 - pct), tick, "down")


def active_stop(side, candle_stop, hard):
    """Whichever stop is closer to the entry. Returns (stop, "candle" | "hard")."""
    closer_hard = hard < candle_stop if side == "short" else hard > candle_stop
    return (hard, "hard") if closer_hard else (candle_stop, "candle")


def loss_if_stopped(side, qty, entry, stop, fee_pct, balance):
    """(usd, % of the book) lost if the stop fills exactly, incl. taker fees on both sides."""
    move = (stop - entry) if side == "short" else (entry - stop)
    usd = qty * move + fee_pct / 100.0 * qty * (entry + stop)
    return usd, (usd / balance * 100 if balance > 0 else 0.0)


# --------------------------------------------------------------------------
# Runner (uses the bot's data client, books, entry/exit bookkeeping and Telegram)
# --------------------------------------------------------------------------
class Rsi1h:
    def __init__(self, bot, cfg=None):
        self.b, self.c = bot, cfg or load_cfg()
        self.st = bot.state.setdefault("rsi1h", {})
        for k, v in (("armed_short", {}), ("armed_long", {}), ("last_scan", 0), ("universe", 0), ("cooldown", {}),
                     ("cooldown_long", {}), ("scan_ist", "")):
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
                  # named exit_estimated until 26 Nov 2026, which clashed with TRADE_HEAD's exit_estimated: the header
                  # lost that column at rsi_4h (25 Nov 2026 14:41 IST) and every older row read one column off
                  "exit_slip_estimated",
                  # 26 Nov 2026: market / limit / limit+market (limit: TRAIL_STOP whose market exit would slip > 0.1%)
                  "exit_type",
                  # 26 Nov 2026: exit_ref / exit_slip_* = vs the best bid (long) / ask (short) at the exit;
                  # exit_trigger_* = vs the stop / target that triggered it; signed, positive = worse; paper market
                  # exits: estimated on the live book, the P&L still uses the stop. entry_spread = ask - bid at entry
                  "exit_trigger", "exit_trigger_slip_pct", "exit_trigger_slip_usd", "entry_spread", "entry_spread_pct",
                  # 27 Nov 2026: net P&L as % of the margin the trade used (1H RSI)
                  "pnl_pct_margin"]

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
        head = [x for x in cols if x not in self.TRADE_TAIL and x != "exit_estimated"]
        if "rsi_4h" in head:            # every row writer puts exit_estimated right after rsi_4h (TRADE_HEAD)
            head.insert(head.index("rsi_4h") + 1, "exit_estimated")
        fixed = ",".join(head + self.TRADE_TAIL) + end
        if fixed != lines[0]:
            lines[0] = fixed
            tmp = path + ".tmp"
            with open(tmp, "w", newline="") as fh:
                fh.writelines(lines)
            os.replace(tmp, path)

    def ensure_cand_header(self):
        """candidates.csv: columns are only ever appended to CAND_COLS; older rows simply have fewer fields."""
        path = os.path.join(self.b.cfg["data_dir"], "candidates.csv")
        try:
            with open(path, newline="") as fh:
                lines = fh.readlines()
        except OSError:
            return
        if not lines:
            return
        end = lines[0][len(lines[0].rstrip("\r\n")):] or "\n"
        cols = lines[0].rstrip("\r\n").split(",")
        if cols != CAND_COLS and cols == CAND_COLS[:len(cols)]:
            lines[0] = ",".join(CAND_COLS) + end
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
                f"BE {c['be_r']:g}R, trail from {c['trail_start_r']:g}R, no target | size: free margin x {c['leverage']:g}x "
                f"isolated, capped at {c['max_impact'] * 100:g}% price impact, up to {c['max_open']} trades per book (extra trades "
                f"only for the leftover of an impact-capped entry), margin >= {c['min_trade_margin'] * 100:g}% of the book | "
                f"stop <= {c['max_stop'] * 100:g}% (else skipped), hard stop {c['hard_stop'] * 100:g}% | universe: USDT perps with 24h vol >= ${c['min_vol'] / 1e3:,.0f}k, hourly | "
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
            if (armed and not paused and self.has_room(book) and bnd + self.c["check_delay"] <= now <= bnd + 60
                    and now - self.last_try[side] >= 3):
                if self.done[side][0] != bnd:
                    self.done[side] = (bnd, set())
                pend = [s for s in armed if s not in self.done[side][1]]
                if pend:
                    self.last_try[side] = now
                    self.evaluate(now, bnd, side, pend)

    def has_room(self, book):
        """A book may open another trade while it has fewer than MAX_OPEN_TRADES open and enough margin for one."""
        return len(self.b.book_trades(book)) < self.c["max_open"] and self.avail_margin(book)[1] is None

    def avail_margin(self, book):
        """(margin a new trade may use, None) or (0, (reason, short reason)).
        Empty book: its whole free margin. Trades open: only the leftover of the previous entry if that entry was cut
        down by the price-impact limit (book["leftover"]), never more than the free margin. Below MIN_TRADE_MARGIN_PCT
        of the balance: too small."""
        b = self.b
        bk = b.book(book)
        free, n = b.free_margin(book), len(b.book_trades(book))
        if n == 0:
            bk.pop("leftover", None)                     # an empty book starts from its whole balance
            avail = free
        else:
            lo = bk.get("leftover") or {}
            if not lo.get("margin"):
                return 0.0, (f"{book} has {n} trade(s) open and the previous entry was not cut by the price-impact "
                             f"limit: no leftover to trade (free margin ${free:.2f} stays unused)", "no leftover")
            avail = min(free, lo["margin"])
        need = self.c["min_trade_margin"] * bk["balance"]
        if avail < need or avail <= 1e-6:
            return 0.0, (f"too small: ${avail:.2f} margin available < {self.c['min_trade_margin'] * 100:g}% of the "
                         f"book's balance ${bk['balance']:.2f} (${need:.2f})" + (f", {n} trade(s) open" if n else ""),
                         "too small")
        return avail, None

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
        for rank, sig in enumerate(sigs, 1):
            self.b.event("RSI1H_SIGNAL", book=book, side=side, symbol=sig["symbol"], rsi_1h=sig["rsi1h"],
                         vol24h_usdt=round(sig["vol24h"]) if sig.get("vol24h") else None, rank=rank,
                         rank_by=c["rank_by"],
                         rsi_ref=sig["rsi_ref"], rsi_div=sig["rsi_div"], rsi_gap=sig["rsi_gap"],
                         div_candle_open=sig["div_open"], stop_ref=sig["stop_ref"], detail=sig["detail"])
            self.enter(sig, now)                 # parallel trades: every signal is tried while margin is free
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
            row.update({k: c.get(k) for k in ("free_margin", "target_qty", "impact_qty", "hard_stop", "liq_price",
                                              "loss_pct_if_stopped")})
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
        """short: the reason as written in candidates.csv (no free margin, already held, cooldown, entry check, size, ...)."""
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
        if len(b.book_trades(book)) >= c["max_open"]:
            return self.skip(sig, book, f"{book} already has {len(b.book_trades(book))} trades open "
                                        f"(MAX_OPEN_TRADES {c['max_open']})", "max open trades")
        avail, why = self.avail_margin(book)
        if why:
            return self.skip(sig, book, why[0], why[1])
        if p is None:
            return self.skip(sig, book, "not in the universe any more", "not in universe")
        held = [n for n, t in b.open_trades() if t["product_id"] == p["id"]]
        if held:
            return self.skip(sig, book, f"this coin is already held by {held[0]}",
                             "already held" if held[0] == book else "held by another book")
        cd_key, cd_min = ("cooldown", c["cooldown_min"]) if side == "short" else ("cooldown_long", c["cooldown_long_min"])
        last = self.st[cd_key].get(sym)
        if last and now - last < cd_min * 60:
            return self.skip(sig, book, f"cooldown: last {sym} {side} closed {(now - last) / 60:.0f} min ago "
                                        f"(< {cd_min:g} min)", "cooldown")
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
            ob = b.data.orderbook(sym, c["book_depth"])
        except Exception as ex:  # noqa: BLE001
            return self.skip(sig, book, f"order book lookup failed: {str(ex)[:120]}", "no order book")
        lv = ob[1] if side == "long" else ob[0]
        if not lv:
            return self.skip(sig, book, "empty order book", "no order book")
        entry, tick, lev = lv[0][0], p["tick"], c["leverage"]
        buf = b.cfg.get("stop_buffer_ticks", 1)
        stop, candle_stop, widened = stop_for(side, sig["stop_ref"], entry, tick, buf, c["min_stop_pct"])
        cand.update(stop_candle=candle_stop, stop_used=stop, stop_pct=abs(entry - stop) / entry * 100)
        if (side == "short" and stop <= entry) or (side == "long" and (stop >= entry or stop <= 0)):
            return self.skip(sig, book, "entry is already beyond the stop", "price past stop")
        if abs(entry - stop) / entry > c["max_stop"] + 1e-12:
            return self.skip(sig, book, f"stop {stop:g} is {abs(entry - stop) / entry * 100:.2f}% from the entry "
                                        f"{entry:g} (> {c['max_stop'] * 100:g}%)", "stop too wide")
        # liquidation must be beyond the stop, with the IOC entry's slippage cap to spare (the fill can only be that
        # much worse than the reference; the stop distance from the fill is then at most this much bigger)
        liq = liq_price(side, entry, lev, c["mmr"])
        liq_pct = abs(liq - entry) / entry
        cand["liq_price"] = round_tick(liq, tick, "nearest")
        if liq_pct <= abs(entry - stop) / entry + b.cfg.get("entry_slip_cap_pct", 0.001):
            return self.skip(sig, book, f"liquidation {liq:g} ({liq_pct * 100:.2f}%) is not beyond the stop {stop:g} "
                                        f"({abs(entry - stop) / entry * 100:.2f}%) at {lev:g}x", "liquidation before stop")
        equity = b.book(book)["balance"]
        free = avail                                   # whole free margin (empty book) or the impact leftover
        impact = impact_limit(lv, p["cv"], c["max_impact"])
        n, info = size_for(free, lev, entry, p["cv"], p["min_notional"], impact)
        cand.update(free_margin=round(free, 4), target_qty=info.get("target_qty"), impact_qty=info.get("impact_qty"))
        if n < 1:
            return self.skip(sig, book, f"size: {info.get('skip')}", info.get("short", "size (min notional)"))
        need = c["min_trade_margin"] * equity
        if info["margin"] < need:
            return self.skip(sig, book, f"too small: margin ${info['margin']:.2f} ({info['binding']} binds) < "
                                        f"{c['min_trade_margin'] * 100:g}% of the book's balance ${equity:.2f} "
                                        f"(${need:.2f})", "too small")
        cand["size"] = n * p["cv"]
        tx = {"side": side, "symbol": sym, "product_id": p["id"], "contracts": n, "cv": p["cv"], "tick": tick}
        why = b.capped_entry(tx, p["min_notional"], ob, impact_qty=impact,
                             impact_capped=info["binding"] == "price impact", sweep=walk_price(lv, n * p["cv"]))
        cand["exec"] = tx.get("exec")
        if why:
            return self.skip(sig, book, f"{why} (capped IOC entry)", why)
        n, entry = tx["contracts"], tx["entry"]
        qty = n * p["cv"]
        cand["size"] = qty
        stop, candle_stop, widened = stop_for(side, sig["stop_ref"], entry, tick, buf, c["min_stop_pct"])
        hard = hard_stop_for(side, entry, tick, c["hard_stop"])
        stop_c = stop
        stop, src = active_stop(side, stop_c, hard)
        liq = round_tick(liq_price(side, entry, lev, c["mmr"]), tick, "nearest")
        loss_usd, loss_pct = loss_if_stopped(side, qty, entry, stop, b.cfg["fee_pct_per_side"], equity)
        cand.update(stop_candle=candle_stop, stop_used=stop, stop_pct=abs(entry - stop) / entry * 100, hard_stop=hard,
                    liq_price=liq, loss_pct_if_stopped=round(loss_pct, 2))
        if (side == "short" and stop <= entry) or (side == "long" and (stop >= entry or stop <= 0)):
            return self.skip(sig, book, "average fill is already beyond the stop", "fill past stop")
        info.update(notional=qty * entry, risk_usd=qty * abs(entry - stop), margin=qty * entry / lev)
        stop_pct = abs(entry - stop) / entry * 100
        detail = sig["detail"] + (
            f" | hard stop {hard:g} ({c['hard_stop'] * 100:g}%) is closer than the candle stop {stop_c:g}" if src == "hard" else
            f" | stop {stop:g} widened to {c['min_stop_pct']:g}% (candle stop {candle_stop:g} "
            f"was {abs(entry - candle_stop) / entry * 100:.2f}%)" if widened else
            f" | stop {stop:g} ({stop_pct:.2f}%, 1 tick beyond the divergence candle)")
        x = tx["exec"]
        detail += (f" | fill {qty:g}/{x['qty_intended']:g} @ {entry:g} (ref {x['entry_ref']:g}, cap {x['entry_cap']:g}, "
                   f"{x['entry_children']} IOC, slippage {x['entry_slip_pct']:+.3f}% = ${x['entry_slip_usd']:.4f}"
                   + (", price-impact limit" if x["liq_capped"] == "yes" else "") + ")")
        detail += (f" | size: target {info['target_qty']:g} (free ${free:.2f} x {lev:g}), impact limit "
                   f"{info['impact_qty']:g} | liq {liq:g}, loss if stopped ${loss_usd:.2f} = {loss_pct:.1f}% of the book")
        if widened and src == "candle":
            b.event("RSI1H_STOP_WIDENED", book=book, symbol=sym, candle_stop=candle_stop, stop=stop,
                    candle_stop_pct=round(abs(entry - candle_stop) / entry * 100, 3), min_pct=c["min_stop_pct"])
        t = {"mode": b.cfg["mode"], "book": book, "side": side, "symbol": sym, "product_id": p["id"],
             "contracts": n, "cv": p["cv"], "tick": tick, "entry": entry, "stop": stop, "tp": None,
             "signal_close": px, "rsi": sig["rsi1h"], "strategy": "rsi1h",
             "extra": {"detail": detail, "rsi_ref": sig["rsi_ref"], "rsi_div": sig["rsi_div"], "rsi_gap": sig["rsi_gap"],
                       "div_candle": sig["div_time"], "ref_candle": sig["ref_time"], "div_open": sig["div_open"],
                       "candle_stop": candle_stop, "stop_widened": widened, "last_price": px,
                       "slippage_pct": c["slippage_pct"], "target_qty": info["target_qty"],
                       "impact_qty": info["impact_qty"], "size_binding": info["binding"], "free_margin": round(free, 4)},
             "ob": None, "setup_time": sig["div_time"], "opened_ts": int(now), "last_checked": int(now),
             "equity": equity, "protected": None, "stop_source": src if src == "hard" else
             ("widened" if widened else "candle"), "exec": tx["exec"],
             "leverage": lev, "margin": info["margin"], "hard_stop": hard, "liq_price": liq,
             "loss_if_stopped_usd": round(loss_usd, 4), "loss_if_stopped_pct": round(loss_pct, 2),
             # exits: no target, BE at 1R, whole-R trail from 3R (read per trade by bot.check_exit_by_candles)
             "trail": True, "trail_r": 0, "stop_set": True, "touch_close": 0, "tc_low": entry, "tc_high": entry,
             "be_r": c["be_r"], "trail_start_r": c["trail_start_r"]}
        sig["taken"] = True
        bk = b.book(book)
        left = free - info["margin"]
        if x["liq_capped"] == "yes" and left > 1e-6:     # only an impact-capped entry leaves a leftover to trade
            bk["leftover"] = {"margin": round(left, 4), "after": sym, "ts": int(now)}
        else:
            bk.pop("leftover", None)
        b.finish_entry(t, info, now)

    # ---- hooks called from bot.close_trade ----
    def exit_price(self, t, px):
        slip = (t.get("extra") or {}).get("slippage_pct", 0) / 100.0
        return px * (1 + slip) if t["side"] == "short" else px * (1 - slip)

    def on_close(self, t, res):
        key = "cooldown" if t["side"] == "short" else "cooldown_long"
        self.st[key][t["symbol"]] = int(res["ts"])
        cut = res["ts"] - 86400
        self.st[key] = {k: v for k, v in self.st[key].items() if v >= cut}

    # ---- reporting ----
    def status_lines(self):
        st = self.st
        top_s = sorted(st["armed_short"].items(), key=lambda kv: -kv[1]["rsi"])[:5]
        top_l = sorted(st["armed_long"].items(), key=lambda kv: kv[1]["rsi"])[:5]
        return ([f"1H RSI: universe {st['universe']} (24h vol >= ${self.c['min_vol'] / 1e3:,.0f}k), last scan "
                 f"{st['scan_ist'] or '-'} | armed short {len(st['armed_short'])}, long {len(st['armed_long'])}"]
                + [f"  SHORT {s} 1H RSI {v['rsi']}" for s, v in top_s]
                + [f"  LONG {s} 1H RSI {v['rsi']}" for s, v in top_l])
