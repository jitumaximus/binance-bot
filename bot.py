#!/usr/bin/env python3
"""
Delta Exchange India - three strategies, three independent accounts
===================================================================
The account (demo or real) is split into three books, one per strategy:
  2H RSI SHORT, 2H RSI LONG, 1H BREAKOUT.
Each book has its own balance (starts at 1/3 of the account), its own sizing, one position at a time,
its own daily loss limit and its own results. Two books never hold the same coin at the same time
(Delta nets positions per contract), so the first one to trigger gets it.

1H RSI LONG / 1H RSI SHORT (RSI1H_ENABLED=1, default; paper mode on Binance only): rsi1h.py - the Fyers 2H RSI
  divergence strategy on 1H RSI arming, 5m (short) / 15m (long) divergence, two $100 books next to POC RETURN.

BOOK JUMP / BJ ABSORB (bookjump.py): retired 23 Nov 2026; only started with BJ_ENABLED=1 (default 0).

POC RETURN (ENABLE_POC=1, paper mode only - its own book, normally run alone as "bot 2")
  POC_ENABLED=0 pauses it (no new orders or trades; book, state and logs kept; shown as "paused").
  Each US session (09:30-16:00 New York) -> volume profile of its 15m candles -> POC, active until the next
  weekday session closes (Friday's POC runs through the weekend until Monday 16:00 NY).
  LONG : price first trades >= POC_AWAY_PCT (2%, POC_MAJORS = top 10 coins, plus tokenised stocks/ETFs and gold/silver)
         or POC_AWAY_OTHER_PCT (5%, all other coins) above the POC; the first touch of the POC after that = buy at market.
  SHORT: mirror (2% below, then the first touch from below).
  Stop: 0.25% (POC_MIN_STOP_PCT) until the 15m touch candle closes, then just beyond that candle's full low/high
  (never closer than 0.25%, never beyond the 1.5% margin stop). Prices polled every POC_POLL_SEC (5) seconds.
  Entry: a post-only LIMIT order at the POC, placed when an armed coin comes within POC_NEAR_PCT (0.15%) of it;
  every POC_ORDER_TIMEOUT_SEC (5 min) it is kept if price is still within 0.15% and no other coin is closer to its
  POC, otherwise cancelled; also cancelled if price moves POC_RETREAT_PCT (0.5%) back away. One resting order at a time.
  If the exchange rejects the post-only order because price is already at the POC, an immediate-or-cancel limit at
  the POC is sent instead (fills at the POC or better, taker fee); if nothing fills, the touch counts as missed.
  POC_SKIP_US_SESSION=1: no entries Mon-Fri 09:30-16:00 New York (levels still built from that session; resting
  orders are cancelled when it opens; arming continues). Trades only after hours and at weekends.
  Exit: stop to entry at 1R, then from TRAIL_START_R (3R) the stop trails 1R behind in whole-R steps; no target.
  A touch consumes that side: the next entry needs another 2% move away first (also after a stop-out).

24H TRADING (SESSION=24h, default; SESSION=us limits entries to the US session)
  New entries Mon-Fri 09:30-15:30 New York time; every open position is closed at 16:00 New York.
  The trading day, daily loss limit and day summary follow the New York date.

SHORT strategy
  1. At every 4-hour candle close, scan all live perpetual futures.
     If the CLOSED 2H candle has RSI(14) > 70, the symbol is "armed".
  2. For armed symbols, watch CLOSED 5-minute candles with a 5-period EMA.
     Setup candle S : green AND its low is strictly above the 5 EMA.
     Entry candle E : the next 5m candle. If it closes RED -> short at its close.
                      If it closes green -> no entry, keep watching.
  3. Stop-loss: the closer of (a) one tick above the high of S and (b) the price where the
     position has lost MARGIN_STOP_LOSS_PCT (30%) of its margin. Take-profit: fixed 1:5.
  4. Hold until stop, take-profit or the 16:00 New York session close.

LONG strategy
  1. At every 2-hour candle close, a CLOSED 2H candle with RSI(14) < 30 arms the
     symbol for longs. The setup expires at the next 2H close and is re-checked then.
  2. On CLOSED 15-minute candles: C1 must close green (record its open and low).
     C2 is the next 15m candle (green or red). If C2 close > C1 open -> long at market.
  3. Stop-loss: the closer of (a) one tick below C1's low and (b) the price where the
     position has lost MARGIN_STOP_LOSS_PCT (30%) of its margin. Take-profit: fixed 1:5.
  ENABLE_SHORT=0, ENABLE_LONG=0 or ENABLE_BREAKOUT=0 in .env switches a strategy off.

ORDER-BOOK FILTER (all strategies, OB_FILTER=1)
  Just before entering, the live order book is read. Within OB_DEPTH_PCT (0.5%) of the mid
  price, a LONG needs bids >= OB_MIN_RATIO (1.5) x asks; a SHORT needs asks >= 1.5 x bids.
  If the book can't be read, the trade is allowed and a warning is sent (at most hourly).

BREAKEVEN STOP (all strategies)
  Once a trade is BE_TRIGGER_R (1) times its original risk in profit, the stop is moved to the
  entry price (rounded one tick in your favour), so the worst case becomes roughly the fees.

BREAKOUT strategy (its own book, runs alongside the RSI strategies)
  1. At every 1-hour close, scan all live perpetual futures. If the opens and closes of the
     last CONSOL_CANDLES (4) closed hourly candles all sit within RANGE_PCT (1%) of each
     other, the symbol is armed until the next hourly close. Range high = highest open/close.
  2. For each 15-minute candle in that hour, at minute BREAKOUT_CHECK_MINUTE (12): if the
     candle is green so far AND price is above the range high -> long at market.
  3. Stop-loss: the closer of one tick below that 15m candle's low so far and the
     30%-of-margin stop. Take-profit: fixed 1:5. Long only.

Modes (set in .env)
  DRY_RUN=1 (default)  PAPER mode. No orders are sent. The bot uses live public
                       market data, simulates the trade and sends Telegram alerts.
  DRY_RUN=0 + DELTA_ENV=testnet   Real orders on Delta's DEMO account.
  DRY_RUN=0 + DELTA_ENV=prod      Real money. Also needs CONFIRM_LIVE=YES.

Commands:
  python bot.py                 run the bot
  python bot.py --selftest      check API access, data, equity, Telegram (no orders)
  python bot.py --find-chat-id  print your Telegram chat id
  python bot.py --telegram-test send a test Telegram message
"""
import argparse
import csv
import hashlib
import hmac
import json
import logging
import math
import os
import re
import sys
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import requests

import bookjump
import execution
import rsi1h

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
IST = ZoneInfo("Asia/Kolkata")
NY = ZoneInfo("America/New_York")
STOCK_TOKEN_RE = re.compile(r"bStocks? Token|xStock Token|ONDO Token|Gold Token|Silver", re.I)   # + gold/silver tokens
PERIOD = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "2h": 7200, "4h": 14400}
ARM_TF = "2h"            # RSI arming timeframe (was 4h); candles close at even UTC hours
ARM = ARM_TF.upper()
BOOKS = (f"{ARM} RSI SHORT", f"{ARM} RSI LONG", "1H BREAKOUT", "1H BREAKDOWN", "POC RETURN") + rsi1h.BOOKS
RR = 5.0  # fixed reward-to-risk (1:5). Not configurable on purpose.
log = logging.getLogger("bot")


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
def load_env(path):
    if not os.path.exists(path):
        return
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ[k.strip()] = (v.strip().strip('"').strip("'"))


def make_cfg():
    e = os.environ

    def s(k, d=""):
        return str(e.get(k, d)).strip()

    def f(k, d):
        try:
            return float(e.get(k, d))
        except ValueError:
            return float(d)

    def i(k, d):
        return int(f(k, d))

    def b(k, d):
        return str(e.get(k, d)).strip().lower() in ("1", "true", "yes", "y", "on")

    def lst(k):
        return [x.strip().upper() for x in s(k).split(",") if x.strip()]

    exchange = s("EXCHANGE", "delta").lower()               # delta | binance
    if exchange == "binance":
        env = s("BINANCE_ENV", "demo").lower()                # demo (Binance futures demo/testnet) | prod
        testnet = env.startswith(("test", "demo"))
        prod_url = "https://fapi.binance.com"
        test_url = "https://demo-fapi.binance.com"
        base = s("BINANCE_BASE_URL") or (test_url if testnet else prod_url)
    else:
        env = s("DELTA_ENV", "testnet").lower()
        testnet = env.startswith(("test", "demo")) or env == "india_testnet"
        prod_url = "https://api.india.delta.exchange"
        test_url = "https://cdn-ind.testnet.deltaex.org"
        base = s("DELTA_BASE_URL") or (test_url if testnet else prod_url)
    dry = b("DRY_RUN", "1")
    cfg = {
        "exchange": exchange,
        "testnet": testnet,
        "dry_run": dry,
        "base_url": base,
        # Paper mode uses real market data (prod). Live mode uses the trading host.
        "data_base_url": prod_url if dry else base,
        "api_key": s("BINANCE_API_KEY") if exchange == "binance" else s("DELTA_API_KEY"),
        "api_secret": s("BINANCE_API_SECRET") if exchange == "binance" else s("DELTA_API_SECRET"),
        "tg_token": s("TELEGRAM_BOT_TOKEN"),
        "tg_chat": s("TELEGRAM_CHAT_ID"),
        "confirm_live": s("CONFIRM_LIVE"),
        "usd_inr": f("USD_INR_RATE", 85),
        "start_capital_inr": f("START_CAPITAL_INR", 50000),
        "capital_usd_override": f("TRADING_CAPITAL_USD", 0),
        "risk_pct": f("RISK_PCT", 1.0),
        "leverage": f("LEVERAGE", e.get("MAX_LEVERAGE", 20)),
        # Share of account equity put up as margin per trade. 0 = size by RISK_PCT instead.
        "margin_pct": f("MARGIN_PCT_OF_EQUITY", 100),
        # Stop when the trade has lost this % of its margin (0 = off). The closer of this and the candle stop is used.
        "margin_stop_pct": f("MARGIN_STOP_LOSS_PCT", 30),
        # Move the stop to entry once profit reaches this many R (0 = off).
        "be_trigger_r": f("BE_TRIGGER_R", 1),
        # Order-book filter: side you trade must outweigh the other within +/- OB_DEPTH_PCT of mid.
        "ob_filter": b("OB_FILTER", "1"),
        "ob_depth_pct": f("OB_DEPTH_PCT", 0.5),
        "ob_min_ratio": f("OB_MIN_RATIO", 1.5),
        # Limits: 0 = off. Only the daily loss limit is on by default (50% of the day's starting equity).
        "max_trades_day": i("MAX_TRADES_PER_DAY", 0),
        "max_trades_month": i("MAX_TRADES_PER_MONTH", 0),
        "daily_loss_pct": f("DAILY_LOSS_LIMIT_PCT", 50),
        "monthly_loss_pct": f("MONTHLY_LOSS_LIMIT_PCT", 0),
        "min_stop_pct": f("MIN_STOP_PCT", 0.25),
        "max_stop_pct": f("MAX_STOP_PCT", 3.0),
        "stop_buffer_ticks": i("STOP_BUFFER_TICKS", 1),
        "stop_trigger": s("STOP_TRIGGER_METHOD", "mark_price"),
        "rsi_len": i("RSI_LENGTH", 14),
        "rsi_level": f("RSI_LEVEL", 70),
        "rsi_long_level": f("RSI_LONG_LEVEL", 30),
        "enable_short": b("ENABLE_SHORT", "1"),
        "enable_long": b("ENABLE_LONG", "1"),
        "enable_breakout": b("ENABLE_BREAKOUT", "1"),
        # short mirror of the 1H breakout (own book); defaults to the breakout switch, so bot 2 (breakout off) stays POC-only
        "enable_breakdown": b("ENABLE_BREAKDOWN", os.getenv("ENABLE_BREAKOUT", "1")),
        "enable_poc": b("ENABLE_POC", "0"),
        # POC_ENABLED=0 pauses the POC RETURN book: no new levels, orders or trades; its balance, history and any
        # open trade are kept (an open trade is still managed to its exit). Unset / 1 = running.
        "poc_paused": os.getenv("POC_ENABLED", "1").strip() == "0",
        # BOOK JUMP / BJ ABSORB (bookjump.py): retired 23 Nov 2026, kept behind BJ_ENABLED (ENABLE_BOOKJUMP is ignored)
        "enable_bookjump": b("BJ_ENABLED", "0"),
        # 1H RSI LONG / 1H RSI SHORT (rsi1h.py): paper mode on Binance only; settings RSI1H_* read by rsi1h.load_cfg
        "enable_rsi1h": b("RSI1H_ENABLED", "1"),
        "poc_away_pct": f("POC_AWAY_PCT", 2.0),            # top coins (POC_MAJORS)
        "poc_away_other_pct": f("POC_AWAY_OTHER_PCT", 5.0),  # every other coin (stock/ETF tokens use 2%)
        "poc_majors": lst("POC_MAJORS") or [x + ("USDT" if exchange == "binance" else "USD") for x in
                                             ("BTC", "ETH", "SOL", "XRP", "BNB", "DOGE", "ADA", "LTC", "AVAX", "LINK")],
        "poc_valid_h": f("POC_VALID_HOURS", 24),
        "poc_rows": i("POC_ROWS", 50),
        "poc_poll": i("POC_POLL_SEC", 5),
        # POC levels still come from the US session, but no POC entries during it (Mon-Fri 09:30-16:00 New York):
        # trades only happen after the close, overnight and at weekends
        "poc_skip_us_session": b("POC_SKIP_US_SESSION", "0"),
        "poc_min_stop_pct": f("POC_MIN_STOP_PCT", 0.25),
        # entry: a post-only limit order AT the POC, placed when an armed coin comes within POC_NEAR_PCT of it;
        # cancelled after POC_ORDER_TIMEOUT_SEC or if price moves POC_RETREAT_PCT back away
        "poc_near_pct": f("POC_NEAR_PCT", 0.15),
        "poc_retreat_pct": f("POC_RETREAT_PCT", 0.5),
        "poc_order_timeout": i("POC_ORDER_TIMEOUT_SEC", 300),
        "trail_start_r": f("TRAIL_START_R", 3),
        # US session: entries from SESSION_OPEN to ENTRY_CUTOFF New York time (Mon-Fri), all closed at SESSION_CLOSE.
        "session": s("SESSION", "24h").lower(),
        "session_open": s("SESSION_OPEN", "09:30"),
        "entry_cutoff": s("ENTRY_CUTOFF", "15:30"),
        "session_close": s("SESSION_CLOSE", "16:00"),
        "consol_candles": i("CONSOL_CANDLES", 4),
        "range_pct": f("RANGE_PCT", 1.0),
        # frozen instruments (price barely moving, e.g. untraded coins on testnet) are skipped by every strategy
        "min_move_pct": f("MIN_MOVE_PCT", 0.1),
        "check_minute": min(max(i("BREAKOUT_CHECK_MINUTE", 12), 1), 14),
        "ema_len": i("EMA_LENGTH", 5),
        "fee_pct_per_side": f("FEE_PCT_PER_SIDE", 0.05),
        "contract_types": lst("CONTRACT_TYPES") or ["perpetual_futures"],
        "allow": lst("SYMBOL_ALLOW"),
        "exclude": lst("SYMBOL_EXCLUDE"),
        "min_turnover_usd": f("MIN_TURNOVER_USD", 0),
        "workers": i("WORKERS", 6),
        "manage_interval": i("MANAGE_INTERVAL_SEC", 15),
        # entry execution (25 Nov 2026; execution.py), every book except POC RETURN (a resting limit at its level):
        # capped IOC limit children instead of a market order (paper: simulated on the live /fapi/v1/depth instead of
        # filling at the last price); size capped at LIQ_CAP_PCT_OF_DEPTH of the top-5 levels on our side.
        "entry_slip_cap_pct": f("ENTRY_SLIP_CAP_PCT", 0.001),
        "liq_cap_pct": f("LIQ_CAP_PCT_OF_DEPTH", 0.30),   # 0.20 until 26 Nov 2026
        "entry_max_children": i("ENTRY_MAX_CHILDREN", 20),
        # limit exits (26 Nov 2026, 1H RSI books, paper): a TRAIL_STOP whose market exit would slip more than
        # EXIT_SLIP_CAP_PCT on the live depth exits with limit children down to the trail stop - that %; the rest
        # waits there and goes at market once price is back to entry + EXIT_GUARD_R x R (execution.py)
        "exit_slip_cap_pct": f("EXIT_SLIP_CAP_PCT", 0.001),
        "exit_guard_r": f("EXIT_GUARD_R", 0.5),
        "exit_max_children": i("EXIT_MAX_CHILDREN", 20),
        "data_dir": BASE_DIR,
        "ua": "delta-rsi-ema-bot/1.0",
    }
    cfg["mode"] = "PAPER" if dry else ("LIVE-TESTNET" if testnet else "LIVE")
    cfg["exchange_name"] = "Binance" if exchange == "binance" else "Delta"
    return cfg


# --------------------------------------------------------------------------
# Pure helpers (unit-tested)
# --------------------------------------------------------------------------
def usd(ex):
    """execution's *_money fields -> the trades.csv *_usd columns (rounded); returns ex."""
    for k in ("exit_slip", "exit_trigger_slip"):
        if k + "_money" in ex:
            v = ex.pop(k + "_money")
            ex[k + "_usd"] = round(v, 4) if v is not None else ""
    return ex


def fnum(x, d=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return d


def round_tick(price, tick, mode="nearest"):
    if not tick or tick <= 0:
        return price
    q = price / tick
    if mode == "up":
        q = math.ceil(q - 1e-9)
    elif mode == "down":
        q = math.floor(q + 1e-9)
    else:
        q = round(q)
    return q * tick


def fmt_price(price, tick):
    s = f"{tick:.10f}".rstrip("0")
    dec = len(s.split(".")[1]) if "." in s else 0
    return f"{price:.{dec}f}"


def rsi_wilder(closes, n=14):
    """Wilder RSI (same smoothing as TradingView's ta.rsi)."""
    if len(closes) < n + 1:
        return None
    gains = losses = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    avg_g, avg_l = gains / n, losses / n
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        avg_g = (avg_g * (n - 1) + max(d, 0.0)) / n
        avg_l = (avg_l * (n - 1) + max(-d, 0.0)) / n
    if avg_l == 0:
        return 100.0 if avg_g > 0 else 50.0
    return 100.0 - 100.0 / (1.0 + avg_g / avg_l)


def ema_series(values, n):
    k = 2.0 / (n + 1)
    out, e = [], None
    for v in values:
        e = v if e is None else v * k + e * (1 - k)
        out.append(e)
    return out


def closed_only(candles, period, now):
    return [c for c in candles if c["time"] + period <= now]


def detect_setup(c5, ema_len=5):
    """c5: ascending list of CLOSED 5m candles. S = c5[-2] (setup), E = c5[-1] (entry candle)."""
    if len(c5) < ema_len + 20:
        return None
    ema = ema_series([c["close"] for c in c5], ema_len)
    S, E = c5[-2], c5[-1]
    out = {
        "S": S,
        "E": E,
        "ema_S": ema[-2],
        "s_green": S["close"] > S["open"],
        "s_gap": S["low"] > ema[-2],
        "e_red": E["close"] < E["open"],
    }
    out["setup"] = out["s_green"] and out["s_gap"]
    out["signal"] = out["setup"] and out["e_red"]
    return out


def detect_long_setup(c15):
    """c15: ascending list of CLOSED 15m candles. C1 = c15[-2] (must be green), C2 = c15[-1].
    Signal when C2 closes strictly above C1's open."""
    if len(c15) < 2:
        return None
    C1, C2 = c15[-2], c15[-1]
    out = {"C1": C1, "C2": C2, "c1_green": C1["close"] > C1["open"],
           "c2_above": C2["close"] > C1["open"]}
    out["signal"] = out["c1_green"] and out["c2_above"]
    return out


def frozen(cands, min_pct):
    """True when the candles' whole high-low range is below min_pct% of price (no real trading)."""
    hi, lo = max(x["high"] for x in cands), min(x["low"] for x in cands)
    return lo <= 0 or (hi - lo) / lo * 100 < min_pct


def detect_consolidation(c1h, range_pct):
    """c1h: the last N CLOSED hourly candles. All opens/closes must sit within range_pct."""
    bodies = [x for cd in c1h for x in (cd["open"], cd["close"])]
    hi, lo = max(bodies), min(bodies)
    if lo <= 0:
        return None
    band = (hi - lo) / lo * 100
    if band > range_pct:
        return None
    return {"range_high": hi, "range_low": lo, "band_pct": band}


def detect_breakout(m1, b15, check_minute, range_high, range_low=None):
    """m1: CLOSED 1-minute candles. Looks at the first check_minute minutes of the 15m candle
    that opened at b15. Returns None if the last minute is not available yet."""
    end = b15 + check_minute * 60
    win = [x for x in m1 if b15 <= x["time"] < end]
    if not win or win[-1]["time"] != end - 60:
        return None
    if win[0]["time"] != b15:
        return {"complete": False}
    o, cl = win[0]["open"], win[-1]["close"]
    out = {"complete": True, "open": o, "close": cl,
           "low": min(x["low"] for x in win), "high": max(x["high"] for x in win),
           "green": cl > o, "above": cl > range_high,
           "red": cl < o, "below": range_low is not None and cl < range_low}
    out["signal"] = out["green"] and out["above"]
    out["signal_short"] = out["red"] and out["below"]          # breakdown
    return out


def tside(t):
    return t.get("side", "short")


def exit_side(t):
    """Order side that closes the trade."""
    return "buy" if tside(t) == "short" else "sell"


def ob_imbalance(bids, asks, depth_pct):
    """bids/asks: lists of (price, size). Sums size within +/- depth_pct of the mid price."""
    if not bids or not asks:
        return None
    best_bid, best_ask = max(p for p, _ in bids), min(p for p, _ in asks)
    if best_bid <= 0 or best_ask <= 0 or best_ask < best_bid:
        return None
    mid = (best_bid + best_ask) / 2
    lo, hi = mid * (1 - depth_pct / 100.0), mid * (1 + depth_pct / 100.0)
    bq = sum(q for p, q in bids if p >= lo)
    aq = sum(q for p, q in asks if p <= hi)
    return {"mid": mid, "bid_qty": bq, "ask_qty": aq,
            "spread_pct": (best_ask - best_bid) / mid * 100}


def ob_ratio(ob, side):
    """How many times the side you trade outweighs the other (bids/asks for long)."""
    num, den = (ob["bid_qty"], ob["ask_qty"]) if side == "long" else (ob["ask_qty"], ob["bid_qty"])
    if den <= 0:
        return float("inf") if num > 0 else 0.0
    return num / den


def be_stop_price(t):
    """Breakeven stop: the entry price, rounded to the tick on the side that cannot lose."""
    return round_tick(t["entry"], t["tick"], "down" if tside(t) == "short" else "up")


def risk0(t):
    """Original risk per unit, before any stop move."""
    return t.get("risk0") or abs(t.get("orig_stop", t["stop"]) - t["entry"])


def compute_size(equity_usd, risk_pct, entry, stop, contract_value, max_leverage):
    """Linear contract: loss per contract at stop = contract_value * (stop - entry)."""
    info = {}
    risk_per_contract = contract_value * abs(stop - entry)
    if risk_per_contract <= 0 or entry <= 0 or contract_value <= 0:
        return 0, {"reason": "invalid stop/entry/contract value"}
    risk_budget = equity_usd * risk_pct / 100.0
    by_risk = math.floor(risk_budget / risk_per_contract)
    notional_per_contract = contract_value * entry
    by_lev = math.floor(equity_usd * max_leverage / notional_per_contract)
    n = int(max(0, min(by_risk, by_lev)))
    info.update(
        risk_budget=risk_budget,
        by_risk=by_risk,
        by_leverage=by_lev,
        binding="leverage cap" if by_lev < by_risk else "risk budget",
        notional=n * notional_per_contract,
        risk_usd=n * risk_per_contract,
    )
    return n, info


def size_by_margin(equity_usd, margin_pct, leverage, entry, stop, contract_value, buffer=0.95):
    """Use margin_pct of equity as margin (5% buffer kept for fees); notional = margin x leverage."""
    if entry <= 0 or contract_value <= 0 or stop <= 0 or stop == entry or leverage <= 0:
        return 0, {"reason": "invalid inputs"}
    margin = equity_usd * margin_pct / 100.0 * buffer
    n = int(max(0, math.floor(margin * leverage / (contract_value * entry))))
    return n, {
        "binding": "margin share",
        "margin": n * contract_value * entry / leverage,
        "notional": n * contract_value * entry,
        "risk_usd": n * contract_value * abs(stop - entry),
    }


def size_trade(cfg, equity_usd, entry, stop, contract_value):
    if cfg["margin_pct"] > 0:
        return size_by_margin(equity_usd, cfg["margin_pct"], cfg["leverage"], entry, stop, contract_value)
    n, info = compute_size(equity_usd, cfg["risk_pct"], entry, stop, contract_value, cfg["leverage"])
    if n > 0:
        info["margin"] = info["notional"] / cfg["leverage"]
    return n, info


def period_keys(ts):
    """Trading day and month follow the New York date."""
    d = datetime.fromtimestamp(ts, NY)
    return d.strftime("%Y-%m-%d"), d.strftime("%Y-%m")


def hhmm(x):
    h, m = x.split(":")
    return int(h) * 60 + int(m)


def session_phase(cfg, ts):
    """'entry' (new trades allowed), 'hold' (manage only) or 'closed' (everything flat)."""
    if cfg["session"] != "us":
        return "entry"
    d = datetime.fromtimestamp(ts, NY)
    if d.weekday() >= 5:
        return "closed"
    m = d.hour * 60 + d.minute
    if hhmm(cfg["session_open"]) <= m < hhmm(cfg["entry_cutoff"]):
        return "entry"
    if hhmm(cfg["entry_cutoff"]) <= m < hhmm(cfg["session_close"]):
        return "hold"
    return "closed"


def book_for(side, strategy):
    if strategy == "poc":
        return "POC RETURN"
    if strategy == "rsi1h":
        return f"1H RSI {side.upper()}"
    if strategy == "breakdown":
        return "1H BREAKDOWN"
    return "1H BREAKOUT" if strategy == "breakout" else f"{ARM} RSI {side.upper()}"


def poc_price(cands, rows=50):
    """Volume profile POC of candles (each candle's volume spread evenly over its high-low range)."""
    lo, hi = min(x["low"] for x in cands), max(x["high"] for x in cands)
    if hi <= lo:
        return None
    width = (hi - lo) / rows
    vol = [0.0] * rows
    for x in cands:
        v, h, l = x.get("volume", 0.0), x["high"], x["low"]
        if not v or v <= 0:
            continue
        if h <= l:
            vol[min(rows - 1, int((h - lo) / width))] += v
            continue
        for r in range(rows):
            a, b = lo + r * width, lo + (r + 1) * width
            ov = min(b, h) - max(a, l)
            if ov > 0:
                vol[r] += v * ov / (h - l)
    m = max(vol)
    if m <= 0:
        return None
    top = [r for r in range(rows) if vol[r] >= m * (1 - 1e-9)]
    r = top[len(top) // 2]                       # flat tie (one wide candle): take the middle row
    return lo + (r + 0.5) * width


def ist(ts):
    return datetime.fromtimestamp(ts, IST).strftime("%Y-%m-%d %H:%M IST")


def human_dur(sec):
    sec = int(max(sec, 0))
    h, r = divmod(sec, 3600)
    m = r // 60
    return f"{h}h {m}m" if h else f"{m}m"


# --------------------------------------------------------------------------
# Delta REST client
# --------------------------------------------------------------------------
class DeltaError(Exception):
    pass


class Delta:
    def __init__(self, base, key, secret, ua):
        self.base, self.key, self.secret, self.ua = base.rstrip("/"), key, secret, ua

    def request(self, method, path, params=None, body=None, auth=False, retries=3):
        query = ("?" + urlencode(params, safe=",")) if params else ""
        payload = json.dumps(body) if body is not None else ""
        last = None
        for attempt in range(retries):
            headers = {"User-Agent": self.ua, "Accept": "application/json"}
            if body is not None:
                headers["Content-Type"] = "application/json"
            if auth:
                ts = str(int(time.time()))
                msg = method + ts + path + query + payload
                sig = hmac.new(self.secret.encode(), msg.encode(), hashlib.sha256).hexdigest()
                headers.update({"api-key": self.key, "timestamp": ts, "signature": sig})
            try:
                r = requests.request(
                    method, self.base + path + query,
                    data=payload if body is not None else None,
                    headers=headers, timeout=15,
                )
            except requests.RequestException as ex:
                last = f"network error: {ex.__class__.__name__}"
                time.sleep(1 + attempt)
                continue
            if r.status_code == 429:
                wait = fnum(r.headers.get("X-RATE-LIMIT-RESET"), 1000) / 1000.0
                time.sleep(min(max(wait, 0.5), 10))
                last = "rate limited (429)"
                continue
            try:
                data = r.json()
            except ValueError:
                last = f"HTTP {r.status_code}: non-JSON response"
                if r.status_code >= 500:
                    time.sleep(1 + attempt)
                    continue
                raise DeltaError(last)
            if isinstance(data, dict) and data.get("success") is False:
                raise DeltaError(f"{method} {path}: {json.dumps(data.get('error', data))[:400]}")
            if r.status_code >= 500:
                last = f"HTTP {r.status_code}"
                time.sleep(1 + attempt)
                continue
            return data
        raise DeltaError(f"{method} {path} failed: {last}")

    # ---- public data ----
    def products(self, contract_types):
        out, after = [], None
        for _ in range(30):
            params = {"contract_types": ",".join(contract_types), "page_size": 200}
            if after:
                params["after"] = after
            d = self.request("GET", "/v2/products", params=params)
            out += d.get("result") or []
            after = (d.get("meta") or {}).get("after")
            if not after:
                break
        return out

    def tickers(self, contract_types):
        d = self.request("GET", "/v2/tickers", params={"contract_types": ",".join(contract_types)})
        return d.get("result") or []

    def ticker(self, symbol):
        d = self.request("GET", f"/v2/tickers/{symbol}")
        return d.get("result") or {}

    def orderbook(self, symbol, depth=100):
        d = self.request("GET", f"/v2/l2orderbook/{symbol}", params={"depth": depth})
        r = d.get("result") or {}

        def norm(levels):
            out = []
            for x in levels or []:
                if isinstance(x, dict):
                    p, q = fnum(x.get("price")), fnum(x.get("size"))
                elif isinstance(x, (list, tuple)) and len(x) >= 2:
                    p, q = fnum(x[0]), fnum(x[1])
                else:
                    continue
                if p > 0 and q > 0:
                    out.append((p, q))
            return out
        return norm(r.get("buy") or r.get("bids")), norm(r.get("sell") or r.get("asks"))

    def candles(self, symbol, res, count, now=None):
        now = int(now or time.time())
        params = {"resolution": res, "symbol": symbol,
                  "start": now - count * PERIOD[res], "end": now}
        d = self.request("GET", "/v2/history/candles", params=params)
        out = []
        for c in d.get("result") or []:
            try:
                out.append({"time": int(c["time"]), "open": float(c["open"]),
                            "high": float(c["high"]), "low": float(c["low"]),
                            "close": float(c["close"]), "volume": float(c.get("volume") or 0)})
            except (KeyError, TypeError, ValueError):
                continue
        out.sort(key=lambda x: x["time"])
        return out

    # ---- private ----
    def balances(self):
        return self.request("GET", "/v2/wallet/balances", auth=True).get("result") or []

    def position(self, product_id):
        d = self.request("GET", "/v2/positions", params={"product_id": product_id}, auth=True)
        r = d.get("result")
        if isinstance(r, list):
            r = next((x for x in r if int(fnum(x.get("product_id"), -1)) == int(product_id)), None)
        if not r:
            return {"size": 0, "entry_price": 0.0}
        return {"size": int(fnum(r.get("size"))), "entry_price": fnum(r.get("entry_price"))}

    def all_positions(self):
        d = self.request("GET", "/v2/positions/margined", auth=True)
        return d.get("result") or []

    def place_order(self, body):
        return self.request("POST", "/v2/orders", body=body, auth=True).get("result") or {}

    def bracket(self, product_id, symbol, sl, tp, trigger):
        body = {
            "product_id": product_id,
            "product_symbol": symbol,
            "stop_loss_order": {"order_type": "market_order", "stop_price": sl},
            "take_profit_order": {"order_type": "limit_order", "stop_price": tp, "limit_price": tp},
            "bracket_stop_trigger_method": trigger,
        }
        return self.request("POST", "/v2/orders/bracket", body=body, auth=True)

    def open_orders(self, product_id):
        d = self.request("GET", "/v2/orders",
                         params={"product_ids": product_id, "states": "open,pending"}, auth=True)
        out = []
        for o in d.get("result") or []:
            pid = o.get("product_id")
            if pid is None or int(fnum(pid, -1)) == int(product_id):
                out.append(o)
        return out

    def order_history(self, product_id):
        d = self.request("GET", "/v2/orders/history",
                         params={"product_ids": product_id, "page_size": 30}, auth=True)
        return d.get("result") or []

    def cancel_order(self, product_id, order_id):
        return self.request("DELETE", "/v2/orders", body={"id": int(order_id), "product_id": product_id}, auth=True)

    def cancel_all(self, product_id):
        return self.request("DELETE", "/v2/orders/all", body={"product_id": product_id}, auth=True)

    def set_leverage(self, product_id, leverage):
        return self.request("POST", f"/v2/products/{product_id}/orders/leverage",
                            body={"leverage": str(leverage)}, auth=True)


class Binance:
    """Binance USDT-M perpetual futures behind the same interface the bot uses for Delta.
    - Each symbol gets a stable integer product id (crc32 of the symbol).
    - contract_value = the symbol's quantity step, so the bot's whole "contracts" x step = Binance quantity.
    - Stop orders go through the Algo Order API (Binance moved conditional orders there on 2025-12-09);
      their ids are returned as "a<algoId>" so cancel_order() knows which endpoint to use.
    - A rejected post-only order (-5022) is reported as "immediate_execution_post_only", like Delta.
    One-way position mode is assumed (Binance's default)."""

    def __init__(self, base, key, secret, ua):
        self.base, self.key, self.secret, self.ua = base.rstrip("/"), key, secret, ua
        self.meta = {}                # product id -> {"symbol", "step", "tick"}
        self.offset_ms = 0
        self._vol, self._vol_ts = {}, 0

    def request(self, method, path, params=None, signed=False, retries=3):
        last = None
        for attempt in range(retries):
            p = dict(params or {})
            headers = {"User-Agent": self.ua}
            if self.key:
                headers["X-MBX-APIKEY"] = self.key
            q = urlencode(p)
            if signed:
                p["timestamp"] = int(time.time() * 1000) + self.offset_ms
                p["recvWindow"] = 10000
                q = urlencode(p)
                q += "&signature=" + hmac.new(self.secret.encode(), q.encode(), hashlib.sha256).hexdigest()
            try:
                r = requests.request(method, self.base + path + (("?" + q) if q else ""), headers=headers, timeout=15)
            except requests.RequestException as ex:
                last = f"network error: {ex.__class__.__name__}"
                time.sleep(1 + attempt)
                continue
            if r.status_code in (418, 429):
                last = f"rate limited ({r.status_code})"
                time.sleep(min(fnum(r.headers.get("Retry-After"), 5), 30))
                continue
            used = fnum(r.headers.get("X-MBX-USED-WEIGHT-1M"), 0)
            if used >= 2000:                       # limit 2400/min per IP: wait for the next minute window
                log.warning("Binance request weight %d/2400 this minute: pausing until the next minute", used)
                time.sleep(61 - time.time() % 60)
            try:
                data = r.json()
            except ValueError:
                last = f"HTTP {r.status_code}: non-JSON response"
                if r.status_code >= 500:
                    time.sleep(1 + attempt)
                    continue
                raise DeltaError(last)
            if isinstance(data, dict) and isinstance(data.get("code"), int) and data["code"] < 0:
                code = data["code"]
                if code == -1021 and attempt < retries - 1:          # timestamp out of sync: resync and retry
                    try:
                        st = requests.get(self.base + "/fapi/v1/time", timeout=10).json()["serverTime"]
                        self.offset_ms = int(st) - int(time.time() * 1000)
                    except Exception:  # noqa: BLE001
                        pass
                    continue
                tag = " immediate_execution_post_only" if code == -5022 else ""
                raise DeltaError(f"{method} {path}: {code} {data.get('msg', '')}{tag}"[:400])
            if r.status_code >= 500:
                last = f"HTTP {r.status_code}"
                time.sleep(1 + attempt)
                continue
            return data
        raise DeltaError(f"{method} {path} failed: {last}")

    @staticmethod
    def pid(symbol):
        return zlib.crc32(symbol.encode()) & 0x7FFFFFFF

    def _m(self, product_id):
        if int(product_id) not in self.meta:
            self.products(None)
        return self.meta[int(product_id)]

    @staticmethod
    def _fmt(x, step):
        dec = max(0, -int(math.floor(math.log10(step)))) if step < 1 else 0
        return f"{x:.{dec}f}"

    # ---- public data ----
    def products(self, contract_types):
        info = self.request("GET", "/fapi/v1/exchangeInfo")
        out = []
        for s_ in info.get("symbols") or []:
            if s_.get("contractType") != "PERPETUAL" or s_.get("quoteAsset") != "USDT":
                continue
            f_ = {x["filterType"]: x for x in s_.get("filters") or []}
            tick = fnum(f_.get("PRICE_FILTER", {}).get("tickSize"))
            step = fnum(f_.get("LOT_SIZE", {}).get("stepSize"))
            sym = s_["symbol"]
            pid = self.pid(sym)
            self.meta[pid] = {"symbol": sym, "step": step, "tick": tick,
                              "min_notional": fnum(f_.get("MIN_NOTIONAL", {}).get("notional"), 5)}
            utype = s_.get("underlyingType") or "COIN"
            out.append({"id": pid, "symbol": sym, "state": "live" if s_.get("status") == "TRADING" else "closed",
                        "tick_size": tick, "contract_value": step, "contract_type": "perpetual_futures",
                        "description": f"{utype} {','.join(s_.get('underlyingSubType') or [])}",
                        "non_crypto": utype != "COIN"})
        return out

    def tickers(self, contract_types):
        mk = self.request("GET", "/fapi/v1/premiumIndex")
        if time.time() - self._vol_ts > 600:                     # 24h volume is heavy (weight 40): every 10 min
            try:
                self._vol = {t["symbol"]: fnum(t.get("quoteVolume"))
                             for t in self.request("GET", "/fapi/v1/ticker/24hr")}
                self._vol_ts = time.time()
            except DeltaError as ex:
                log.warning("24h volume: %s", ex)
        return [{"symbol": x["symbol"], "mark_price": fnum(x.get("markPrice")),
                 "turnover_usd": self._vol.get(x["symbol"], -1)} for x in mk]

    def ticker(self, symbol):
        x = self.request("GET", "/fapi/v1/premiumIndex", params={"symbol": symbol})
        return {"mark_price": fnum(x.get("markPrice"))}

    def orderbook(self, symbol, depth=100):
        limit = depth if depth in (5, 10, 20, 50, 100, 500, 1000) else 100     # weight: <=50 -> 2, 100 -> 5
        d = self.request("GET", "/fapi/v1/depth", params={"symbol": symbol, "limit": limit})
        return ([(fnum(p), fnum(q)) for p, q in d.get("bids") or []],
                [(fnum(p), fnum(q)) for p, q in d.get("asks") or []])

    def candles(self, symbol, res, count, now=None):
        now = int(now or time.time())
        d = self.request("GET", "/fapi/v1/klines", params={"symbol": symbol, "interval": res,
                                                          "endTime": now * 1000, "limit": min(int(count) + 1, 1500)})
        out = []
        for c in d or []:
            out.append({"time": int(c[0]) // 1000, "open": float(c[1]), "high": float(c[2]), "low": float(c[3]),
                        "close": float(c[4]), "volume": float(c[5])})
        return [c for c in out if c["time"] <= now]

    # ---- private ----
    def balances(self):
        return [{"asset_symbol": b["asset"], "balance": b.get("balance"), "available_balance": b.get("availableBalance")}
                for b in self.request("GET", "/fapi/v2/balance", signed=True) if fnum(b.get("balance"))]

    def position(self, product_id):
        m = self._m(product_id)
        rows = self.request("GET", "/fapi/v2/positionRisk", params={"symbol": m["symbol"]}, signed=True)
        amt = sum(fnum(r.get("positionAmt")) for r in rows or [])
        ep = next((fnum(r.get("entryPrice")) for r in rows or [] if fnum(r.get("positionAmt"))), 0.0)
        return {"size": int(round(amt / m["step"])), "entry_price": ep}

    def all_positions(self):
        out = []
        for r in self.request("GET", "/fapi/v2/positionRisk", signed=True) or []:
            amt = fnum(r.get("positionAmt"))
            if amt:
                pid = self.pid(r["symbol"])
                step = self.meta.get(pid, {}).get("step") or 1
                out.append({"product_id": pid, "product_symbol": r["symbol"], "size": int(round(amt / step))})
        return out

    def place_order(self, body):
        m = self._m(body["product_id"])
        p = {"symbol": m["symbol"], "side": str(body["side"]).upper(),
             "quantity": self._fmt(int(body["size"]) * m["step"], m["step"])}
        if body.get("reduce_only"):
            p["reduceOnly"] = "true"
        if body.get("stop_order_type"):                          # conditional (stop) order -> Algo Order API
            p.update(algoType="CONDITIONAL",
                     type="TAKE_PROFIT_MARKET" if body["stop_order_type"] == "take_profit_order" else "STOP_MARKET",
                     triggerPrice=body["stop_price"],
                     workingType="MARK_PRICE" if body.get("stop_trigger_method", "mark_price") == "mark_price"
                     else "CONTRACT_PRICE")
            r = self.request("POST", "/fapi/v1/algoOrder", params=p, signed=True)
            return {"id": f"a{r.get('algoId')}", "state": str(r.get("algoStatus", "")).lower()}
        if body.get("order_type") == "limit_order":
            tif = "GTX" if body.get("post_only") else "IOC" if body.get("time_in_force") == "ioc" else "GTC"
            p.update(type="LIMIT", price=body["limit_price"], timeInForce=tif)
            if tif == "IOC":
                p["newOrderRespType"] = "RESULT"                 # the reply then carries executedQty / avgPrice
        else:
            p["type"] = "MARKET"
        if body.get("client_order_id"):
            p["newClientOrderId"] = re.sub(r"[^A-Za-z0-9_.:/-]", "", str(body["client_order_id"]))[:36]
        r = self.request("POST", "/fapi/v1/order", params=p, signed=True)
        return {"id": str(r.get("orderId")), "state": str(r.get("status", "")).lower(),
                "average_fill_price": fnum(r.get("avgPrice")), "filled_qty": fnum(r.get("executedQty"))}

    def bracket(self, product_id, symbol, sl, tp, trigger):
        raise DeltaError("bracket orders are not used on Binance; protection is placed as separate orders")

    def open_orders(self, product_id):
        sym = self._m(product_id)["symbol"]
        out = [{"id": str(o["orderId"]), "side": o["side"].lower(), "limit_price": fnum(o.get("price")),
                "stop_price": fnum(o.get("stopPrice")), "order_type": o.get("type", "").lower(),
                "state": o.get("status", "").lower()}
               for o in self.request("GET", "/fapi/v1/openOrders", params={"symbol": sym}, signed=True) or []]
        for o in self.request("GET", "/fapi/v1/openAlgoOrders", params={"symbol": sym}, signed=True) or []:
            out.append({"id": f"a{o['algoId']}", "side": o["side"].lower(), "limit_price": fnum(o.get("price")),
                        "stop_price": fnum(o.get("triggerPrice")), "order_type": str(o.get("orderType", "")).lower(),
                        "state": str(o.get("algoStatus", "")).lower()})
        return out

    def order_history(self, product_id):
        sym = self._m(product_id)["symbol"]
        rows = self.request("GET", "/fapi/v1/allOrders", params={"symbol": sym, "limit": 30}, signed=True) or []
        return [{"id": int(o["orderId"]), "side": o["side"].lower(), "average_fill_price": fnum(o.get("avgPrice")),
                 "state": "filled" if o.get("status") == "FILLED" else str(o.get("status", "")).lower()} for o in rows]

    def cancel_order(self, product_id, order_id):
        sym = self._m(product_id)["symbol"]
        oid = str(order_id)
        if oid.startswith("a"):
            return self.request("DELETE", "/fapi/v1/algoOrder", params={"algoId": oid[1:]}, signed=True)
        return self.request("DELETE", "/fapi/v1/order", params={"symbol": sym, "orderId": oid}, signed=True)

    def cancel_all(self, product_id):
        sym = self._m(product_id)["symbol"]
        self.request("DELETE", "/fapi/v1/allOpenOrders", params={"symbol": sym}, signed=True)
        return self.request("DELETE", "/fapi/v1/algoOpenOrders", params={"symbol": sym}, signed=True)

    def set_leverage(self, product_id, leverage):
        sym = self._m(product_id)["symbol"]
        try:                                                     # isolated margin, like Delta's per-position margin
            self.request("POST", "/fapi/v1/marginType", params={"symbol": sym, "marginType": "ISOLATED"}, signed=True)
        except DeltaError as ex:
            if "-4046" not in str(ex):                           # -4046: already isolated
                log.warning("margin type %s: %s", sym, ex)
        return self.request("POST", "/fapi/v1/leverage", params={"symbol": sym, "leverage": int(leverage)}, signed=True)


# --------------------------------------------------------------------------
# Telegram
# --------------------------------------------------------------------------
class BinanceLiveVenue:
    """Live IOC limit children for execution.run_entry (Binance). A rejected child counts as unfilled."""

    def __init__(self, api, data, t):
        self.api, self.data, self.t = api, data, t

    def book(self):
        return self.data.orderbook(self.t["symbol"], 20)

    def ioc(self, side, price, qty, book=None):
        t = self.t
        try:
            r = self.api.place_order({"product_id": t["product_id"], "size": int(round(qty / t["cv"])),
                                      "side": "buy" if side == "LONG" else "sell", "order_type": "limit_order",
                                      "limit_price": fmt_price(price, t["tick"]), "time_in_force": "ioc"})
        except Exception as ex:  # noqa: BLE001
            log.warning("IOC child %s %s @ %s: %s", t["symbol"], qty, price, ex)
            return 0.0, None
        got = fnum(r.get("filled_qty"))
        return got, (fnum(r.get("average_fill_price")) or None) if got > 0 else None


class Telegram:
    def __init__(self, token, chat_id):
        self.token, self.chat_id = token, str(chat_id or "")

    @property
    def enabled(self):
        return bool(self.token and self.chat_id)

    def send(self, text):
        log.info("TELEGRAM >> %s", text.replace("\n", " | ")[:300])
        if not self.enabled:
            return
        for i in range(0, len(text), 3900):
            try:
                requests.post(
                    f"https://api.telegram.org/bot{self.token}/sendMessage",
                    json={"chat_id": self.chat_id, "text": text[i:i + 3900],
                          "disable_web_page_preview": True},
                    timeout=10,
                )
            except requests.RequestException as ex:
                log.warning("Telegram send failed: %s", ex.__class__.__name__)

    def poll(self, state):
        """Return new command texts from the authorised chat only."""
        if not self.enabled:
            return []
        try:
            r = requests.get(
                f"https://api.telegram.org/bot{self.token}/getUpdates",
                params={"offset": state.get("tg_offset", 0), "timeout": 0}, timeout=10,
            )
            data = r.json()
        except (requests.RequestException, ValueError) as ex:
            log.warning("Telegram poll failed: %s", ex.__class__.__name__)
            return []
        out = []
        for u in data.get("result") or []:
            state["tg_offset"] = u["update_id"] + 1
            m = u.get("message") or {}
            if str((m.get("chat") or {}).get("id")) == self.chat_id and m.get("text"):
                out.append(m["text"].strip())
        return out


# --------------------------------------------------------------------------
# Bot
# --------------------------------------------------------------------------
class Bot:
    def __init__(self, cfg):
        self.cfg = cfg
        Exchange = Binance if cfg.get("exchange") == "binance" else Delta
        self.api = Exchange(cfg["base_url"], cfg["api_key"], cfg["api_secret"], cfg["ua"])
        self.data = self.api if cfg["data_base_url"] == cfg["base_url"] else \
            Exchange(cfg["data_base_url"], "", "", cfg["ua"])
        self.tg = Telegram(cfg["tg_token"], cfg["tg_chat"])
        self.state_file = os.path.join(cfg["data_dir"], "state.json")
        self.events_file = os.path.join(cfg["data_dir"], "events.jsonl")
        self.trades_file = os.path.join(cfg["data_dir"], "trades.csv")
        self.state = self.load_state()
        self.products, self.products_ts = [], 0
        self.eval_boundary, self.eval_done = 0, set()
        self.last_eval_try = 0
        self.bo_boundary, self.bo_done, self.last_bo_try = 0, set(), 0
        self.last_session_close_try = 0
        self.next_scan_try = 0
        self.last_err_notify = 0
        self.last_equity, self.last_equity_ts = 0.0, 0
        self.last_protect_check = 0
        self.last_ob_warn = 0
        self.bj = None                       # BOOK JUMP runner (bookjump.py) when BJ_ENABLED=1
        self.rsi1h, self.rsi1h_refused = None, ""   # 1H RSI books (rsi1h.py)
        if cfg.get("enable_rsi1h"):
            self.rsi1h_refused = rsi1h.refuse_reason(cfg)
            if self.rsi1h_refused:
                cfg["enable_rsi1h"] = False
            else:
                self.rsi1h = rsi1h.Rsi1h(self)
        self.migrate_trades_header()

    def migrate_trades_header(self):
        """Older trades.csv files lack the ob_ratio / strategy / book_balance columns; add them to the header."""
        try:
            with open(self.trades_file) as fh:
                lines = fh.readlines()
            if lines and "book_balance_usd" not in lines[0]:
                head = lines[0].rstrip("\r\n")
                if "ob_ratio" not in head:
                    head += ",ob_ratio"
                lines[0] = head + ",strategy,book_balance_usd\n"
                with open(self.trades_file, "w") as fh:
                    fh.writelines(lines)
        except OSError:
            pass

    def check_orderbook(self, symbol, side):
        """Returns (info, skip_reason). skip_reason is "" when the trade may go ahead."""
        c = self.cfg
        try:
            bids, asks = self.data.orderbook(symbol)
            ob = ob_imbalance(bids, asks, c["ob_depth_pct"])
            if ob is None:
                raise DeltaError("empty or crossed order book")
        except Exception as ex:  # noqa: BLE001
            log.warning("order book unavailable for %s: %s", symbol, ex)
            if time.time() - self.last_ob_warn > 3600:
                self.last_ob_warn = time.time()
                self.tg.send(f"NOTE: order book unavailable for {symbol} ({str(ex)[:120]}). "
                             f"Trade allowed without the order-book check.")
            return None, ""
        ratio = ob_ratio(ob, side)
        info = {"bid_qty": round(ob["bid_qty"], 4), "ask_qty": round(ob["ask_qty"], 4),
                "ratio": round(ratio, 2) if ratio != float("inf") else 999.0,
                "spread_pct": round(ob["spread_pct"], 4)}
        if ratio < c["ob_min_ratio"]:
            heavier = "bids" if side == "long" else "asks"
            return info, (f"order book: {heavier} only {ratio:.2f}x the other side within "
                          f"{c['ob_depth_pct']:g}% (need {c['ob_min_ratio']:g}x)")
        return info, ""

    # ---- state / logs ----
    def load_state(self):
        base = {"paused": False, "armed": {}, "armed_long": {}, "armed_bo": {}, "last_1h": 0,
                "open_trade": None, "day": {}, "month": {},
                "last_4h": 0, "tg_offset": 0, "paper_pnl": 0.0, "books": None, "summary_sent": ""}
        try:
            with open(self.state_file) as fh:
                base.update(json.load(fh))
        except (OSError, ValueError):
            pass
        return base

    def save_state(self):
        tmp = self.state_file + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(self.state, fh, indent=1)
        os.replace(tmp, self.state_file)

    def event(self, typ, **kw):
        rec = {"ts": datetime.now(IST).isoformat(timespec="seconds"), "event": typ, **kw}
        with open(self.events_file, "a") as fh:
            fh.write(json.dumps(rec) + "\n")
        log.info("EVENT %s %s", typ, json.dumps(kw)[:300])

    def inr(self, usd):
        return usd * self.cfg["usd_inr"]

    # ---- the three books ----
    def init_books(self):
        """First start of the 3-book version: split the account equally. An open trade moves into its book."""
        on = self.enabled_books()
        if self.state.get("books") and len(on) == 1 and on[0] not in self.state["books"]:
            self.state["books"][on[0]] = self.new_book(0.0)    # filled by sweep_retired()
        if self.state.get("books"):
            for side in ("SHORT", "LONG"):        # arming timeframe changed: rename the books, keep balances
                old, new = f"4H RSI {side}", f"{ARM} RSI {side}"
                if old != new and old in self.state["books"] and new not in self.state["books"]:
                    self.state["books"][new] = self.state["books"].pop(old)
                    if self.state["books"][new]["open_trade"]:
                        self.state["books"][new]["open_trade"]["book"] = new
            for n in on:
                if n in self.state["books"]:
                    continue
                donors = [(b["balance"], m) for m, b in self.state["books"].items()
                          if not b["open_trade"] and m in BOOKS]           # BOOK JUMP's own book never donates
                if not donors:
                    self.state["books"][n] = self.new_book(0.0)
                    continue
                bal, donor = max(donors)
                half = round(bal / 2, 4)
                d = self.state["books"][donor]
                d["balance"], d["start"] = round(d["balance"] - half, 4), round(d["start"] - half, 4)
                self.state["books"][n] = self.new_book(half)
                self.save_state()
                self.tg.send(f"New strategy {n}: funded with ${half:.2f} = half of {donor}'s ${bal:.2f}. "
                             f"{donor} now ${d['balance']:.2f}.")
            return
        total = self.equity_usd()
        each = round(total / len(on), 2)
        self.state["books"] = {n: self.new_book(each) for n in on}
        self.state["books_start_total"] = round(total, 2)
        self.state["books_since"] = datetime.now(IST).isoformat(timespec="seconds")
        t = self.state.get("open_trade")
        if t:
            t["book"] = book_for(tside(t), t.get("strategy", "rsi"))
            if t["book"] in self.state["books"]:
                self.state["books"][t["book"]]["open_trade"] = t
            else:                                   # its strategy is off in this bot (paper leftover): drop it
                self.tg.send(f"Old {t['symbol']} {t['book']} paper trade dropped (strategy not enabled here).")
                t = None
        self.state["open_trade"] = None
        self.save_state()
        self.tg.send(f"Account split into {len(on)} strateg{'y' if len(on) == 1 else 'ies'}: ${total:.2f} -> ${each:.2f} each "
                     f"({', '.join(on)})." + (f"\nOpen {t['symbol']} trade moved to {t['book']}." if t else ""))

    @staticmethod
    def new_book(balance):
        return {"start": balance, "balance": balance, "open_trade": None, "day": {}, "month": {},
                "trades": 0, "wins": 0}

    def book(self, name):
        return self.state["books"][name]

    def open_trades(self):
        """Every open trade: each book's open_trade, plus the 1H RSI books' parallel trades (more_trades, since
        26 Nov 2026; open_trade is the oldest one, so older readers of state.json still see a position)."""
        return [(n, t) for n, b in self.state["books"].items()
                for t in ([b["open_trade"]] if b["open_trade"] else []) + list(b.get("more_trades") or [])]

    def book_trades(self, name):
        b = self.book(name)
        return ([b["open_trade"]] if b["open_trade"] else []) + list(b.get("more_trades") or [])

    def free_margin(self, name):
        """1H RSI books: the book's balance minus the margin of its open trades (what a new parallel trade may use)."""
        return self.book(name)["balance"] - sum(t.get("margin") or 0.0 for t in self.book_trades(name))

    def add_trade(self, name, t):
        b = self.book(name)
        if b["open_trade"] is None:
            b["open_trade"] = t
        else:
            b.setdefault("more_trades", []).append(t)

    def drop_trade(self, name, t):
        """Remove t from its book; the oldest parallel trade moves up into open_trade."""
        b = self.book(name)
        more = list(b.get("more_trades") or [])
        if b["open_trade"] is t or (b["open_trade"] or {}).get("opened_ts") == t.get("opened_ts") and \
                (b["open_trade"] or {}).get("symbol") == t.get("symbol"):
            b["open_trade"] = more.pop(0) if more else None
        else:
            more = [x for x in more if not (x is t or (x.get("symbol") == t.get("symbol")
                                                       and x.get("opened_ts") == t.get("opened_ts")))]
        if "more_trades" in b or more:
            b["more_trades"] = more

    def book_free(self, name):
        return self.book(name)["open_trade"] is None

    def enabled_books(self):
        c = self.cfg
        return [n for n, on in zip(BOOKS, (c["enable_short"], c["enable_long"], c["enable_breakout"], c["enable_breakdown"],
                                         c["enable_poc"], c.get("enable_rsi1h"), c.get("enable_rsi1h"))) if on]

    def paused_books(self):
        return ["POC RETURN"] if self.cfg["enable_poc"] and self.cfg["poc_paused"] else []

    def book_names(self):
        on = self.enabled_books()
        return [n for n in BOOKS if n in self.state["books"] and
                (n in on or self.book(n)["open_trade"] or abs(self.book(n)["balance"]) > 1e-9)]

    def sweep_retired(self):
        """With exactly one strategy enabled, every switched-off strategy's balance moves into it once that
        strategy has no open trade. When nothing else is left open, the enabled book is set to the real wallet."""
        on = self.enabled_books()
        if len(on) != 1:
            return
        tgt = self.book(on[0])
        moved = []
        for n, b in self.state["books"].items():
            if n == on[0] or n not in BOOKS or b["open_trade"] or abs(b["balance"]) < 1e-9:
                continue                                    # BOOK JUMP's own book is never swept
            moved.append(f"{n} ${b['balance']:.2f}")
            tgt["balance"] = round(tgt["balance"] + b["balance"], 4)
            tgt["start"] = round(tgt["start"] + b["balance"], 4)
            b["balance"], b["start"] = 0.0, 0.0
        others_open = any(b["open_trade"] for n, b in self.state["books"].items() if n != on[0] and n in BOOKS)
        if (moved or not self.state.get("swept_to_wallet")) and not others_open and not tgt["open_trade"]:
            msg = ""
            if not self.cfg["dry_run"]:
                self.last_equity_ts = 0
                w = round(self.equity_usd(), 4)
                if w > 0:
                    msg = f"\nSet to the real Delta wallet: ${w:.2f} (books said ${tgt['balance']:.2f})."
                    tgt["balance"] = tgt["start"] = w
            self.state["swept_to_wallet"] = True
            self.state["sweep_since"] = datetime.now(IST).isoformat(timespec="seconds")
            self.save_state()
            self.tg.send(f"{on[0]} now trades the whole account: ${tgt['balance']:.2f}"
                         + (f"\nMoved in: {', '.join(moved)}" if moved else "") + msg)
        elif moved:
            self.save_state()
            self.tg.send(f"Moved into {on[0]}: {', '.join(moved)} -> ${tgt['balance']:.2f}. "
                         "Waiting for the remaining open trade(s) of switched-off strategies to close.")

    # ---- products / equity ----
    def switch_exchange(self):
        """EXCHANGE changed since the last run (e.g. Delta -> Binance): POC levels and any resting order belong to the
        old exchange's symbols, so they are dropped and rebuilt; book balances carry over (paper) or are set to the new
        wallet (live). Refuses to switch while a trade from the old exchange is still open."""
        c = self.cfg
        old = self.state.get("exchange", "delta")
        if old == c["exchange"]:
            return
        if self.open_trades():
            sys.exit(f"Open trade(s) from {old} still managed by this bot: let them close (or close them on {old}) "
                     f"before switching to {c['exchange']}.")
        self.state["poc"], self.state["poc_pending"] = None, None
        msg = f"Switched exchange: {old} -> {c['exchange_name']} [{c['mode']}]. POC levels will be rebuilt from {c['exchange_name']} data."
        if not c["dry_run"]:
            self.last_equity_ts = 0
            w = round(self.equity_usd(), 2)
            on = self.enabled_books()
            if len(on) == 1 and w > 0:
                bk = self.book(on[0])
                msg += f"\n{on[0]} set to the {c['exchange_name']} wallet: ${w:,.2f} (was ${bk['balance']:.2f} on {old})."
                bk["balance"] = bk["start"] = w
                self.state["sweep_since"] = datetime.now(IST).isoformat(timespec="seconds")
        self.state["exchange"] = c["exchange"]
        self.save_state()
        self.event("EXCHANGE_SWITCH", old=old, new=c["exchange"])
        self.tg.send(msg)

    def get_products(self, force=False):
        if self.products and not force and time.time() - self.products_ts < 3600:
            return self.products
        raw = self.data.products(self.cfg["contract_types"])
        prods = []
        for p in raw:
            if str(p.get("state", "")).lower() != "live":
                continue
            sym = p.get("symbol", "")
            if self.cfg["allow"] and sym.upper() not in self.cfg["allow"]:
                continue
            if sym.upper() in self.cfg["exclude"]:
                continue
            tick, cv = fnum(p.get("tick_size")), fnum(p.get("contract_value"))
            if not sym or tick <= 0 or cv <= 0:
                continue
            prods.append({"id": int(p["id"]), "symbol": sym, "tick": tick, "cv": cv,
                          "type": p.get("contract_type"),
                          # tokenised stocks / ETFs (bStocks, xStocks, ONDO fund tokens), from Delta's description
                          "stock_token": bool(STOCK_TOKEN_RE.search(str(p.get("description") or "")))
                          or bool(p.get("non_crypto"))})
        if self.cfg["min_turnover_usd"] > 0:
            try:
                tk = {t.get("symbol"): fnum(t.get("turnover_usd"), -1)
                      for t in self.data.tickers(self.cfg["contract_types"])}
                if any(v >= 0 for v in tk.values()):
                    prods = [p for p in prods if tk.get(p["symbol"], 0) >= self.cfg["min_turnover_usd"]]
                else:
                    log.warning("turnover_usd missing from tickers; MIN_TURNOVER_USD ignored")
            except DeltaError as ex:
                log.warning("turnover filter skipped: %s", ex)
        self.products, self.products_ts = prods, time.time()
        log.info("Loaded %d live products", len(prods))
        return prods

    def equity_usd(self):
        c = self.cfg
        if c["capital_usd_override"] > 0:
            base = c["capital_usd_override"]
        elif c["dry_run"]:
            base = c["start_capital_inr"] / c["usd_inr"]
        else:
            if time.time() - self.last_equity_ts < 60 and self.last_equity > 0:
                return self.last_equity
            total = 0.0
            for b in self.api.balances():
                sym = (b.get("asset_symbol") or (b.get("asset") or {}).get("symbol") or "").upper()
                bal = fnum(b.get("balance"))
                if sym in ("USD", "USDT", "USDC"):
                    total += bal
                elif sym == "INR":
                    total += bal / c["usd_inr"]
            self.last_equity, self.last_equity_ts = total, time.time()
            return total
        return base + (self.state.get("paper_pnl", 0.0) if c["dry_run"] else 0.0)

    def wallet_line(self):
        """Live/demo: the real wallet total, as a cross-check of the three book balances."""
        if self.cfg["dry_run"]:
            return ""
        try:
            self.last_equity_ts = 0
            w = self.equity_usd()
            return f"Delta wallet: ${w:.2f} (books total ${sum(b['balance'] for n, b in self.state['books'].items() if n in BOOKS):.2f})\n"
        except Exception as ex:  # noqa: BLE001
            log.warning("wallet lookup failed: %s", ex)
            return ""

    # ---- guardrails ----
    def bucket(self, kind, key, equity, book=None):
        holder = self.book(book) if book else self.state
        b = holder[kind].setdefault(key, {"trades": 0, "pnl": 0.0, "start_equity": equity,
                                          "notified": False})
        if not b.get("start_equity"):
            b["start_equity"] = equity
        return b

    def entry_allowed(self, book, equity, now=None, product_id=None):
        c, st = self.cfg, self.state
        now = now or time.time()
        dk, mk = period_keys(now)
        d, m = self.bucket("day", dk, equity, book), self.bucket("month", mk, equity, book)
        if st["paused"]:
            return False, "bot is paused"
        if session_phase(c, now) != "entry":
            return False, "outside the US session entry window"
        if self.book(book)["open_trade"]:
            return False, f"{book} already has a position open"
        if product_id is not None and any(t["product_id"] == product_id for _, t in self.open_trades()):
            return False, "this coin is already held by another strategy"
        if c["max_trades_day"] > 0 and d["trades"] >= c["max_trades_day"]:
            return False, f"daily trade cap reached ({c['max_trades_day']})"
        if c["max_trades_month"] > 0 and m["trades"] >= c["max_trades_month"]:
            return False, f"monthly trade cap reached ({c['max_trades_month']})"
        if c["daily_loss_pct"] > 0 and d["pnl"] <= -c["daily_loss_pct"] / 100.0 * d["start_equity"]:
            return False, f"daily loss limit hit ({c['daily_loss_pct']}%)"
        if c["monthly_loss_pct"] > 0 and m["pnl"] <= -c["monthly_loss_pct"] / 100.0 * m["start_equity"]:
            return False, f"monthly loss limit hit ({c['monthly_loss_pct']}%)"
        return True, ""

    # ---- RSI arming scan (2H) ----
    def scan_4h(self, now, boundary):
        prods = self.get_products()
        expected = boundary - PERIOD[ARM_TF]
        c = self.cfg

        def work(p):
            try:
                return p, self.data.candles(p["symbol"], ARM_TF, 150, now), None
            except Exception as ex:  # noqa: BLE001
                return p, None, str(ex)

        old_s, old_l = self.state["armed"], self.state.get("armed_long", {})
        new_s, new_l = {}, {}
        checked = stale = errors = 0
        with ThreadPoolExecutor(max_workers=c["workers"]) as ex:
            for p, cand, err in ex.map(work, prods):
                if err:
                    errors += 1
                    continue
                cand = closed_only(cand, PERIOD[ARM_TF], now)
                if len(cand) < c["rsi_len"] * 2 or cand[-1]["time"] != expected:
                    stale += 1
                    continue
                checked += 1
                if frozen(cand[-12:], c["min_move_pct"]):          # flat for the last 12 arming candles
                    continue
                r = rsi_wilder([x["close"] for x in cand], c["rsi_len"])
                if r is None:
                    continue
                if c["enable_short"] and r > c["rsi_level"]:
                    prev = old_s.get(p["symbol"])
                    new_s[p["symbol"]] = {"product_id": p["id"], "rsi": round(r, 2),
                                          "armed_since": prev["armed_since"] if prev else boundary}
                if c["enable_long"] and r < c["rsi_long_level"]:
                    # Long setups expire at every arming-candle close: only 15m candles after this close count.
                    new_l[p["symbol"]] = {"product_id": p["id"], "rsi": round(r, 2), "armed_since": boundary}
        newly_s = [s for s in new_s if s not in old_s]
        dropped_s = [s for s in old_s if s not in new_s]
        newly_l = [s for s in new_l if s not in old_l]
        dropped_l = [s for s in old_l if s not in new_l]
        self.state["armed"], self.state["armed_long"] = new_s, new_l
        self.state["last_4h"] = boundary
        self.save_state()
        self.event("SCAN_4H", boundary=ist(boundary), checked=checked, stale=stale, errors=errors,
                   armed_short=len(new_s), armed_long=len(new_l), newly_short=newly_s, newly_long=newly_l,
                   dropped_short=dropped_s, dropped_long=dropped_l)
        for s in newly_s:
            self.event("ARMED", side="short", symbol=s, rsi=new_s[s]["rsi"])
        for s in newly_l:
            self.event("ARMED", side="long", symbol=s, rsi=new_l[s]["rsi"])
        if newly_s or dropped_s or newly_l or dropped_l:
            lines = [f"{ARM} scan {ist(boundary)} [{c['mode']}]", f"Checked {checked} instruments"]
            if c["enable_short"]:
                lines.append(f"SHORT setups (RSI>{c['rsi_level']:g}): {len(new_s)}")
                top = sorted(new_s.items(), key=lambda kv: -kv[1]["rsi"])[:10]
                lines += [f"  {s}  RSI {v['rsi']}" for s, v in top]
                if len(new_s) > 10:
                    lines.append(f"  ... and {len(new_s) - 10} more")
            if c["enable_long"]:
                lines.append(f"LONG setups (RSI<{c['rsi_long_level']:g}): {len(new_l)}")
                top = sorted(new_l.items(), key=lambda kv: kv[1]["rsi"])[:10]
                lines += [f"  {s}  RSI {v['rsi']}" for s, v in top]
                if len(new_l) > 10:
                    lines.append(f"  ... and {len(new_l) - 10} more")
            self.tg.send("\n".join(lines))

    # ---- signal evaluation (short on 5m, long on 15m) ----
    def eval_5m(self, now, boundary, pending):
        """Short side only (kept for compatibility)."""
        return self.eval_signals(now, boundary, pending, [])

    def eval_signals(self, now, boundary, pend_short, pend_long):
        c = self.cfg
        prods = {p["symbol"]: p for p in self.get_products()}
        jobs = [("short", s, "5m") for s in pend_short] + [("long", s, "15m") for s in pend_long]

        def work(job):
            try:
                return job, self.data.candles(job[1], job[2], 80 if job[2] == "5m" else 40, now), None
            except Exception as ex:  # noqa: BLE001
                return job, None, str(ex)

        signals = []
        with ThreadPoolExecutor(max_workers=c["workers"]) as ex:
            for (side, sym, res), cand, err in ex.map(work, jobs):
                if err or cand is None:
                    continue
                per = PERIOD[res]
                cand = closed_only(cand, per, now)
                if not cand or cand[-1]["time"] != boundary - per:
                    continue  # candle not available yet; retried shortly
                self.eval_done.add(f"{side}:{sym}")
                if sym not in prods:
                    continue
                if side == "short":
                    r = detect_setup(cand, c["ema_len"])
                    a = self.state["armed"].get(sym)
                    if not r or not a:
                        continue
                    S = r["S"]
                    if S["time"] < a["armed_since"]:
                        continue  # setup candle must form after the arming-candle close that armed the symbol
                    if r["setup"]:
                        self.event("SETUP", side="short", symbol=sym, setup_candle=ist(S["time"]),
                                   high=S["high"], low=S["low"], ema=round(r["ema_S"], 6),
                                   next_candle_red=r["e_red"])
                    if r["signal"]:
                        signals.append({"side": "short", "symbol": sym, "p": prods[sym], "rsi": a["rsi"],
                                        "S": S, "E": r["E"], "entry": r["E"]["close"], "period": per,
                                        "score": a["rsi"] - c["rsi_level"]})
                else:
                    r = detect_long_setup(cand)
                    a = self.state.get("armed_long", {}).get(sym)
                    if not r or not a:
                        continue
                    C1 = r["C1"]
                    if C1["time"] < a["armed_since"]:
                        continue  # C1 must form after the arming-candle close that armed this setup
                    if r["c1_green"]:
                        self.event("SETUP", side="long", symbol=sym, c1_candle=ist(C1["time"]),
                                   c1_open=C1["open"], c1_low=C1["low"], c2_close=r["C2"]["close"],
                                   c2_close_above_c1_open=r["c2_above"])
                    if r["signal"]:
                        signals.append({"side": "long", "symbol": sym, "p": prods[sym], "rsi": a["rsi"],
                                        "S": C1, "E": r["C2"], "entry": r["C2"]["close"], "period": per,
                                        "score": c["rsi_long_level"] - a["rsi"]})
        signals.sort(key=lambda x: -x["score"])
        for sig in signals:
            self.event("SIGNAL", side=sig["side"], symbol=sig["symbol"], entry=sig["entry"],
                       setup_high=sig["S"]["high"], setup_low=sig["S"]["low"], rsi_4h=sig["rsi"])
            bk = book_for(sig["side"], "rsi")
            if not self.book_free(bk):
                self.event("SKIP", side=sig["side"], symbol=sig["symbol"], reason=f"{bk} already has a position open")
                continue
            self.try_enter(sig, now)

    # ---- breakout strategy ----
    def scan_1h(self, now, boundary):
        c = self.cfg
        prods = self.get_products()
        expected = boundary - PERIOD["1h"]
        n = c["consol_candles"]

        def work(p):
            try:
                return p, self.data.candles(p["symbol"], "1h", n + 6, now), None
            except Exception as ex:  # noqa: BLE001
                return p, None, str(ex)

        new, checked, stale, errors = {}, 0, 0, 0
        with ThreadPoolExecutor(max_workers=c["workers"]) as ex:
            for p, cand, err in ex.map(work, prods):
                if err:
                    errors += 1
                    continue
                cand = closed_only(cand, PERIOD["1h"], now)
                if (len(cand) < n or cand[-1]["time"] != expected
                        or cand[-1]["time"] - cand[-n]["time"] != (n - 1) * PERIOD["1h"]):
                    stale += 1
                    continue
                checked += 1
                if frozen(cand[-n:], c["min_move_pct"]):
                    continue
                r = detect_consolidation(cand[-n:], c["range_pct"])
                if r:
                    new[p["symbol"]] = {"product_id": p["id"], "range_high": r["range_high"],
                                        "range_low": r["range_low"], "band_pct": round(r["band_pct"], 3),
                                        "armed_since": boundary}
        self.state["armed_bo"] = new
        self.state["last_1h"] = boundary
        self.save_state()
        self.event("SCAN_1H", boundary=ist(boundary), checked=checked, stale=stale, errors=errors,
                   armed=len(new), symbols=sorted(new)[:40])

    def eval_breakout(self, now, b15, pend):
        c = self.cfg
        cm = c["check_minute"]
        prods = {p["symbol"]: p for p in self.get_products()}
        armed = self.state.get("armed_bo", {})

        def work(sym):
            try:
                return sym, self.data.candles(sym, "1m", cm + 6, now), None
            except Exception as ex:  # noqa: BLE001
                return sym, None, str(ex)

        signals = []
        with ThreadPoolExecutor(max_workers=c["workers"]) as ex:
            for sym, cand, err in ex.map(work, pend):
                a = armed.get(sym)
                if err or cand is None or not a:
                    continue
                r = detect_breakout(closed_only(cand, 60, now), b15, cm, a["range_high"], a.get("range_low"))
                if r is None:
                    continue  # minute candles not in yet; retried shortly
                self.bo_done.add(f"bo:{sym}")
                if not r["complete"] or sym not in prods:
                    continue
                if r["green"]:
                    self.event("CHECK", strategy="breakout", symbol=sym, candle=ist(b15), open=r["open"],
                               price=r["close"], low=r["low"], range_high=a["range_high"],
                               above_range=r["above"])
                if r["signal_short"] and c["enable_breakdown"]:
                    signals.append({"side": "short", "strategy": "breakdown", "symbol": sym, "p": prods[sym],
                                    "rsi": None, "S": {"time": b15, "low": r["low"], "high": r["high"]},
                                    "E": {"time": b15 + (cm - 1) * 60}, "entry": r["close"], "period": 60,
                                    "score": -a["band_pct"],
                                    "extra": {"range_high": a["range_high"], "range_low": a["range_low"],
                                              "band_pct": a["band_pct"]}})
                if r["signal"] and c["enable_breakout"]:
                    signals.append({"side": "long", "strategy": "breakout", "symbol": sym, "p": prods[sym],
                                    "rsi": None, "S": {"time": b15, "low": r["low"], "high": r["high"]},
                                    "E": {"time": b15 + (cm - 1) * 60}, "entry": r["close"], "period": 60,
                                    "score": -a["band_pct"],
                                    "extra": {"range_high": a["range_high"], "range_low": a["range_low"],
                                              "band_pct": a["band_pct"]}})
        signals.sort(key=lambda x: -x["score"])
        for sig in signals:
            bk = book_for(sig["side"], sig["strategy"])
            self.event("SIGNAL", strategy=sig["strategy"], side=sig["side"], symbol=sig["symbol"], entry=sig["entry"],
                       candle_low=sig["S"]["low"], candle_high=sig["S"]["high"],
                       range_high=sig["extra"]["range_high"], range_low=sig["extra"]["range_low"])
            if not self.book_free(bk):
                self.event("SKIP", symbol=sig["symbol"], reason=f"{bk} already has a position open")
                continue
            self.try_enter(sig, now)

    def tick_breakout(self, now):
        b1 = int(now) // PERIOD["1h"] * PERIOD["1h"]
        if b1 != self.state.get("last_1h") and now >= b1 + 8 and now >= self.next_scan_try:
            try:
                self.scan_1h(now, b1)
            except Exception:  # noqa: BLE001
                self.next_scan_try = now + 30
                raise
        b15 = int(now) // PERIOD["15m"] * PERIOD["15m"]
        t_check = b15 + self.cfg["check_minute"] * 60
        armed = self.state.get("armed_bo", {})
        if armed and t_check + 3 <= now <= t_check + 50 and now - self.last_bo_try >= 3:
            if self.bo_boundary != b15:
                self.bo_boundary, self.bo_done = b15, set()
            pend = [s for s, a in armed.items() if f"bo:{s}" not in self.bo_done
                    and a["armed_since"] <= b15 < a["armed_since"] + PERIOD["1h"]]
            free = (self.cfg["enable_breakout"] and self.book_free("1H BREAKOUT")) or \
                   (self.cfg["enable_breakdown"] and self.book_free("1H BREAKDOWN"))
            if pend and free and not self.state["paused"] \
                    and session_phase(self.cfg, now) == "entry":
                self.last_bo_try = now
                self.eval_breakout(now, b15, pend)

    # ---- POC RETURN ----
    @staticmethod
    def in_us_session(now):
        """True during the US cash session: Mon-Fri 09:30-16:00 New York (US holidays are not excluded)."""
        d = datetime.fromtimestamp(now, NY)
        return d.weekday() < 5 and (9 * 60 + 30) <= d.hour * 60 + d.minute < 16 * 60

    @staticmethod
    def last_us_close(now):
        """Unix time of the most recent weekday 16:00 New York at or before now."""
        d = datetime.fromtimestamp(now, NY)
        for back in range(0, 8):
            day = (d - timedelta(days=back)).date()
            t = datetime(day.year, day.month, day.day, 16, 0, tzinfo=NY)
            if day.weekday() < 5 and t.timestamp() <= now:
                return int(t.timestamp())
        return None

    def build_pocs(self, now, made):
        """POC of every instrument's last US session (09:30-16:00 NY, 15m candles)."""
        c = self.cfg
        prods = self.get_products()
        start = made - 390 * 60

        def work(p):
            try:
                return p, self.data.candles(p["symbol"], "15m", 30, made), None
            except Exception as ex:  # noqa: BLE001
                return p, None, str(ex)

        levels, errors = {}, 0
        with ThreadPoolExecutor(max_workers=c["workers"]) as ex:
            for p, cand, err in ex.map(work, prods):
                if err:
                    errors += 1
                    continue
                cand = [x for x in cand if start <= x["time"] < made]
                if len(cand) < 20 or frozen(cand, c["min_move_pct"]):
                    continue
                poc = poc_price(cand, c["poc_rows"])
                if poc:
                    levels[p["symbol"]] = {"poc": poc, "product_id": p["id"], "armed_long": False, "armed_short": False}
        self.state["poc"] = {"made": made, "levels": levels, "tiered": 2}
        self.save_state()
        self.event("POC_BUILT", made=ist(made), levels=len(levels), errors=errors)
        self.tg.send(f"POC levels built from the US session ending {ist(made)} [{c['mode']}]\n"
                     f"{len(levels)} instruments (errors {errors}). Active until the next US session close (Friday's: until Monday's). "
                     f"Waiting for {c['poc_away_pct']:g}% moves away (top coins + stock/ETF/gold/silver tokens) / {c['poc_away_other_pct']:g}% "
                     f"(all others), then the first touch.")

    def poc_tier1(self, sym, prods):
        """Top coins and tokenised stocks / ETFs use the 2% rule; every other coin 5%."""
        return sym in self.cfg["poc_majors"] or bool(prods.get(sym, {}).get("stock_token"))

    def poc_pause_step(self, now):
        """POC_ENABLED=0: cancel a resting POC order (if any) and do nothing else. An open POC trade is
        managed by manage_open_trades like every other book."""
        pend = self.state.get("poc_pending")
        if pend:
            lv = (self.state.get("poc") or {}).get("levels", {}).get(pend["symbol"])
            if self.poc_cancel(pend, "POC RETURN paused (POC_ENABLED=0)", None, lv, now):
                self.save_state()

    def tick_poc(self, now):
        c = self.cfg
        made = self.last_us_close(now)
        pc = self.state.get("poc") or {}
        if made and pc.get("made") != made and not self.state.get("poc_pending"):        # a POC stays active until the next weekday session close
            if now - getattr(self, "_poc_try", 0) >= 120:
                self._poc_try = now
                self.build_pocs(now, made)
            return
        pc = self.state.get("poc") or {}
        if not pc.get("levels") and not self.state.get("poc_pending"):
            return
        if now - getattr(self, "_poc_poll", 0) < c["poc_poll"]:
            return
        self._poc_poll = now
        prices = {x.get("symbol"): fnum(x.get("mark_price")) or fnum(x.get("close"))
                  for x in self.data.tickers(c["contract_types"])}
        prods = {p["symbol"]: p for p in self.get_products()}
        if pc.get("levels") and pc.get("tiered") != 2:                # 5% rule for other coins: drop arms made under the old 2% rule
            for sym, lv in pc["levels"].items():
                if not self.poc_tier1(sym, prods):
                    lv["armed_long"] = lv["armed_short"] = False
            pc["tiered"] = 2
        changed = False
        near = c["poc_near_pct"] / 100.0
        pend = self.state.get("poc_pending")
        if pend:
            changed |= self.poc_pending_step(pend, prices, made, now)
            pend = self.state.get("poc_pending")
        if made and pc.get("made") != made:        # levels are rebuilt on the next tick, once no order is resting
            if changed:
                self.save_state()
            return
        cands = []
        no_entry = c["poc_skip_us_session"] and self.in_us_session(now)
        for sym, lv in pc["levels"].items():
            px, poc = prices.get(sym), lv["poc"]
            if not px or sym not in prods:
                continue
            away = (c["poc_away_pct"] if self.poc_tier1(sym, prods) else c["poc_away_other_pct"]) / 100.0
            for side, armed_key in (("long", "armed_long"), ("short", "armed_short")):
                sign = 1 if side == "long" else -1
                dist = (px - poc) / poc * sign            # >0: still on the armed side of the POC
                if not lv[armed_key]:
                    if dist >= away:
                        lv[armed_key] = True
                        lv.pop(f"wait_{side}", None)
                        changed = True
                        self.event("POC_ARMED", side=side, symbol=sym, poc=poc, price=px)
                    continue
                if pend and pend["symbol"] == sym and pend["side"] == side:
                    continue
                if no_entry:                               # US session: arming only, no touches / orders
                    continue
                if dist <= 0:                              # reached the POC without our order resting: missed
                    lv[armed_key] = False
                    lv.pop(f"wait_{side}", None)
                    changed = True
                    self.event("POC_MISSED", side=side, symbol=sym, poc=poc, price=px,
                               reason="price reached the POC while no order of ours was resting there")
                    continue
                if lv.get(f"wait_{side}"):                 # timed out: wait until price leaves the near zone
                    if dist > near:
                        lv.pop(f"wait_{side}")
                        changed = True
                    continue
                if dist <= near:
                    cands.append((dist, sym, side, poc))
        if pend and pend.get("recheck") is not None:      # timed out but still within 0.15% of its POC
            closer = [x for x in cands if x[0] < pend["recheck"]]
            if closer:
                lvp = pc["levels"].get(pend["symbol"])
                changed |= self.poc_cancel(pend, f"{min(closer)[1]} is closer to its POC",
                                           prices.get(pend["symbol"]), lvp, now)
                pend = self.state.get("poc_pending")
            else:
                pend.pop("recheck")
                pend["placed"] = int(now)                   # keep the order resting; check again in 5 minutes
                changed = True
        if cands and not pend and self.book_free("POC RETURN") and not self.state["paused"] \
                and session_phase(c, now) == "entry":
            dist, sym, side, poc = min(cands)               # one resting order at a time: the closest coin
            changed |= self.poc_place_limit(sym, side, poc, prods[sym], pc, now)
        if changed:
            self.save_state()

    # ---- POC limit-order entry ----
    def poc_place_limit(self, sym, side, poc, p, pc, now):
        c = self.cfg
        poc_px = round_tick(poc, p["tick"], "down" if side == "long" else "up")
        b15 = int(now) // PERIOD["15m"] * PERIOD["15m"]
        mn = c["poc_min_stop_pct"] / 100
        sig = {"side": side, "symbol": sym, "p": p, "strategy": "poc", "entry": poc_px, "limit": True,
               "S": {"time": b15, "low": poc_px * (1 - mn), "high": poc_px * (1 + mn)},
               "E": {"time": int(now) - PERIOD["15m"]}, "period": PERIOD["15m"],
               "extra": {"poc": poc, "made": pc["made"], "touch_close": b15 + PERIOD["15m"]}}
        prep = self.try_enter(sig, now)
        if not prep:
            self.pocl(sym)[f"wait_{side}"] = True          # don't retry every poll; again after price leaves the zone
            return True
        trade, info = prep
        oid = None
        if not c["dry_run"]:
            try:
                self.api.set_leverage(p["id"], int(math.ceil(c["leverage"])))
                res = self.api.place_order({"product_id": p["id"], "size": trade["contracts"],
                                            "side": "buy" if side == "long" else "sell",
                                            "order_type": "limit_order", "limit_price": fmt_price(poc_px, p["tick"]),
                                            "post_only": True, "time_in_force": "gtc",
                                            "client_order_id": f"poc{side}-{int(now)}"})
                oid = res.get("id")
            except Exception as ex:  # noqa: BLE001
                if "immediate_execution_post_only" in str(ex):
                    return self.poc_take_at_poc(sym, side, poc, poc_px, p, trade, info, now)
                self.event("POC_ORDER_FAILED", side=side, symbol=sym, error=str(ex)[:200])
                self.pocl(sym)[f"wait_{side}"] = True
                return True
        self.state["poc_pending"] = {"symbol": sym, "side": side, "product_id": p["id"], "price": poc_px,
                                     "order_id": oid, "placed": int(now), "trade": trade, "info": info}
        self.event("POC_ORDER", side=side, symbol=sym, poc=poc, limit=poc_px, contracts=trade["contracts"], order_id=oid)
        return True

    def poc_take_at_poc(self, sym, side, poc, poc_px, p, trade, info, now):
        """The post-only order was rejected because price is already at/through the POC: take liquidity with an
        immediate-or-cancel limit AT the POC - it fills at the POC or better, never worse; any unfilled part is
        cancelled by the exchange. Taker fee instead of maker, but no slippage beyond the POC."""
        lv = self.pocl(sym)
        try:
            self.api.place_order({"product_id": p["id"], "size": trade["contracts"],
                                  "side": "buy" if side == "long" else "sell",
                                  "order_type": "limit_order", "limit_price": fmt_price(poc_px, p["tick"]),
                                  "time_in_force": "ioc", "client_order_id": f"poc{side}ioc-{int(now)}"})
        except Exception as ex:  # noqa: BLE001
            self.event("POC_ORDER_FAILED", side=side, symbol=sym, error=f"IOC at POC: {str(ex)[:180]}")
            lv[f"wait_{side}"] = True
            return True
        time.sleep(1)
        pos = self.api.position(p["id"])
        pend = {"symbol": sym, "side": side, "product_id": p["id"], "price": poc_px, "order_id": None,
                "placed": int(now), "trade": trade, "info": info}
        if pos["size"] != 0:
            self.event("POC_ORDER", side=side, symbol=sym, poc=poc, limit=poc_px, contracts=trade["contracts"],
                       order_id=None, note="post-only rejected (price already at the POC): IOC limit at the POC")
            return self.poc_filled(pend, pos["entry_price"] or poc_px, abs(pos["size"]), now)
        lv["armed_" + side] = False                       # price moved away before the IOC: the touch is used up
        lv.pop("wait_" + side, None)
        self.event("POC_MISSED", side=side, symbol=sym, poc=poc, reason="post-only rejected and the IOC at the POC did not fill")
        return True

    def pocl(self, sym):
        return self.state["poc"]["levels"][sym]

    def poc_pending_step(self, pend, prices, made, now):
        """Resting POC order: filled -> open the trade; timed out / price moved away / level gone -> cancel."""
        c = self.cfg
        sym, side, poc_px = pend["symbol"], pend["side"], pend["price"]
        sign = 1 if side == "long" else -1
        px = prices.get(sym) or 0
        lv = (self.state.get("poc") or {}).get("levels", {}).get(sym)
        if c["dry_run"]:
            if px and (px - poc_px) * sign <= 0:                 # paper: filled when price reaches the limit
                return self.poc_filled(pend, poc_px, pend["trade"]["contracts"], now)
        else:
            pos = self.api.position(pend["product_id"])
            if pos["size"] != 0:
                if pend.get("order_id"):
                    try:
                        self.api.cancel_order(pend["product_id"], pend["order_id"])   # any unfilled remainder
                    except Exception:  # noqa: BLE001
                        pass
                return self.poc_filled(pend, pos["entry_price"] or poc_px, abs(pos["size"]), now)
        why = None
        if lv is None or (made and made != pend["trade"]["extra"].get("made")):
            why = "POC level expired"
        elif c["poc_skip_us_session"] and self.in_us_session(now):
            why = "US session started (no POC entries during US hours)"
        elif now - pend["placed"] >= c["poc_order_timeout"]:
            d = (px - poc_px) / poc_px * sign if px else 1.0
            if d <= c["poc_near_pct"] / 100:
                pend["recheck"] = d       # still close: tick_poc keeps it unless another coin is closer to its POC
                return False
            why = f"not filled within {c['poc_order_timeout'] // 60} min"
        elif px and (px - poc_px) / poc_px * sign > c["poc_retreat_pct"] / 100:
            why = f"price moved {c['poc_retreat_pct']:g}% back away from the POC"
        elif not c["dry_run"] and pend.get("order_id") and not any(
                str(o.get("id")) == str(pend["order_id"]) for o in self.api.open_orders(pend["product_id"])):
            why = "order no longer open on the exchange (post-only rejected or cancelled)"
        if not why:
            return False
        return self.poc_cancel(pend, why, px, lv, now, wait=why.startswith("order no longer open"))

    def poc_cancel(self, pend, why, px, lv, now, wait=False):
        c = self.cfg
        side, poc_px = pend["side"], pend["price"]
        if not c["dry_run"]:
            if pend.get("order_id"):
                try:
                    self.api.cancel_order(pend["product_id"], pend["order_id"])
                except Exception as ex:  # noqa: BLE001
                    log.warning("cancel POC order: %s", ex)
            time.sleep(1)
            pos = self.api.position(pend["product_id"])           # it may have filled while we cancelled
            if pos["size"] != 0:
                return self.poc_filled(pend, pos["entry_price"] or poc_px, abs(pos["size"]), now)
        self.state["poc_pending"] = None
        if lv is not None and wait:
            lv[f"wait_{side}"] = True               # rejected: re-place only after price leaves the 0.15% zone
        self.event("POC_ORDER_CANCELLED", side=side, symbol=pend["symbol"], reason=why, price=px)
        return True

    def poc_filled(self, pend, fill, contracts, now):
        c = self.cfg
        t = pend["trade"]
        self.state["poc_pending"] = None
        lv = (self.state.get("poc") or {}).get("levels", {}).get(pend["symbol"])
        if lv is not None:
            lv["armed_" + pend["side"]] = False                    # the touch uses up this side
            lv.pop("wait_" + pend["side"], None)
        b15 = int(now) // PERIOD["15m"] * PERIOD["15m"]
        mn = c["poc_min_stop_pct"] / 100.0
        t.update(entry=fill, contracts=contracts, opened_ts=int(now), last_checked=int(now),
                 touch_close=b15 + PERIOD["15m"], tc_low=fill, tc_high=fill)
        t["extra"]["touch_close"] = t["touch_close"]
        t["stop"] = (round_tick(fill * (1 + mn), t["tick"], "up") if t["side"] == "short"
                     else round_tick(fill * (1 - mn), t["tick"], "down"))
        self.event("POC_FILLED", side=pend["side"], symbol=pend["symbol"], fill=fill, contracts=contracts)
        if not c["dry_run"]:
            if not self.replace_stop_live(t, t["stop"], first=True):
                self.emergency_close(t, "could not confirm a stop-loss order on the exchange")
                return True
            t["protected"] = {"sl": True, "tp": None}
        self.finish_entry(t, pend["info"], now)
        return True

    def strategy_desc(self):
        c = self.cfg
        sess = (f"US session: entries Mon-Fri {c['session_open']}-{c['entry_cutoff']} New York, "
                f"all closed at {c['session_close']} NY" if c["session"] == "us" else "24h, day = New York date")
        return "Strategies (separate accounts): " + ", ".join(x for x in [
            f"{ARM} RSI SHORT (RSI>{c['rsi_level']:g}, 5m EMA setup)" if c["enable_short"] else "",
            f"{ARM} RSI LONG (RSI<{c['rsi_long_level']:g}, 15m confirmation)" if c["enable_long"] else "",
            f"1H BREAKOUT ({c['consol_candles']} hourly candles within {c['range_pct']:g}%, 15m check at minute "
            f"{c['check_minute']})" if c["enable_breakout"] else "",
            "1H RSI LONG / 1H RSI SHORT (1H RSI arming, 5m/15m RSI divergence, BE 1R, trail from 3R)"
            if c.get("enable_rsi1h") else "",
            f"POC RETURN (prev. US-session POC, {c['poc_away_pct']:g}% away (top {len(c['poc_majors'])} coins + stock/ETF/gold/silver tokens) / "
            f"{c['poc_away_other_pct']:g}% (others) then first touch, active until the next "
            f"session close; stop 0.25% then beyond the touch candle (min 0.25%); BE 1R, trail from {c['trail_start_r']:g}R"
            + ("; NO entries during the US session (Mon-Fri 09:30-16:00 NY): only after hours and weekends"
               if c["poc_skip_us_session"] else "") + ")"
            + (" - PAUSED (POC_ENABLED=0): no new orders or trades" if c["poc_paused"] else "")
            if c["enable_poc"] else ""] if x) + f"\n{sess}"

    def try_enter(self, sig, now):
        c, p = self.cfg, sig["p"]
        bk = book_for(sig.get("side", "short"), sig.get("strategy", "rsi"))
        equity = self.book(bk)["balance"]   # this strategy's own account
        ok, why = self.entry_allowed(bk, equity, now, p["id"])
        if not ok:
            self.event("SKIP", symbol=sig["symbol"], book=bk, reason=why)
            dk, mk = period_keys(now)
            b = self.bucket("day", dk, equity, bk) if "daily" in why else self.bucket("month", mk, equity, bk)
            if "loss limit" in why and not b.get("notified"):
                b["notified"] = True
                self.save_state()
                self.tg.send(f"[{bk}] trading stopped: {why}. No new entries for this strategy until the next session.")
            return
        tick = p["tick"]
        entry = sig["entry"]
        side = sig.get("side", "short")
        m = c["margin_stop_pct"] / 100.0 / c["leverage"] if c["margin_stop_pct"] > 0 else 0.0
        stop_source = "candle"
        if side == "short":
            stop = round_tick(sig["S"]["high"] + c["stop_buffer_ticks"] * tick, tick, "up")
            if m:
                # price where the short has lost margin_stop_pct of its margin (price rises by pct/leverage)
                margin_stop = round_tick(entry * (1 + m), tick, "down")
                if margin_stop < stop:
                    stop, stop_source = margin_stop, "margin"
            if stop <= entry:
                self.event("SKIP", side=side, symbol=sig["symbol"], reason="entry close is not below the stop level")
                return
        else:
            stop = round_tick(sig["S"]["low"] - c["stop_buffer_ticks"] * tick, tick, "down")
            if m:
                # price where the long has lost margin_stop_pct of its margin (price falls by pct/leverage)
                margin_stop = round_tick(entry * (1 - m), tick, "up")
                if margin_stop > stop:
                    stop, stop_source = margin_stop, "margin"
            if stop >= entry or stop <= 0:
                self.event("SKIP", side=side, symbol=sig["symbol"], reason="entry close is not above the stop level")
                return
        risk_px = abs(entry - stop)
        stop_pct = risk_px / entry * 100
        if stop_pct < c["min_stop_pct"]:
            self.event("SKIP", side=side, symbol=sig["symbol"], reason=f"stop too tight ({stop_pct:.3f}% < {c['min_stop_pct']}%): fees would dominate")
            return
        if stop_pct > c["max_stop_pct"]:
            self.event("SKIP", side=side, symbol=sig["symbol"], reason=f"stop too wide ({stop_pct:.2f}% > {c['max_stop_pct']}%)")
            return
        tp = round_tick(entry - RR * risk_px if side == "short" else entry + RR * risk_px, tick, "nearest")
        if tp <= 0:
            self.event("SKIP", side=side, symbol=sig["symbol"], reason="take-profit would be at or below zero")
            return
        ob_info = None
        if c["ob_filter"] and sig.get("strategy") != "poc":
            ob_info, ob_skip = self.check_orderbook(sig["symbol"], side)
            if ob_skip:
                self.event("SKIP", side=side, symbol=sig["symbol"], reason=ob_skip, orderbook=ob_info)
                return
        n, info = size_trade(c, equity, entry, stop, p["cv"])
        if n < 1:
            self.event("SKIP", symbol=sig["symbol"], reason="position size under 1 contract for the allowed margin/risk",
                       **{k: (round(v, 4) if isinstance(v, float) else v) for k, v in info.items()})
            return
        trade = {
            "mode": c["mode"], "book": bk, "side": side, "symbol": sig["symbol"], "product_id": p["id"], "contracts": n,
            "cv": p["cv"], "tick": tick, "entry": entry, "stop": stop, "tp": tp,
            "signal_close": entry, "rsi": sig.get("rsi"),
            "strategy": sig.get("strategy", "rsi"), "extra": sig.get("extra", {}), "ob": ob_info, "setup_time": sig["S"]["time"],
            "opened_ts": sig["E"]["time"] + sig.get("period", PERIOD["5m"]),
            "last_checked": sig["E"]["time"] + sig.get("period", PERIOD["5m"]),
            "equity": equity, "protected": None, "stop_source": stop_source,
            "leverage": c["leverage"], "margin": info.get("margin", 0.0),
        }
        if trade["strategy"] == "poc":       # no target; stop re-set at the touch candle's close, then trailing
            x = sig["extra"]
            trade.update(tp=None, trail=True, trail_r=0, stop_set=False, touch_close=x["touch_close"],
                         tc_low=entry, tc_high=entry)
        if sig.get("limit"):
            return trade, info
        if c["dry_run"]:
            # paper: capped IOC entry simulated on the live depth (was: filled at the signal candle's close)
            mn = getattr(self.data, "meta", {}).get(p["id"], {}).get("min_notional", 5)
            why = self.capped_entry(trade, mn)
            if why:
                self.event("SKIP", side=side, symbol=sig["symbol"], book=bk, reason=why, execution=trade.get("exec"))
                return
            fill = trade["entry"]
            if (side == "short" and stop <= fill) or (side == "long" and stop >= fill):
                self.event("SKIP", side=side, symbol=sig["symbol"], book=bk, reason="fill past stop")
                return
            if trade.get("tp"):
                risk_f = abs(fill - stop)
                trade["tp"] = round_tick(fill - RR * risk_f if side == "short" else fill + RR * risk_f, tick, "nearest")
            info = dict(info, notional=trade["contracts"] * p["cv"] * fill,
                        margin=trade["contracts"] * p["cv"] * fill / c["leverage"])
            self.finish_entry(trade, info, now)
            return
        try:
            self.enter_live(trade)
        except Exception as ex:  # noqa: BLE001
            log.exception("live entry failed")
            self.event("ENTRY_FAILED", symbol=sig["symbol"], error=str(ex)[:300])
            self.tg.send(f"ENTRY FAILED {sig['symbol']}: {str(ex)[:300]}\nCheck positions on Delta manually.")
            return
        if trade.get("aborted"):
            return
        self.finish_entry(trade, info, now)

    def enter_live(self, t):
        c = self.cfg
        # The margin-based stop assumes this exact leverage, so do not trade if it cannot be set.
        self.api.set_leverage(t["product_id"], int(math.ceil(c["leverage"])))
        cid = f"rsi{tside(t)}-{int(time.time())}"
        if c["exchange"] == "binance":       # capped IOC limit children (execution.py) instead of a market order
            mn = getattr(self.api, "meta", {}).get(t["product_id"], {}).get("min_notional", 5)
            why = self.capped_entry(t, mn)
            self.event("ORDER_SENT", symbol=t["symbol"], size=t["contracts"], client_order_id=cid,
                       execution=t.get("exec"), skipped=why or None)
            if why:
                t["aborted"] = True
                self.event("SKIP", symbol=t["symbol"], book=t["book"], reason=why, execution=t.get("exec"))
                return
            res = {"average_fill_price": t["entry"]}
        else:
            res = self.api.place_order({"product_id": t["product_id"], "size": t["contracts"],
                                        "side": "sell" if tside(t) == "short" else "buy",
                                        "order_type": "market_order", "client_order_id": cid})
            self.event("ORDER_SENT", symbol=t["symbol"], size=t["contracts"], client_order_id=cid,
                       state=res.get("state"))
        pos = None
        for _ in range(10):
            time.sleep(1)
            pos = self.api.position(t["product_id"])
            if pos["size"] != 0:
                break
        if not pos or pos["size"] == 0:
            raise DeltaError("entry order sent but no open position found; check Delta")
        fill = pos["entry_price"] or fnum(res.get("average_fill_price")) or t["entry"]
        t["contracts"] = abs(pos["size"])
        t["entry"] = fill
        t["opened_ts"] = t["last_checked"] = int(time.time())
        risk = (t["stop"] - fill) if tside(t) == "short" else (fill - t["stop"])
        if risk <= 0:
            self.emergency_close(t, "fill price is already beyond the stop")
            return
        if t.get("trail"):                       # POC: no target; initial stop POC_MIN_STOP_PCT from the real fill
            mn = c["poc_min_stop_pct"] / 100.0
            t["stop"] = (round_tick(fill * (1 + mn), t["tick"], "up") if tside(t) == "short"
                         else round_tick(fill * (1 - mn), t["tick"], "down"))
            t["tc_low"] = t["tc_high"] = fill
            if not self.replace_stop_live(t, t["stop"], first=True):
                self.emergency_close(t, "could not confirm a stop-loss order on the exchange")
                return
            t["protected"] = {"sl": True, "tp": None}
            return
        t["tp"] = round_tick(fill - RR * risk if tside(t) == "short" else fill + RR * risk, t["tick"], "nearest")
        sl_ok, tp_ok = self.place_protection(t)
        if not sl_ok:
            self.emergency_close(t, "could not confirm a stop-loss order on the exchange")
            return
        t["protected"] = {"sl": sl_ok, "tp": tp_ok}
        if not tp_ok:
            self.tg.send(f"WARNING {t['symbol']}: stop-loss is set but the take-profit order could not be confirmed. Check Delta.")

    def find_protection(self, t):
        sl = tp = None
        tol = t["tick"] * 0.5 + 1e-12
        for o in self.api.open_orders(t["product_id"]):
            if o.get("side") != exit_side(t):
                continue
            price = fnum(o.get("stop_price")) or fnum(o.get("limit_price"))
            if price <= 0:
                continue
            if t.get("tp") and abs(price - t["tp"]) < abs(price - t["stop"]):
                tp = o
            elif abs(price - t["stop"]) <= tol or (sl is None and not t.get("be_moved")):
                # after a breakeven move, only an order AT the new stop counts as the stop-loss
                sl = o
        return sl, tp

    def place_protection(self, t):
        tick, pid = t["tick"], t["product_id"]
        sl_p, tp_p = fmt_price(t["stop"], tick), fmt_price(t["tp"], tick)
        trig = self.cfg["stop_trigger"]
        try:
            self.api.bracket(pid, t["symbol"], sl_p, tp_p, trig)
        except Exception as ex:  # noqa: BLE001
            log.warning("bracket order failed, falling back to separate orders: %s", ex)
        time.sleep(1)
        sl, tp = self.find_protection(t)
        if not sl:
            try:
                self.api.place_order({"product_id": pid, "size": t["contracts"], "side": exit_side(t),
                                      "order_type": "market_order", "stop_order_type": "stop_loss_order",
                                      "stop_price": sl_p, "stop_trigger_method": trig, "reduce_only": True})
            except Exception as ex:  # noqa: BLE001
                log.warning("standalone stop failed: %s", ex)
            time.sleep(1)
            sl, tp = self.find_protection(t)
        if sl and not tp:
            try:
                self.api.place_order({"product_id": pid, "size": t["contracts"], "side": exit_side(t),
                                      "order_type": "limit_order", "limit_price": tp_p,
                                      "reduce_only": True, "time_in_force": "gtc"})
            except Exception as ex:  # noqa: BLE001
                log.warning("standalone take-profit failed: %s", ex)
            time.sleep(1)
            sl, tp = self.find_protection(t)
        return bool(sl), bool(tp)

    def emergency_close(self, t, reason):
        log.error("EMERGENCY CLOSE %s: %s", t["symbol"], reason)
        try:
            self.api.cancel_all(t["product_id"])
        except Exception as ex:  # noqa: BLE001
            log.warning("cancel_all failed: %s", ex)
        try:
            self.api.place_order({"product_id": t["product_id"], "size": t["contracts"], "side": exit_side(t),
                                  "order_type": "market_order", "reduce_only": True})
        except Exception as ex:  # noqa: BLE001
            self.tg.send(f"CRITICAL: could not close {t['symbol']} ({reason}). Close it manually on {self.cfg['exchange_name']} NOW. Error: {str(ex)[:200]}")
            t["aborted"] = True
            self.event("EMERGENCY_CLOSE_FAILED", symbol=t["symbol"], reason=reason)
            return
        t["aborted"] = True
        self.event("ABORTED", symbol=t["symbol"], reason=reason)
        self.tg.send(f"TRADE ABORTED {t['symbol']}: {reason}. Position was closed at market.")

    def capped_entry(self, t, min_notional, book=None, impact_qty=None, impact_capped=False, sweep=None):
        """Liquidity cap + capped IOC entry (execution.py) for trade t (side, symbol, product_id, contracts, cv = qty
        step, tick). Paper: simulated on the live /fapi/v1/depth; live: BinanceLiveVenue. Returns "" when something
        filled (t updated: contracts = filled, entry = average fill, exec = the numbers), else the short skip reason
        ("no order book", "too little liquidity", "no fill within cap"); t["exec"] holds what is known.
        impact_qty (1H RSI books, 26 Nov 2026): the size was already limited to the 0.05% price-impact limit, which
        replaces the liq_cap_pct-of-top-5 cap; liq_cap_qty / liq_capped then report that limit. sweep: the first IOC
        child takes the whole size at this price (the last level inside the impact limit), capped as usual."""
        c, step = self.cfg, t["cv"]
        side = "LONG" if tside(t) == "long" else "SHORT"
        ex = t["exec"] = {}
        if book is None:
            try:
                book = self.data.orderbook(t["symbol"], 20)
            except Exception as e:  # noqa: BLE001
                log.warning("order book %s: %s", t["symbol"], e)
                return "no order book"
        levels = execution.our_side(book, side)
        if not levels:
            return "no order book"
        ref = levels[0][0]
        qty = round(t["contracts"] * step, 10)
        if impact_qty is None:
            capped, max_q, depth5 = execution.liquidity_cap(qty, step, levels, c["liq_cap_pct"])
            ex.update(depth5_qty=round(depth5, 10), liq_cap_qty=max_q,
                      liq_capped="yes" if capped < qty - 1e-12 else "no", qty_risk=qty, entry_ref=ref)
        else:
            capped = min(qty, impact_qty)
            ex.update(depth5_qty=round(sum(q for _, q in levels[:5]), 10), liq_cap_qty=impact_qty,
                      liq_capped="yes" if impact_capped or capped < qty - 1e-12 else "no", qty_risk=qty, entry_ref=ref)
        if capped < step - 1e-12 or capped * ref < min_notional - 1e-9:
            return "too little liquidity"
        min_units = math.ceil(min_notional / ref / step - 1e-9) * step
        venue = (execution.PaperVenue(lambda: self.data.orderbook(t["symbol"], 20)) if c["dry_run"]
                 else BinanceLiveVenue(self.api, self.data, t))
        r = execution.run_entry(venue, side, capped, step, t["tick"], c["entry_slip_cap_pct"], book,
                                min_units=min_units, sleep=time.sleep, max_children=c["entry_max_children"],
                                sweep=sweep)
        ex.update(entry_cap=r["cap"], entry_children=r["children"], qty_intended=capped, qty_filled=r["filled"])
        if r["filled"] < step - 1e-12 or not r["avg"]:
            return "no fill within cap"
        pct, money = execution.slippage("BUY" if side == "LONG" else "SELL", ref, r["avg"], r["filled"])
        ex.update(entry_avg=r["avg"], entry_slip_pct=round(pct, 4), entry_slip_usd=round(money, 4))
        spr, spr_pct = execution.spread(book)                  # best ask - best bid at entry (26 Nov 2026)
        if spr is not None:
            ex.update(entry_spread=spr, entry_spread_pct=round(spr_pct, 4))
        t["contracts"] = int(round(r["filled"] / step))
        t["entry"] = r["avg"]
        return ""

    def exit_exec(self, t, res):
        """Exit execution numbers for trades.csv, both signed (26 Nov 2026): vs the stop / target that triggered the
        exit (exit_trigger_*) and vs the best bid / ask (exit_ref / exit_slip_*). Paper: the market exit is estimated
        by walking the live depth when the exit is detected (the P&L still uses the stop / candle price); live: the
        fill vs the trigger."""
        if res.get("exec"):
            return res["exec"]                       # a limit exit: its simulated fills
        qty = t["contracts"] * t["cv"]
        sell = tside(t) == "long"
        r = res.get("reason") or ""
        trig = t.get("tp") if r == "TAKE_PROFIT" else t["stop"] if r.endswith("STOP") or r == "STOP_LOSS" else None
        kind = "target" if r == "TAKE_PROFIT" else "stop"
        try:
            if self.cfg["dry_run"]:
                bids, asks = self.data.orderbook(t["symbol"], 20)
                levels = bids if sell else asks
                if not levels:
                    return {}
                got, avg = execution.walk_market(levels, qty)
                ex = execution.exit_slips("LONG" if sell else "SHORT", avg, got, levels[0][0], trig, kind)
                ex.update(exit_avg=avg, exit_children=1, exit_type="market",
                          exit_estimated="yes" if got >= qty - 1e-12 else f"yes (book holds {got:g} of {qty:g})")
                return usd(ex)
            if not trig:
                return {}
            ex = execution.exit_slips("LONG" if sell else "SHORT", res["price"], qty, None, trig, kind)
            ex.update(exit_avg=res["price"], exit_children=1, exit_estimated="no", exit_type="market")
            return usd(ex)
        except Exception as e:  # noqa: BLE001
            log.warning("exit slippage %s: %s", t["symbol"], e)
            return {}

    # ---- limit exits (1H RSI books, paper; execution.py) ----
    def start_limit_exit(self, t, res):
        """TRAIL_STOP: when a market exit would slip more than exit_slip_cap_pct on the live depth, exit with limit
        children from the best price down to the floor (trail stop - that %); the rest waits there (pending_exit,
        saved in state.json) until it fills or price is back to entry + exit_guard_r x R. Returns False (the caller
        exits at market as before) when the book is deep enough or unavailable."""
        c, side = self.cfg, ("LONG" if tside(t) == "long" else "SHORT")
        qty = round(t["contracts"] * t["cv"], 10)
        try:
            book = self.data.orderbook(t["symbol"], 20)
        except Exception as e:  # noqa: BLE001
            log.warning("limit exit %s: no order book (%s), exiting at market", t["symbol"], e)
            return False
        take = execution.exit_take_side(side)
        levels = execution.our_side(book, take)
        if not levels or not execution.exit_needs_limit(book, side, qty, c["exit_slip_cap_pct"]):
            return False
        mslip = execution.market_slip_pct(levels, qty, take)
        sign = 1 if side == "LONG" else -1
        guard = t["entry"] + sign * c["exit_guard_r"] * risk0(t)
        pend = execution.start_exit(side, qty, t["tick"], t["stop"], c["exit_slip_cap_pct"], guard, res["reason"],
                                    "trail", touch=levels[0][0], trigger=t["stop"])
        pend.update(unit=t["cv"], trigger_ts=res["ts"])
        t["pending_exit"] = pend
        self.save_state()
        venue = execution.PaperVenue(lambda: self.data.orderbook(t["symbol"], 20))
        execution.exit_steps(pend, venue, t["cv"], t["tick"], book, sleep=time.sleep,
                             max_children=c["exit_max_children"])
        fmt = lambda x: fmt_price(x, t["tick"])  # noqa: E731
        msg = (f"TRAIL STOP [{t['book']}] {t['symbol']} {tside(t).upper()}: market order would slip "
               + ("more than the visible book holds" if mslip is None else f"{mslip:.3f}%")
               + f" -> limit exit. Floor {fmt(pend['floor'])} (stop {fmt(t['stop'])} - {c['exit_slip_cap_pct'] * 100:g}%); "
               f"filled {pend['qty'] - pend['left']:g}/{pend['qty']:g} in {pend['children']} order(s)")
        if not execution.exit_done(pend):
            msg += (f"; {pend['left']:g} waiting at {fmt(pend['floor'])}, market if price reaches {fmt(guard)} "
                    f"(entry {'+' if sign > 0 else '-'} {c['exit_guard_r']:g}R)")
        self.event("LIMIT_EXIT", book=t["book"], symbol=t["symbol"], side=tside(t), floor=pend["floor"], guard=guard,
                   market_slip_pct=None if mslip is None else round(mslip, 4), filled=pend["qty"] - pend["left"],
                   qty=pend["qty"], children=pend["children"])
        self.tg.send(msg)
        if execution.exit_done(pend):
            self.finish_limit_exit(t)
        else:
            self.save_state()
        return True

    def step_limit_exit(self, t, now, force=None):
        """A pending limit exit, every manage pass: the rest at market when the best bid (long) / ask (short) is
        back at the guard (or force = a reason: session close / killswitch), else the resting limit is checked."""
        pend = t["pending_exit"]
        try:
            book = self.data.orderbook(t["symbol"], 20)
        except Exception as e:  # noqa: BLE001
            log.warning("pending exit %s: no order book (%s)", t["symbol"], e)
            if not force:
                return
            px = self.last_price(t["symbol"])
            book = ([(px, pend["left"])], [(px, pend["left"])])
        levels = execution.our_side(book, execution.exit_take_side(pend["side"]))
        if force:
            execution.exit_market(pend, book, force)
        elif levels and execution.guard_crossed(pend["side"], levels[0][0], pend["guard"]):
            execution.exit_market(pend, book, f"price back to entry {'+' if pend['side'] == 'LONG' else '-'} "
                                              f"{self.cfg['exit_guard_r']:g}R")
        else:
            execution.exit_rest(pend, book, pend.get("unit") or 0)
        if execution.exit_done(pend):
            self.finish_limit_exit(t, now)
        else:
            self.save_state()

    def finish_limit_exit(self, t, now=None):
        pend = t.pop("pending_exit")
        ex = usd(execution.exit_summary(pend))
        self.close_trade(t, {"reason": pend["reason"], "price": ex["exit_avg"], "ts": int(now or time.time()),
                             "estimated": True, "exec": ex})

    def finish_entry(self, t, info, now):
        c = self.cfg
        dk, mk = period_keys(now)
        eq, bk = t["equity"], t["book"]
        self.bucket("day", dk, eq, bk)["trades"] += 1
        self.bucket("month", mk, eq, bk)["trades"] += 1
        t["risk_usd"] = t["contracts"] * t["cv"] * abs(t["stop"] - t["entry"])
        t["orig_stop"], t["risk0"], t["be_moved"] = t["stop"], abs(t["stop"] - t["entry"]), False
        t["notional"] = t["contracts"] * t["cv"] * t["entry"]
        self.add_trade(bk, t)
        self.save_state()
        self.event("ENTRY", book=bk, side=tside(t), symbol=t["symbol"], mode=c["mode"], entry=t["entry"], stop_loss=t["stop"],
                   take_profit=t["tp"], contracts=t["contracts"], risk_usd=round(t["risk_usd"], 2),
                   notional_usd=round(t["notional"], 2), rsi_4h=t["rsi"], stop_source=t.get("stop_source"),
                   leverage=t.get("leverage"), margin_usd=round(t.get("margin", 0.0), 2),
                   orderbook=t.get("ob"), **({"execution": t["exec"]} if t.get("exec") else {}),
                   **({"setup": t["extra"].get("detail")} if t.get("strategy") == "rsi1h" else {}))
        fmt = lambda x: fmt_price(x, t["tick"])  # noqa: E731
        if t.get("strategy") == "rsi1h":
            x = t["extra"]
            self.tg.send(
                f"{tside(t).upper()} ENTERED [{bk}] [{c['mode']}]\n{t['symbol']}\n"
                f"Entry: {fmt(t['entry'])} (last price {x['last_price']:g})\n"
                + (f"{execution.entry_text(t['exec'], fmt)}\n" if t.get("exec") else "") +
                f"Stop-loss: {fmt(t['stop'])} ({'widened to ' + format(self.rsi1h.c['min_stop_pct'], 'g') + '%' if x['stop_widened'] else '1 tick beyond the divergence candle'})\n"
                f"Target: none - stop to entry at {t['be_r']:g}R, then trailing in whole R from {t['trail_start_r']:g}R\n"
                f"Size: {t['contracts'] * t['cv']:g} (~${t['notional']:.0f} notional), {t['leverage']:g}x isolated, "
                f"margin ${t['margin']:.2f}\n"
                f"Sizing: target {x.get('target_qty', 0):g} (free margin ${x.get('free_margin', 0):.2f} x {t['leverage']:g}), "
                f"0.05% impact limit {x.get('impact_qty', 0):g} -> {x.get('size_binding', '')} binds\n"
                f"Hard stop: {fmt(t['hard_stop'])}{' (active)' if t.get('stop_source') == 'hard' else ''} | "
                f"Liquidation: {fmt(t['liq_price'])}\n"
                f"Loss if stopped: ${t['loss_if_stopped_usd']:.2f} = {t['loss_if_stopped_pct']:.1f}% of {bk}'s ${eq:.2f} "
                f"(incl. fees)\n"
                f"{bk}: {len(self.book_trades(bk))} trade(s) open, free margin ${self.free_margin(bk):.2f}\n"
                f"1H RSI {t['rsi']} | {x['detail']}\nNo real order placed (paper mode).")
            return
        stop_label = (f"{c['margin_stop_pct']:g}%-of-margin stop" if t.get("stop_source") == "margin"
                      else f"{c['poc_min_stop_pct']:g}% initial stop" if t.get("trail") else "candle stop")
        self.tg.send(
            f"{tside(t).upper()} ENTERED [{bk}] [{c['mode']}]\n"
            f"{t['symbol']}\n"
            f"Entry: {fmt(t['entry'])}\nStop-loss: {fmt(t['stop'])} ({stop_label}"
            + (f"; moves beyond the 15m touch candle when it closes, min {c['poc_min_stop_pct']:g}%)\nTarget: none - stop to entry at 1R, trailing from "
               f"{c['trail_start_r']:g}R\n" if t.get("trail") else f")\nTake-profit (1:5): {fmt(t['tp'])}\n") +
            f"Size: {t['contracts']} contracts (~${t['notional']:.0f} notional)\n"
            f"Leverage {t.get('leverage'):g}x, margin ${t.get('margin', 0):.2f} ({t.get('margin', 0) / eq * 100:.0f}% of account)\n"
            f"Risk: ${t['risk_usd']:.2f} / Rs {self.inr(t['risk_usd']):.0f} ({t['risk_usd'] / eq * 100:.2f}% of {bk}'s ${eq:.2f})\n"
            + self.entry_detail(t, fmt) + self.ob_line(t)
            + ("\nNo real order placed (paper mode)." if c["dry_run"] else
               ("" if t.get("trail") or (t.get("protected", {}) and t["protected"].get("tp")) else "\nWARNING: take-profit not confirmed."))
        )

    def ob_line(self, t):
        ob = t.get("ob")
        if not self.cfg["ob_filter"]:
            return ""
        if not ob:
            return "\nOrder book: not available (check skipped)"
        mine, other = ("bids", "asks") if tside(t) == "long" else ("asks", "bids")
        return (f"\nOrder book (+/-{self.cfg['ob_depth_pct']:g}%): {mine} {ob['ratio']:g}x {other} "
                f"({ob['bid_qty']:,.0f} bid vs {ob['ask_qty']:,.0f} ask contracts)")

    def entry_detail(self, t, fmt):
        if t.get("strategy") == "poc":
            x = t.get("extra", {})
            return (f"POC {fmt(x.get('poc', 0))} (US session ending {ist(x.get('made', 0))}); price was "
                    f"{self.cfg['poc_away_pct'] if self.poc_tier1(t['symbol'], {p['symbol']: p for p in self.get_products()}) else self.cfg['poc_away_other_pct']:g}%+ "
                    f"{'above' if tside(t) == 'long' else 'below'} it and came back")
        if t.get("strategy") in ("breakout", "breakdown"):
            x = t.get("extra", {})
            return (f"1H range: {fmt(x.get('range_low', 0))} - {fmt(x.get('range_high', 0))} "
                    f"({x.get('band_pct', 0):.2f}% band, bodies)\n{t['strategy'].capitalize()} candle (15m): "
                    f"{ist(t['setup_time'])}")
        return (f"{ARM} RSI: {t.get('rsi')}\nSetup candle{' (C1, 15m)' if tside(t) == 'long' else ' (5m)'}: "
                f"{ist(t['setup_time'])}")

    # ---- managing the open trade ----
    def check_exit_by_candles(self, t, now, allow_be=True):
        """Walk 1-minute candles since last check; stop assumed first if both hit in one candle.
        A breakeven move triggered inside a candle only applies from the next candle."""
        start = int(t["last_checked"])
        count = int((now - start) / 60) + 3
        if count > 1400:
            count = 1400
        cand = closed_only(self.data.candles(t["symbol"], "1m", count, now), 60, now)
        last = start
        for cd in cand:
            if cd["time"] < start:
                continue
            last = cd["time"] + 60
            short = tside(t) == "short"
            if t.get("trail") and not t.get("stop_set") and cd["time"] >= t["touch_close"]:
                self.set_touch_stop(t)
            if (cd["high"] >= t["stop"]) if short else (cd["low"] <= t["stop"]):
                reason = ("TRAIL_STOP" if t.get("trail_r") else "BREAKEVEN_STOP" if t.get("be_moved") else "STOP_LOSS")
                return {"reason": reason, "price": t["stop"], "ts": cd["time"] + 60, "estimated": True}
            if t.get("tp") and ((cd["low"] <= t["tp"]) if short else (cd["high"] >= t["tp"])):
                return {"reason": "TAKE_PROFIT", "price": t["tp"], "ts": cd["time"] + 60, "estimated": True}
            if t.get("trail") and not t.get("stop_set"):
                t["tc_low"], t["tc_high"] = min(t["tc_low"], cd["low"]), max(t["tc_high"], cd["high"])
                continue                        # R is only known once the touch candle has closed
            if t.get("trail") and risk0(t) > 0:
                fav = (t["entry"] - cd["low"]) if short else (cd["high"] - t["entry"])
                r = int(math.floor(fav / risk0(t) + 1e-9))
                if r >= t.get("trail_start_r", self.cfg["trail_start_r"]) and r > t.get("trail_r", 0):
                    self.apply_trail(t, r)
            be_r = t.get("be_r", self.cfg["be_trigger_r"])
            if allow_be and be_r > 0 and not t.get("be_moved") and risk0(t) > 0:
                fav = (t["entry"] - cd["low"]) if short else (cd["high"] - t["entry"])
                if fav >= be_r * risk0(t):
                    self.apply_be(t, cd["time"] + 60)
        t["last_checked"] = last
        return None

    def last_price(self, symbol):
        tk = self.data.ticker(symbol)
        return fnum(tk.get("mark_price")) or fnum(tk.get("close"))

    def live_exit_info(self, t):
        try:
            hist = self.api.order_history(t["product_id"])
            cands = [o for o in hist if o.get("side") == exit_side(t) and fnum(o.get("average_fill_price")) > 0
                     and str(o.get("state", "")).lower() in ("closed", "filled")]
            if cands:
                o = max(cands, key=lambda x: int(fnum(x.get("id"), 0)))
                px = fnum(o.get("average_fill_price"))
                tol = 0.2 * risk0(t)
                short = tside(t) == "short"
                if t.get("tp") and ((px <= t["tp"] + tol) if short else (px >= t["tp"] - tol)):
                    reason = "TAKE_PROFIT"
                elif t.get("trail_r") and abs(px - t["stop"]) <= tol:
                    reason = "TRAIL_STOP"
                elif t.get("be_moved") and abs(px - t["stop"]) <= tol:
                    reason = "BREAKEVEN_STOP"
                elif (px >= t["stop"] - tol) if short else (px <= t["stop"] + tol):
                    reason = "STOP_LOSS"
                else:
                    reason = "OTHER (closed between stop and target)"
                return {"reason": reason, "price": px, "ts": int(time.time()), "estimated": False}
        except Exception as ex:  # noqa: BLE001
            log.warning("order history lookup failed: %s", ex)
        t2 = dict(t)
        t2["last_checked"] = t["opened_ts"]
        res = self.check_exit_by_candles(t2, time.time(), allow_be=False)
        if res:
            return res
        return {"reason": "OTHER (position closed; reason unknown)", "price": self.last_price(t["symbol"]),
                "ts": int(time.time()), "estimated": True}

    def touch_stop_price(self, t):
        """POC trades: the stop just beyond the touch candle's full high (short) / low (long),
        never closer than POC_MIN_STOP_PCT and never beyond the margin stop."""
        tick, c = t["tick"], self.cfg
        hi, lo = t["tc_high"], t["tc_low"]
        try:
            start = t["touch_close"] - PERIOD["15m"]
            for cd in self.data.candles(t["symbol"], "15m", 4, t["touch_close"] + 5):
                if cd["time"] == start:
                    hi, lo = max(hi, cd["high"]), min(lo, cd["low"])
        except Exception as ex:  # noqa: BLE001
            log.warning("touch candle fetch %s: %s", t["symbol"], ex)
        e, buf = t["entry"], c["stop_buffer_ticks"] * tick
        mn = c["poc_min_stop_pct"] / 100.0
        mx = c["margin_stop_pct"] / 100.0 / c["leverage"] if c["margin_stop_pct"] > 0 else 0.015
        if tside(t) == "long":
            return round_tick(max(min(lo - buf, e * (1 - mn)), e * (1 - mx)), tick, "up")
        return round_tick(min(max(hi + buf, e * (1 + mn)), e * (1 + mx)), tick, "down")

    def notify_touch_stop(self, t):
        fmt = lambda x: fmt_price(x, t["tick"])  # noqa: E731
        self.tg.send(f"STOP SET [{t['book']}] {t['symbol']} {tside(t).upper()} [{t['mode']}]\n"
                     f"Touch candle closed: stop {fmt(t['stop'])} ({t['risk0'] / t['entry'] * 100:.2f}% from entry "
                     f"{fmt(t['entry'])}). 1R = {fmt(t['risk0'])}; stop to entry at "
                     f"{fmt(t['entry'] + (1 if tside(t) == 'long' else -1) * t['risk0'])}.")

    # ---- live stop handling for POC trades (no target; stop replaced at candle close, breakeven, each trail step) ----
    def stop_orders(self, t):
        return [o for o in self.api.open_orders(t["product_id"])
                if o.get("side") == exit_side(t) and fnum(o.get("stop_price")) > 0]

    def replace_stop_live(self, t, new, first=False):
        """Place a reduce-only stop at `new`, confirm it, then cancel every other stop order. The old stop stays
        in force until the new one is confirmed, so the position is never unprotected."""
        tol = t["tick"] * 0.5 + 1e-12
        try:
            self.api.place_order({"product_id": t["product_id"], "size": t["contracts"], "side": exit_side(t),
                                  "order_type": "market_order", "stop_order_type": "stop_loss_order",
                                  "stop_price": fmt_price(new, t["tick"]),
                                  "stop_trigger_method": self.cfg["stop_trigger"], "reduce_only": True})
        except Exception as ex:  # noqa: BLE001
            log.warning("stop order at %s failed: %s", new, ex)
            if not t.get("stop_warned"):
                t["stop_warned"] = True
                self.tg.send(f"WARNING {t['symbol']}: could not place the stop at {fmt_price(new, t['tick'])} "
                             f"({str(ex)[:150]}). " + ("" if first else "The previous stop is still active. ") + "Retrying.")
            return False
        for _ in range(3):
            time.sleep(1)
            orders = self.stop_orders(t)
            if any(abs(fnum(o.get("stop_price")) - new) <= tol for o in orders):
                break
        else:
            return False
        for o in orders:
            if abs(fnum(o.get("stop_price")) - new) > tol and o.get("id") is not None:
                try:
                    self.api.cancel_order(t["product_id"], o["id"])
                except Exception as ex:  # noqa: BLE001
                    log.warning("cancel old stop %s failed: %s", o.get("id"), ex)
        t["stop"], t["stop_warned"] = new, False
        self.save_state()
        return True

    def manage_poc_live(self, t, now):
        c = self.cfg
        if t.get("forced"):
            return
        if not t.get("stop_set"):
            if now >= t["touch_close"] + 5 and now - t.get("tc_try", 0) >= 10:
                t["tc_try"] = now
                new = self.touch_stop_price(t)
                if self.replace_stop_live(t, new):
                    t["orig_stop"], t["risk0"], t["stop_set"] = t["stop"], abs(t["entry"] - t["stop"]), True
                    t["risk_usd"] = t["contracts"] * t["cv"] * t["risk0"]
                    self.save_state()
                    self.notify_touch_stop(t)
        else:
            px = self.last_price(t["symbol"])
            if px > 0 and risk0(t) > 0:
                fav = (t["entry"] - px) if tside(t) == "short" else (px - t["entry"])
                r = int(math.floor(fav / risk0(t) + 1e-9))
                sign = 1 if tside(t) == "long" else -1
                if r >= c["trail_start_r"] and r > t.get("trail_r", 0):
                    new = round_tick(t["entry"] + sign * (r - 1) * risk0(t), t["tick"], "up" if sign > 0 else "down")
                    if self.replace_stop_live(t, new):
                        t["trail_r"], t["be_moved"] = r, True
                        self.save_state()
                        self.tg.send(f"TRAILING STOP [{t['book']}] {t['symbol']} {tside(t).upper()} [{t['mode']}]\n"
                                     f"Reached {r}R: stop moved to {fmt_price(new, t['tick'])} ({r - 1}R locked).")
                elif not t.get("be_moved") and c["be_trigger_r"] > 0 and fav >= c["be_trigger_r"] * risk0(t):
                    if self.replace_stop_live(t, be_stop_price(t)):
                        t["be_moved"], t["be_ts"] = True, int(now)
                        self.save_state()
                        self.notify_be(t)
        if now - t.get("last_protect_check", 0) >= 60:
            t["last_protect_check"] = now
            tol = t["tick"] * 0.5 + 1e-12
            if not any(abs(fnum(o.get("stop_price")) - t["stop"]) <= tol for o in self.stop_orders(t)):
                self.tg.send(f"ALERT {t['symbol']}: stop-loss order missing on exchange, re-placing.")
                if not self.replace_stop_live(t, t["stop"], first=True):
                    self.emergency_close(t, "stop-loss missing and could not be re-placed")

    def set_touch_stop(self, t):
        """Paper POC trades: apply the touch-candle stop when the candle closes."""
        t["stop"] = self.touch_stop_price(t)
        t["orig_stop"], t["risk0"], t["stop_set"] = t["stop"], abs(t["entry"] - t["stop"]), True
        t["risk_usd"] = t["contracts"] * t["cv"] * t["risk0"]
        self.save_state()
        self.notify_touch_stop(t)

    def apply_trail(self, t, r):
        sign = 1 if tside(t) == "long" else -1
        t["stop"] = round_tick(t["entry"] + sign * (r - 1) * risk0(t), t["tick"], "up" if sign > 0 else "down")
        t["trail_r"], t["be_moved"] = r, True
        self.save_state()
        self.tg.send(f"TRAILING STOP [{t['book']}] {t['symbol']} {tside(t).upper()}\nReached {r}R: stop moved to "
                     f"{fmt_price(t['stop'], t['tick'])} ({r - 1}R locked).")

    def apply_be(self, t, ts):
        """Record a breakeven move (paper) and notify."""
        t["stop"], t["be_moved"], t["be_ts"] = be_stop_price(t), True, ts
        self.save_state()
        self.notify_be(t)

    def notify_be(self, t):
        fmt = lambda x: fmt_price(x, t["tick"])  # noqa: E731
        self.event("BREAKEVEN", side=tside(t), symbol=t["symbol"], new_stop=t["stop"],
                   trigger_r=self.cfg["be_trigger_r"])
        self.tg.send(f"STOP MOVED TO ENTRY [{t['mode']}]\n{t['symbol']} {tside(t).upper()}\n"
                     f"Profit reached {self.cfg['be_trigger_r']:g}R. Stop {fmt(t['orig_stop'])} -> {fmt(t['stop'])} (entry {fmt(t['entry'])}).\n"
                     + (f"Target unchanged at {fmt(t['tp'])}. " if t.get("tp") else "No target: trailing from "
                        f"{self.cfg['trail_start_r']:g}R. ") + "Worst case now is about the fees.")

    def move_stop_live(self, t):
        new, old = be_stop_price(t), t["stop"]
        try:
            self.api.place_order({"product_id": t["product_id"], "size": t["contracts"], "side": exit_side(t),
                                  "order_type": "market_order", "stop_order_type": "stop_loss_order",
                                  "stop_price": fmt_price(new, t["tick"]),
                                  "stop_trigger_method": self.cfg["stop_trigger"], "reduce_only": True})
        except Exception as ex:  # noqa: BLE001
            log.warning("breakeven stop order failed: %s", ex)
            if not t.get("be_warned"):
                t["be_warned"] = True
                self.tg.send(f"WARNING {t['symbol']}: could not move the stop to entry ({str(ex)[:150]}). "
                             f"The original stop is still active. Retrying.")
            return False
        time.sleep(1)
        t["stop"], t["be_moved"] = new, True
        sl, _ = self.find_protection(t)
        if sl:
            t["be_ts"] = int(time.time())
            self.save_state()
            self.notify_be(t)
            return True
        t["stop"], t["be_moved"] = old, False  # not confirmed; the old stop stays in force
        self.save_state()
        return False

    def manage_open_trades(self, now):
        for _, t in self.open_trades():
            try:
                if t.get("strategy") == "rsi1h" and t["mode"] == "PAPER":
                    self.manage_rsi1h_paper(t, now)
                    continue
                self.manage_open_trade(t, now)
            except Exception:  # noqa: BLE001
                log.exception("managing %s failed", t.get("symbol"))

    def manage_rsi1h_paper(self, t, now):
        """1H RSI paper trades: as manage_open_trade's paper path, plus limit exits for big TRAIL_STOP exits.
        The hard stop (3% against the entry) is simulated: the active stop is always the closer of the two, so it is
        hit as the stop; the exit is labelled HARD_STOP when the hard stop was the active one."""
        if t.get("pending_exit"):
            self.step_limit_exit(t, now)
            return
        res = self.check_exit_by_candles(t, now)
        self.save_state()
        if res:
            if res["reason"] == "STOP_LOSS" and t.get("stop_source") == "hard":
                res["reason"] = "HARD_STOP"
            if res["reason"] == "TRAIL_STOP" and self.start_limit_exit(t, res):
                return
            self.close_trade(t, res)

    def manage_open_trade(self, t, now):
        if t["mode"] == "PAPER":
            res = self.check_exit_by_candles(t, now)
            self.save_state()
            if res:
                self.close_trade(t, res)
            return
        pos = self.api.position(t["product_id"])
        if pos["size"] == 0:
            time.sleep(3)
            if self.api.position(t["product_id"])["size"] == 0:
                res = t.pop("forced", None) or self.live_exit_info(t)
                try:
                    self.api.cancel_all(t["product_id"])
                except Exception as ex:  # noqa: BLE001
                    log.warning("cleanup cancel_all failed: %s", ex)
                self.close_trade(t, res)
                return
        if t.get("trail"):
            self.manage_poc_live(t, now)
            return
        be_r = self.cfg["be_trigger_r"]
        if be_r > 0 and not t.get("be_moved") and risk0(t) > 0 and now - t.get("be_try", 0) >= 30:
            px = self.last_price(t["symbol"])
            fav = (t["entry"] - px) if tside(t) == "short" else (px - t["entry"])
            if px > 0 and fav >= be_r * risk0(t):
                t["be_try"] = now
                self.move_stop_live(t)
        if t.get("forced"):
            return  # a session-close / killswitch exit is in flight
        if now - t.get("last_protect_check", 0) >= 60:
            t["last_protect_check"] = now
            sl, _tp = self.find_protection(t)
            if not sl:
                self.tg.send(f"ALERT {t['symbol']}: stop-loss order missing on exchange, re-placing.")
                sl_ok, _ = self.place_protection(t)
                if not sl_ok:
                    self.emergency_close(t, "stop-loss missing and could not be re-placed")

    def close_trade(self, t, res):
        c = self.cfg
        exit_px = res["price"]
        if t.get("strategy") == "rsi1h" and self.rsi1h and not res.get("exec"):
            exit_px = self.rsi1h.exit_price(t, exit_px)            # RSI1H_SLIPPAGE_PCT against us (0 = none)
            self.rsi1h.on_close(t, res)                             # 30-min cooldown after a short
        n, cv = t["contracts"], t["cv"]
        direction = 1 if tside(t) == "long" else -1
        gross = n * cv * (exit_px - t["entry"]) * direction
        fees = c["fee_pct_per_side"] / 100.0 * n * cv * (t["entry"] + exit_px)
        net = gross - fees
        risk_px = risk0(t)
        r_mult = (exit_px - t["entry"]) * direction / risk_px if risk_px else 0.0
        gross, r_mult = gross + 0.0, r_mult + 0.0  # avoid printing "-0.0" on breakeven exits
        dk, mk = period_keys(res["ts"])
        eq, bk = t["equity"], t["book"]
        book = self.book(bk)
        self.bucket("day", dk, eq, bk)["pnl"] += net
        self.bucket("month", mk, eq, bk)["pnl"] += net
        if c["dry_run"]:
            self.state["paper_pnl"] = self.state.get("paper_pnl", 0.0) + net
        book["balance"] = round(book["balance"] + net, 4)
        book["trades"] += 1
        book["wins"] += int(net > 0)
        self.drop_trade(bk, t)
        self.save_state()
        fmt = lambda x: fmt_price(x, t["tick"])  # noqa: E731
        xx = self.exit_exec(t, res)
        self.event("EXIT", book=bk, side=tside(t), symbol=t["symbol"], reason=res["reason"], exit_price=exit_px, entry=t["entry"],
                   stop_loss=t["stop"], take_profit=t["tp"], gross_pnl_usd=round(gross, 4),
                   est_fees_usd=round(fees, 4), net_pnl_usd=round(net, 4), r_multiple=round(r_mult, 2),
                   estimated=res["estimated"], **({"execution": xx} if xx else {}))
        ee = t.get("exec") or {}
        new_file = not os.path.exists(self.trades_file)
        with open(self.trades_file, "a", newline="") as fh:
            w = csv.writer(fh)
            if new_file:
                w.writerow(["open_time_ist", "close_time_ist", "mode", "symbol", "side", "contracts", "entry",
                            "stop_loss", "take_profit", "exit_price", "exit_reason", "gross_pnl_usd",
                            "est_fees_usd", "net_pnl_usd", "net_pnl_inr", "r_multiple", "rsi_4h", "exit_estimated",
                            "ob_ratio", "strategy", "book_balance_usd"])
            w.writerow([ist(t["opened_ts"]), ist(res["ts"]), t["mode"], t["symbol"], tside(t).upper(), n,
                        fmt(t["entry"]), fmt(t["stop"]), fmt(t["tp"]) if t.get("tp") else "trail", fmt(exit_px), res["reason"],
                        round(gross, 4), round(fees, 4), round(net, 4), round(self.inr(net), 2),
                        round(r_mult, 2), "" if t.get("rsi") is None else t.get("rsi"), res["estimated"],
                        (t.get("ob") or {}).get("ratio", ""), bk, round(book["balance"], 4)]
                       + ((["", "", (t.get("extra") or {}).get("detail", "") if t.get("strategy") == "rsi1h" else ""]
                           + [ee.get(k, "") for k in ("entry_ref", "entry_avg", "entry_slip_pct", "entry_slip_usd",
                                                      "entry_children", "qty_intended", "qty_filled", "liq_capped")]
                           + [xx.get(k, "") for k in ("exit_ref", "exit_avg", "exit_slip_pct", "exit_slip_usd",
                                                      "exit_children", "exit_estimated", "exit_type", "exit_trigger",
                                                      "exit_trigger_slip_pct", "exit_trigger_slip_usd")]
                           + [ee.get(k, "") for k in ("entry_spread", "entry_spread_pct")])
                          if self.rsi1h else []))
        d = book["day"].get(dk, {})
        m = book["month"].get(mk, {})
        bal_line = (f"{bk} balance: ${book['balance']:.2f} (Rs {self.inr(book['balance']):,.0f})"
                    + (" [paper]" if c["dry_run"] else "") + "\n")
        if not c["dry_run"]:
            time.sleep(1)  # give the exchange a moment to settle the closed position
            bal_line += self.wallet_line()
        self.tg.send(
            f"TRADE CLOSED [{bk}] [{t['mode']}] - {res['reason'].replace('_', ' ')}\n"
            f"{t['symbol']} {tside(t).upper()} x{n}\n"
            f"Entry {fmt(t['entry'])} -> Exit {fmt(exit_px)}\n"
            f"Stop {fmt(t['stop'])} | Target {fmt(t['tp']) if t.get('tp') else 'none (trailing)'}\n"
            f"Gross P/L: ${gross:+.2f} (Rs {self.inr(gross):+.0f})\n"
            f"Est. fees: ${fees:.2f}\n"
            f"Net P/L: ${net:+.2f} (Rs {self.inr(net):+.0f})  [{r_mult:+.2f}R]\n"
            f"Duration: {human_dur(res['ts'] - t['opened_ts'])}\n"
            + (f"{execution.exit_text(xx, fmt)}\n" if xx else "") +
            f"{bal_line}"
            f"{bk} today: {d.get('trades', 0)} trades, ${d.get('pnl', 0):+.2f} | Month: {m.get('trades', 0)} trades, ${m.get('pnl', 0):+.2f}"
            + ("\n(Exit price estimated from candle data.)" if res["estimated"] else "")
        )

    # ---- telegram commands ----
    def status_text(self):
        c, st = self.cfg, self.state
        dk, _ = period_keys(time.time())
        lines = [f"Mode: {c['mode']}  |  Paused: {st['paused']}  |  Session now: {session_phase(c, time.time())}"]
        for n in self.book_names():
            b = self.book(n)
            d = b["day"].get(dk, {})
            trades = self.book_trades(n)
            tag = (" (paused)" if n in self.paused_books() else "") if n in self.enabled_books() else " (off)"
            lines.append(f"\n{n}{tag}: ${b['balance']:.2f} "
                         f"(start ${b['start']:.2f}) | today {d.get('trades', 0)} trades, ${d.get('pnl', 0):+.2f}"
                         + (f" | {len(trades)} open, free margin ${self.free_margin(n):.2f}" if len(trades) > 1 else ""))
            lines += [self.trade_status_line(t, b["balance"]) for t in trades] or ["  no position"]
        lines.append("")
        lines.append(self.wallet_line().strip() or "")
        lines.append(f"Short setups armed: {len(st['armed'])}")
        top = sorted(st["armed"].items(), key=lambda kv: -kv[1]["rsi"])[:5]
        lines += [f"  {s} RSI {v['rsi']}" for s, v in top]
        armed_l = st.get("armed_long", {})
        lines.append(f"Long setups armed: {len(armed_l)}")
        top = sorted(armed_l.items(), key=lambda kv: kv[1]["rsi"])[:5]
        lines += [f"  {s} RSI {v['rsi']}" for s, v in top]
        pc = st.get("poc") or {}
        if self.cfg["enable_poc"] and self.cfg["poc_paused"]:
            lines.append("POC RETURN: paused (POC_ENABLED=0) - no new orders or trades")
        elif self.cfg["enable_poc"]:
            lv = pc.get("levels", {})
            lines.append(f"POC levels: {len(lv)} (US session ending {ist(pc['made']) if pc.get('made') else '-'}) | "
                         f"armed long {sum(x['armed_long'] for x in lv.values())}, "
                         f"armed short {sum(x['armed_short'] for x in lv.values())}")
            top = [(k, x) for k, x in lv.items() if x["armed_long"] or x["armed_short"]][:8]
            lines += [f"  {k} POC {x['poc']:.6g} armed {'LONG' if x['armed_long'] else ''}"
                      f"{' SHORT' if x['armed_short'] else ''}" for k, x in top]
        bo = st.get("armed_bo", {})
        lines.append(f"Breakout ranges armed this hour: {len(bo)}")
        top = sorted(bo.items(), key=lambda kv: kv[1]["band_pct"])[:5]
        lines += [f"  {s} range high {v['range_high']} ({v['band_pct']}%)" for s, v in top]
        if self.rsi1h:
            lines += [""] + self.rsi1h.status_lines()
        if self.bj:
            lines += ["", self.bj.status_text()]
        return "\n".join(x for x in lines if x is not None)

    def trade_status_line(self, t, balance):
        f = lambda x: fmt_price(x, t["tick"])  # noqa: E731
        if t.get("strategy") == "rsi1h":
            loss = (self.rsi1h_loss_pct(t, balance))
            return (f"  {tside(t).upper()} {t['symbol']} size {t['contracts'] * t['cv']:g} entry {f(t['entry'])} | "
                    f"margin ${t.get('margin') or 0:.2f} | stop {f(t['stop'])}"
                    f"{' (at entry)' if t.get('be_moved') and not t.get('trail_r') else ''}"
                    f"{' (trail ' + str(t['trail_r'] - 1) + 'R)' if t.get('trail_r') else ''} | "
                    f"liq {f(t['liq_price']) if t.get('liq_price') else '-'} | loss if stopped {loss:+.1f}% of book")
        return (f"  {tside(t).upper()} {t['symbol']} x{t['contracts']} entry {f(t['entry'])} "
                f"SL {f(t['stop'])}{' (at entry)' if t.get('be_moved') else ''} "
                f"TP {f(t['tp']) if t.get('tp') else 'none (trailing)'}")

    def rsi1h_loss_pct(self, t, balance):
        """Loss if the current stop fills, incl. fees, as % of the book (negative = the stop locks in a profit)."""
        qty = t["contracts"] * t["cv"]
        move = (t["stop"] - t["entry"]) if tside(t) == "short" else (t["entry"] - t["stop"])
        usd = qty * move + self.cfg["fee_pct_per_side"] / 100.0 * qty * (t["entry"] + t["stop"])
        return usd / balance * 100 if balance > 0 else 0.0

    def report(self, day=None):
        """Per-strategy results: one New York day, or since the account was split."""
        lines, tot_pnl, tot_bal = [], 0.0, 0.0
        for n in self.book_names():
            b = self.book(n)
            if day:
                d = b["day"].get(day, {})
                trades, pnl, base = d.get("trades", 0), d.get("pnl", 0.0), d.get("start_equity") or b["balance"]
                extra = f"{trades} trades"
            else:
                pnl, base = b["balance"] - b["start"], b["start"]
                extra = f"{b['trades']} trades, {b['wins']} wins"
            tot_pnl += pnl
            tot_bal += b["balance"]
            lines.append(f"{n}: {extra} | P&L ${pnl:+.2f} ({pnl / base * 100 if base else 0:+.1f}%) "
                         f"| balance ${b['balance']:.2f}")
        lines.append(f"ALL {len(self.book_names())}: P&L ${tot_pnl:+.2f} (Rs {self.inr(tot_pnl):+,.0f}) | balance ${tot_bal:.2f}")
        if self.bj:
            lines.append(self.bj.report_line(day))
        return "\n".join(lines)

    def close_now(self, t, reason):
        """Close one position at market (session close / killswitch). Returns a short status text."""
        if t["mode"] == "PAPER":
            if t.get("pending_exit"):                # a limit exit in progress: the rest at market
                self.step_limit_exit(t, time.time(), force=reason)
                return f"{t['symbol']} closed (paper)."
            px = self.last_price(t["symbol"])
            self.close_trade(t, {"reason": reason, "price": px, "ts": int(time.time()), "estimated": True})
            return f"{t['symbol']} closed (paper)."
        if t.get("forced"):
            return f"{t['symbol']} close already sent."
        try:
            self.api.cancel_all(t["product_id"])
            self.api.place_order({"product_id": t["product_id"], "size": t["contracts"], "side": exit_side(t),
                                  "order_type": "market_order", "reduce_only": True})
        except Exception as ex:  # noqa: BLE001
            return f"closing {t['symbol']} failed: {str(ex)[:200]}. Close it manually on {self.cfg['exchange_name']}."
        t["forced"] = {"reason": reason, "price": 0, "ts": int(time.time()), "estimated": True}
        time.sleep(2)
        t["forced"]["price"] = self.last_price(t["symbol"])
        self.save_state()
        return f"{t['symbol']} close order sent; summary follows when the position is confirmed flat."

    def kill_switch(self):
        self.state["paused"] = True
        self.save_state()
        open_ = self.open_trades()
        if not open_:
            return "Paused. No open positions."
        return "Paused.\n" + "\n".join(f"[{n}] " + self.close_now(t, "KILLSWITCH") for n, t in open_)

    def session_tasks(self, now):
        """16:00 New York: close everything, then send the day summary once."""
        c = self.cfg
        if c["session"] != "us":
            d = datetime.fromtimestamp(now, NY)
            today = d.strftime("%Y-%m-%d")
            if self.state.get("summary_sent") != today:
                prev = self.state.get("summary_sent")
                self.state["summary_sent"] = today
                self.save_state()
                if prev:
                    self.tg.send(f"Day summary {prev} (New York date)\n" + self.report(prev)
                                 + f"\n\nSince {self.state['books_since'][:10]}\n" + self.report())
            return
        if session_phase(c, now) != "closed":
            return
        open_ = [(n, t) for n, t in self.open_trades() if not t.get("forced")]
        if open_ and now - self.last_session_close_try >= 30:
            self.last_session_close_try = now
            for n, t in open_:
                msg = self.close_now(t, "SESSION_CLOSE")
                if "failed" in msg:
                    self.tg.send(f"[{n}] session close: {msg}")
            return
        d = datetime.fromtimestamp(now, NY)
        day = d.strftime("%Y-%m-%d")
        if d.weekday() < 5 and d.hour * 60 + d.minute >= hhmm(c["session_close"]) \
                and self.state.get("summary_sent") != day and not self.open_trades():
            self.state["summary_sent"] = day
            self.save_state()
            self.tg.send(f"Day summary {day} (US session)\n" + self.report(day)
                         + f"\n\nSince {self.state['books_since'][:10]}\n" + self.report()
                         + ("\n" + self.wallet_line().strip() if not c["dry_run"] else ""))

    def handle_telegram(self):
        for text in self.tg.poll(self.state):
            cmd = text.split()[0].lower().split("@")[0]
            if cmd == "/status":
                self.tg.send(self.status_text())
            elif cmd == "/summary":
                self.tg.send(f"Results since {self.state['books_since'][:10]} (account split 3 ways)\n" + self.report())
            elif cmd == "/pause":
                self.state["paused"] = True
                self.tg.send("Paused: no new entries. Open positions keep their stops and targets.")
            elif cmd == "/resume":
                self.state["paused"] = False
                self.tg.send("Resumed.")
            elif cmd == "/killswitch":
                self.tg.send(self.kill_switch())
            else:
                self.tg.send("Commands: /status /summary /pause /resume /killswitch")
        self.save_state()

    # ---- BOOK JUMP (bookjump.py) ----
    def bookjump_setup(self):
        """ENABLE_BOOKJUMP=1: create the BOOK JUMP runner (BOOK JUMP + BJ ABSORB books, paper mode on Binance only).
        Returns the start-up line(s)."""
        c = self.cfg
        if not c["enable_bookjump"]:
            return ""
        why = bookjump.refuse_reason(c)
        if why:
            self.event("BJ_REFUSED", reason=why)
            return f"\nBOOK JUMP: NOT started - it only runs in paper mode on Binance ({why})."
        try:
            books = self.state["books"]
            self.bj = bookjump.BookJump(c, self.tg, self.event, self.in_us_session,
                                        Binance("https://fapi.binance.com", "", "", c["ua"]), c["data_dir"],
                                        lambda: bool(self.state.get("paused")),
                                        lambda name=bookjump.BOOK: books.get(name),
                                        lambda snap, name=bookjump.BOOK: books.__setitem__(name, snap))
            return "\n" + self.bj.prepare()
        except Exception as ex:  # noqa: BLE001
            self.bj = None
            log.exception("BOOK JUMP setup failed")
            return f"\nBOOK JUMP: NOT started - {str(ex)[:200]}"

    # ---- main loop ----
    def startup(self):
        c = self.cfg
        if not c["dry_run"] and not c["testnet"] and c["confirm_live"] != "YES":
            sys.exit("Refusing to trade real money: set CONFIRM_LIVE=YES in .env if you really mean it.")
        if not c["dry_run"] and not (c["api_key"] and c["api_secret"]):
            sys.exit(f"{c['exchange_name'].upper()}_API_KEY / {c['exchange_name'].upper()}_API_SECRET missing in .env")
        prods = self.get_products(force=True)
        if not prods:
            sys.exit("No live products found. Check DELTA_ENV / BINANCE_ENV and network access.")
        self.switch_exchange()
        eq = self.equity_usd()
        if eq <= 0 and c["dry_run"] and self.state.get("books"):
            # paper with books (26 Nov 2026): each book trades its own balance; the old account figure
            # (START_CAPITAL_INR + all paper P&L, retired books included) can go below 0 without meaning anything
            log.warning("paper account figure is $%.2f (START_CAPITAL_INR + all paper P&L); the books trade their own "
                        "balances, starting anyway", eq)
        elif eq <= 0:
            sys.exit("Could not determine account equity (zero balance?). Set TRADING_CAPITAL_USD in .env or fund the account.")
        if self.rsi1h:
            self.rsi1h.ensure_books()                    # $100 each, before init_books (which would fund from POC)
            self.rsi1h.ensure_trades_header()
            self.rsi1h.ensure_cand_header()
        self.init_books()
        self.state["paused_books"] = self.paused_books()        # read by mintoro-api (dashboard shows "paused")
        self.save_state()
        if not c["dry_run"]:
            try:
                held = {t["product_id"] for _, t in self.open_trades()}
                for p in self.api.all_positions():
                    if int(fnum(p.get("size"))) != 0 and int(fnum(p.get("product_id"))) not in held:
                        self.tg.send(f"WARNING: unmanaged open position found on {p.get('product_symbol', p.get('product_id'))}. The bot will not touch it.")
            except Exception as ex:  # noqa: BLE001
                log.warning("position reconciliation skipped: %s", ex)
        bj_line = self.bookjump_setup()
        if self.rsi1h:
            bj_line += self.rsi1h.startup_line()
        elif self.rsi1h_refused:
            bj_line += f"\n1H RSI books: NOT started - paper mode on Binance only ({self.rsi1h_refused})."
        if os.environ.get("ENABLE_BOOKJUMP", "").strip() == "1" and not c["enable_bookjump"]:
            log.info("ENABLE_BOOKJUMP=1 in .env is ignored: BOOK JUMP is retired (BJ_ENABLED=1 would start it)")
        log.info("Books running: %s%s", ", ".join(self.enabled_books()),
                 f" (paused: {', '.join(self.paused_books())})" if self.paused_books() else "")
        self.tg.send(
            f"Bot started [{c['mode']}]\n{len(prods)} instruments, account ${eq:.2f} (Rs {self.inr(eq):.0f})\n"
            + "\n".join(f"  {n}: ${self.book(n)['balance']:.2f}" + (" (paused)" if n in self.paused_books() else "")
                        for n in self.book_names()) + bj_line + "\n"
            + self.strategy_desc() + "\nOne position at a time per strategy; a coin is held by one strategy at most"
            + (f" | stop to entry at +{c['be_trigger_r']:g}R" if c["be_trigger_r"] > 0 else "")
            + (f" | order book {c['ob_min_ratio']:g}x within {c['ob_depth_pct']:g}%" if c["ob_filter"] else "") + "\n"
            f"Leverage {c['leverage']:g}x | "
            + (f"margin {c['margin_pct']:g}% of account per trade" if c["margin_pct"] > 0 else f"risk {c['risk_pct']}% per trade")
            + f" | stop: candle or {c['margin_stop_pct']:g}% of margin, whichever is closer\n"
            f"Limits: daily loss " + (f"{c['daily_loss_pct']:g}% of the day's starting equity" if c["daily_loss_pct"] > 0 else "off")
            + (f", trades/day {c['max_trades_day']}" if c["max_trades_day"] > 0 else "")
            + (f", trades/month {c['max_trades_month']}" if c["max_trades_month"] > 0 else "")
            + (f", monthly loss {c['monthly_loss_pct']:g}%" if c["monthly_loss_pct"] > 0 else "") + "\n"
            "Commands: /status /summary /pause /resume /killswitch"
        )
        self.event("STARTED", mode=c["mode"], instruments=len(prods), equity_usd=round(eq, 2),
                   short=c["enable_short"], long=c["enable_long"], breakout=c["enable_breakout"],
                   session=c["session"], books={n: self.book(n)["balance"] for n in self.book_names()})
        if self.bj:
            self.bj.start()

    def tick(self, now):
        if now - getattr(self, "_last_tg", 0) >= 3:
            self._last_tg = now
            self.handle_telegram()
        if self.cfg["enable_short"] or self.cfg["enable_long"]:
            try:
                self.tick_rsi(now)
            except Exception:  # noqa: BLE001
                log.exception("RSI strategies tick failed")
        if self.cfg["enable_breakout"] or self.cfg["enable_breakdown"]:
            self.tick_breakout(now)
        if self.cfg["enable_poc"]:
            try:
                if self.cfg["poc_paused"]:
                    self.poc_pause_step(now)
                else:
                    self.tick_poc(now)
            except Exception:  # noqa: BLE001
                log.exception("POC tick failed")
        if self.rsi1h:
            try:
                self.rsi1h.tick(now)
            except Exception:  # noqa: BLE001
                log.exception("1H RSI tick failed")
        if now - getattr(self, "_last_manage", 0) >= self.cfg["manage_interval"]:
            self._last_manage = now
            self.manage_open_trades(now)
        self.session_tasks(now)
        if now - getattr(self, "_last_sweep", 0) >= 60:
            self._last_sweep = now
            try:
                self.sweep_retired()
            except Exception:  # noqa: BLE001
                log.exception("sweep failed")

    def tick_rsi(self, now):
        b4 = int(now) // PERIOD[ARM_TF] * PERIOD[ARM_TF]
        if b4 != self.state["last_4h"] and now >= b4 + 8 and now >= self.next_scan_try:
            try:
                self.scan_4h(now, b4)
            except Exception:  # noqa: BLE001
                self.next_scan_try = now + 30
                raise
        b5 = int(now) // PERIOD["5m"] * PERIOD["5m"]
        armed_l = self.state.get("armed_long", {})
        if (self.state["armed"] or armed_l) and b5 + 3 <= now <= b5 + 50 and now - self.last_eval_try >= 3:
            if self.eval_boundary != b5:
                self.eval_boundary, self.eval_done = b5, set()
            entry_ok = session_phase(self.cfg, now) == "entry" and not self.state["paused"]
            pend_s = ([s for s in self.state["armed"] if f"short:{s}" not in self.eval_done]
                      if entry_ok and self.book_free(f"{ARM} RSI SHORT") else [])
            pend_l = ([s for s in armed_l if f"long:{s}" not in self.eval_done]
                      if entry_ok and self.book_free(f"{ARM} RSI LONG") and b5 % PERIOD["15m"] == 0 else [])
            if pend_s or pend_l:
                self.last_eval_try = now
                self.eval_signals(now, b5, pend_s, pend_l)

    def run(self):
        self.startup()
        while True:
            try:
                self.tick(time.time())
            except KeyboardInterrupt:
                raise
            except Exception as ex:  # noqa: BLE001
                log.exception("loop error")
                if time.time() - self.last_err_notify > 600:
                    self.last_err_notify = time.time()
                    self.tg.send(f"Bot error (still running): {str(ex)[:300]}")
            time.sleep(2)


# --------------------------------------------------------------------------
# CLI helpers
# --------------------------------------------------------------------------
def setup_logging():
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = RotatingFileHandler(os.path.join(BASE_DIR, "bot.log"), maxBytes=2_000_000, backupCount=3)
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    log.setLevel(logging.INFO)
    log.addHandler(fh)
    log.addHandler(sh)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def selftest(cfg):
    print(f"Mode: {cfg['mode']}   trading host: {cfg['base_url']}   data host: {cfg['data_base_url']}")
    bot = Bot(cfg)
    ok = True
    try:
        prods = bot.get_products(force=True)
        print(f"[OK] products: {len(prods)} live ({','.join(cfg['contract_types'])})")
        for p in prods[:3]:
            print(f"     {p['symbol']}: id={p['id']} tick={p['tick']} contract_value={p['cv']}")
    except Exception as ex:  # noqa: BLE001
        ok = False
        print(f"[FAIL] products: {ex}")
        prods = []
    if prods:
        sym = prods[0]["symbol"]
        try:
            c4 = closed_only(bot.data.candles(sym, ARM_TF, 150), PERIOD[ARM_TF], time.time())
            c5 = closed_only(bot.data.candles(sym, "5m", 80), PERIOD["5m"], time.time())
            r = rsi_wilder([x["close"] for x in c4], cfg["rsi_len"])
            res = detect_setup(c5, cfg["ema_len"])
            print(f"[OK] candles {sym}: {len(c4)} x 4h (RSI {r:.1f}), {len(c5)} x 5m, latest 4h open {ist(c4[-1]['time'])}"
                  if r is not None and c4 else f"[WARN] candles {sym}: not enough data")
            if res:
                print(f"     last 5m pair: setup={res['setup']} signal={res['signal']}")
        except Exception as ex:  # noqa: BLE001
            ok = False
            print(f"[FAIL] candles: {ex}")
    if prods:
        sym = prods[0]["symbol"]
        try:
            bids, asks = bot.data.orderbook(sym)
            ob = ob_imbalance(bids, asks, cfg["ob_depth_pct"])
            if ob:
                print(f"[OK] order book {sym}: {len(bids)} bid / {len(asks)} ask levels, within "
                      f"{cfg['ob_depth_pct']:g}%: bids {ob['bid_qty']:,.0f} vs asks {ob['ask_qty']:,.0f} "
                      f"(long ratio {ob_ratio(ob, 'long'):.2f}x), spread {ob['spread_pct']:.3f}%")
            else:
                print(f"[WARN] order book {sym}: empty ({len(bids)} bids, {len(asks)} asks)")
        except Exception as ex:  # noqa: BLE001
            print(f"[WARN] order book {sym}: {ex} (trades would go ahead without the check)")
    if cfg["api_key"] and cfg["api_secret"]:
        try:
            bal = bot.api.balances()
            print("[OK] authenticated. Wallet balances:")
            for b in bal:
                print(f"     {b.get('asset_symbol')}: balance={b.get('balance')} available={b.get('available_balance')}")
            print(f"     Equity used for sizing: ${bot.equity_usd():.2f}")
        except Exception as ex:  # noqa: BLE001
            ok = False
            print(f"[FAIL] private API (check key, secret, IP whitelist, DELTA_ENV): {ex}")
    else:
        print("[SKIP] no API key set; paper mode needs none")
        print(f"     Paper equity: ${bot.equity_usd():.2f}")
    if prods:
        p = prods[0]
        try:
            px = bot.last_price(p["symbol"])
            stop = round_tick(px * 1.004, p["tick"], "up")
            if cfg["margin_stop_pct"] > 0:
                stop = min(stop, round_tick(px * (1 + cfg["margin_stop_pct"] / 100.0 / cfg["leverage"]), p["tick"], "down"))
            n, info = size_trade(cfg, bot.equity_usd(), px, stop, p["cv"])
            print(f"[INFO] sizing example {p['symbol']}: price {px}, stop {stop}, contracts {n}, "
                  f"margin ${info.get('margin', 0):.2f}, notional ${info.get('notional', 0):.0f}, "
                  f"loss if stopped ${info.get('risk_usd', 0):.2f} ({info.get('risk_usd', 0) / max(bot.equity_usd(), 1e-9) * 100:.0f}% of account)")
        except Exception as ex:  # noqa: BLE001
            print(f"[WARN] sizing example skipped: {ex}")
    if bot.tg.enabled:
        bot.tg.send(f"Self-test message from the Delta bot [{cfg['mode']}]. If you can read this, Telegram works.")
        print("[OK] Telegram message sent")
    else:
        print("[SKIP] Telegram not configured (need TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)")
    print("RESULT:", "all checks passed" if ok else "some checks failed, see above")


def find_chat_id(cfg):
    if not cfg["tg_token"]:
        sys.exit("TELEGRAM_BOT_TOKEN missing in .env")
    r = requests.get(f"https://api.telegram.org/bot{cfg['tg_token']}/getUpdates", timeout=10).json()
    seen = {}
    for u in r.get("result") or []:
        ch = (u.get("message") or {}).get("chat") or {}
        if ch.get("id"):
            seen[ch["id"]] = ch.get("first_name") or ch.get("title") or ""
    if not seen:
        print("No messages found. Open your bot in Telegram, send it any message (e.g. hi), then run this again.")
    for cid, name in seen.items():
        print(f"chat id: {cid}   ({name})   -> add  TELEGRAM_CHAT_ID={cid}  to .env")


def main():
    load_env(os.path.join(BASE_DIR, ".env"))
    setup_logging()
    cfg = make_cfg()
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--find-chat-id", action="store_true")
    ap.add_argument("--telegram-test", action="store_true")
    a = ap.parse_args()
    if a.find_chat_id:
        return find_chat_id(cfg)
    if a.telegram_test:
        Telegram(cfg["tg_token"], cfg["tg_chat"]).send("Test message from the Delta bot.")
        return None
    if a.selftest:
        return selftest(cfg)
    try:
        Bot(cfg).run()
    except KeyboardInterrupt:
        log.info("stopped by user")
    return None


if __name__ == "__main__":
    main()
