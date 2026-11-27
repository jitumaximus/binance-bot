"""EMA20 PULLBACK setup rules on closed 5m candles - pure functions, no I/O (used live by ema20.py and by the dry run).

LONG (SHORT is the exact mirror):
 1. Alert candle: green, crosses the 20 EMA from below (its open, or the previous close, below the EMA) and closes
    above the EMA.
 2. Pullback: of the next 2 candles at least one is red and neither closes below the 20 EMA. Trigger = the red
    candle's high (the higher high if both are red).
 3. From the 3rd candle on, armed: enter as soon as price trades above the trigger. Cancelled if a 5m candle closes
    below the 20 EMA first, or after max_wait candles without a breach.
 4. 1R = entry - lowest low of the alert and pullback candles, at least min_r_pct of the price.
EMA: 20-period EMA of the 5m closes, seeded with the first close; the first `warm` candles are warm-up only.
"""

EMA_LEN = 20
WARM = 40           # candles before the first alert can count (the seed's weight is < 2% after 40 candles)


def ema_series(values, n=EMA_LEN):
    k, out, e = 2.0 / (n + 1), [], None
    for v in values:
        e = v if e is None else v * k + e * (1 - k)
        out.append(e)
    return out


def _green(c):
    return c["close"] > c["open"]


def _red(c):
    return c["close"] < c["open"]


def is_alert(cands, ema, i, side):
    """Candle i crosses the EMA in the trade's direction and closes beyond it."""
    c, e = cands[i], ema[i]
    prev_beyond = i > 0 and ((cands[i - 1]["close"] < ema[i - 1]) if side == "long" else (cands[i - 1]["close"] > ema[i - 1]))
    if side == "long":
        return _green(c) and c["close"] > e and (c["open"] < e or prev_beyond)
    return _red(c) and c["close"] < e and (c["open"] > e or prev_beyond)


def closes_against(c, e, side):
    """A close on the wrong side of the EMA: below it for a long, above it for a short."""
    return c["close"] < e if side == "long" else c["close"] > e


def scan_setups(cands, side, max_wait=12, min_r_pct=0.002, n=EMA_LEN, warm=WARM):
    """Walk ascending CLOSED 5m candles and return every setup, oldest first. Each setup:
      alert_i, alert_time, pull (2 candle times), status, trigger, low (lowest low / highest high), r (price units, at
      the trigger as a stand-in for the entry), ema (at the alert), armed_i / armed_time (close of the 2nd pullback
      candle; armed from the next candle), end_i / end_time, reason.
    status: 'pullback' (waiting for the 2nd pullback candle), 'failed' (no valid pullback), 'armed' (still waiting at
    the last candle), 'breached' (a later candle traded beyond the trigger: an entry), 'cancelled' (closed against the
    EMA first), 'expired' (max_wait candles without a breach).
    A breach and a close against the EMA in the same candle count as a breach (the live price crosses first).
    While a setup is pending or armed, no new alert is taken for that side."""
    if len(cands) <= warm:
        return []
    ema = ema_series([c["close"] for c in cands], n)
    out, cur = [], None
    for i in range(warm, len(cands)):
        c, e = cands[i], ema[i]
        if cur is not None:
            k = i - cur["alert_i"]
            if cur["status"] == "pullback":
                cur["pull"].append(c["time"])
                if closes_against(c, e, side):
                    cur.update(status="failed", reason="pullback candle closed against the EMA", end_i=i, end_time=c["time"])
                elif k == 2:
                    pb = cands[i - 1:i + 1]
                    against = [x for x in pb if (_red(x) if side == "long" else _green(x))]
                    if not against:
                        cur.update(status="failed", reason="no " + ("red" if side == "long" else "green") +
                                   " pullback candle", end_i=i, end_time=c["time"])
                    else:
                        trig = max(x["high"] for x in against) if side == "long" else min(x["low"] for x in against)
                        grp = cands[cur["alert_i"]:i + 1]
                        low = min(x["low"] for x in grp) if side == "long" else max(x["high"] for x in grp)
                        r = max(abs(trig - low), trig * min_r_pct)
                        cur.update(status="armed", trigger=trig, low=low, r=r, armed_i=i, armed_time=c["time"])
                if cur["status"] != "failed":
                    continue
            elif cur["status"] == "armed":
                hit = c["high"] > cur["trigger"] if side == "long" else c["low"] < cur["trigger"]
                if hit:
                    cur.update(status="breached", end_i=i, end_time=c["time"], reason="price beyond the trigger")
                elif closes_against(c, e, side):
                    cur.update(status="cancelled", end_i=i, end_time=c["time"],
                               reason="5m close " + ("below" if side == "long" else "above") + " the 20 EMA")
                elif i - cur["armed_i"] >= max_wait:
                    cur.update(status="expired", end_i=i, end_time=c["time"],
                               reason=f"{max_wait} candles without a breach")
                if cur["status"] == "armed":
                    continue
            cur = None
            # the candle that ended a setup may itself be a new alert (e.g. a cancel close is never an alert, but a
            # failed pullback or an expiry candle can be)
        if is_alert(cands, ema, i, side):
            cur = {"side": side, "alert_i": i, "alert_time": c["time"], "pull": [], "status": "pullback",
                   "ema": e, "alert_open": c["open"], "alert_close": c["close"]}
            out.append(cur)
    return out


def ema_at_last(cands, n=EMA_LEN):
    """EMA of the last closed candle (None when too short)."""
    if len(cands) < n:
        return None
    return ema_series([c["close"] for c in cands], n)[-1]
