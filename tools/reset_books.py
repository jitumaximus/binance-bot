#!/usr/bin/env python3
"""Reset the paper books (28 Nov 2026): archive trades.csv, events.jsonl, candidates.csv and state.json into
data/archive/<IST date>/ (copies; nothing is deleted), then start every book in BOOKS at START with fresh stats.
Retired books stay in state.json with their stats (the dashboard's retired section); an old book whose name a new
book reuses is renamed (RENAME, e.g. "1H RSI LONG" -> "1H RSI LONG (v1)"), in state.json and in trades.csv's
strategy column, so its history stays its own. events.jsonl and candidates.csv start empty.

  venv/bin/python tools/reset_books.py --dir ~/binance-bot --yes

Refuses while the binance-bot service is running or while any book holds an open trade (it never closes trades).
state.json keeps everything that is not a statistic (armed setups, universes, cooldowns, Telegram offset, ...);
"""
import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")
BOOKS = ("1H RSI LONG", "1H RSI SHORT", "EMA20 LONG", "EMA20 SHORT", "4H RETRACE LONG", "4H RETRACE SHORT")
START = 100.0
FILES = ("trades.csv", "events.jsonl", "candidates.csv", "state.json")
RENAME = {"1H RSI LONG": "1H RSI LONG (v1)", "1H RSI SHORT": "1H RSI SHORT (v1)"}


def open_trades(state):
    return [(n, t.get("symbol")) for n, b in (state.get("books") or {}).items()
            for t in ([b["open_trade"]] if b.get("open_trade") else []) + list(b.get("more_trades") or [])]


def write_atomic(path, text):
    tmp = path + ".tmp"
    with open(tmp, "w", newline="") as fh:
        fh.write(text)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def reset(d, now=None, trade_header=None, cand_header=None, again=False):
    """Archive and reset the files in folder d. Returns the archive folder.
    again: reset the six running books once more (no renaming; their trades.csv rows are removed - the retired books
    keep theirs) and clear the armed EMA20 / 4H RETRACE setups."""
    now = now or datetime.now(IST)
    with open(os.path.join(d, "state.json")) as fh:
        state = json.load(fh)
    left = open_trades(state)
    if left:
        raise SystemExit(f"open trades, not resetting: {left}")
    arch = os.path.join(d, "data", "archive", now.strftime("%Y-%m-%d"))
    if os.path.exists(arch):
        arch += now.strftime("_%H%M%S")
    os.makedirs(arch)
    for f in FILES:
        p = os.path.join(d, f)
        if os.path.exists(p):
            shutil.copy2(p, os.path.join(arch, f))
    since = now.isoformat(timespec="seconds")
    ren = {} if again else RENAME
    old = {ren.get(n, n): b for n, b in (state.get("books") or {}).items() if not (again and n in BOOKS)}
    for n, b in old.items():
        b.setdefault("since", state.get("books_since") if n == "POC RETURN" else None)
        b["retired_at"] = b.get("retired_at") or since
    state["books"] = dict(old, **{n: {"start": START, "balance": START, "open_trade": None, "day": {}, "month": {},
                                      "trades": 0, "wins": 0, "since": since} for n in BOOKS})
    state.update(paper_pnl=0.0, day={}, month={}, books_since=since, books_start_total=START * len(BOOKS),
                 reset_at=since, reset_archive=os.path.relpath(arch, d), paused_books=[])
    if again:
        for k in ("ema20", "retrace4h"):
            if isinstance(state.get(k), dict):
                state[k].update(armed={}, used={})
    write_atomic(os.path.join(d, "state.json"), json.dumps(state, indent=1))
    write_atomic(os.path.join(d, "events.jsonl"), "")
    tp = os.path.join(d, "trades.csv")
    if os.path.exists(tp):                     # renamed books keep their rows under the new name
        with open(tp, newline="") as fh:
            rows = list(csv.reader(fh))
        if rows and "strategy" in rows[0]:
            k = rows[0].index("strategy")
            if again:
                rows = rows[:1] + [r for r in rows[1:] if not (len(r) > k and r[k] in BOOKS)]
            for r in rows[1:]:
                if len(r) > k and r[k] in ren:
                    r[k] = ren[r[k]]
            tmp = tp + ".tmp"
            with open(tmp, "w", newline="") as fh:
                csv.writer(fh).writerows(rows)
            os.replace(tmp, tp)
    elif trade_header:
        write_atomic(tp, ",".join(trade_header) + "\n")
    if cand_header:
        write_atomic(os.path.join(d, "candidates.csv"), ",".join(cand_header) + "\n")
    return arch


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--yes", action="store_true")
    ap.add_argument("--service", default="binance-bot")
    ap.add_argument("--again", action="store_true", help="reset the six books again (no renaming, drop their trades)")
    a = ap.parse_args()
    d = os.path.abspath(os.path.expanduser(a.dir))
    sys.path.insert(0, d)
    import rsi1h  # noqa: E402  (the headers the bot writes)
    if subprocess.run(["systemctl", "is-active", "--quiet", a.service]).returncode == 0:
        raise SystemExit(f"{a.service} is running: stop it first")
    if not a.yes:
        raise SystemExit("add --yes")
    arch = reset(d, trade_header=rsi1h.Rsi1h.TRADE_HEAD + rsi1h.Rsi1h.TRADE_TAIL, cand_header=rsi1h.CAND_COLS,
                 again=a.again)
    print(f"archived {', '.join(FILES)} -> {arch}; {len(BOOKS)} books at ${START:.0f}")


if __name__ == "__main__":
    main()
