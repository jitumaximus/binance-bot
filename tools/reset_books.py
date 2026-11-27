#!/usr/bin/env python3
"""Reset the paper books (28 Nov 2026): archive trades.csv, events.jsonl, candidates.csv and state.json into
data/archive/<IST date>/ (copies; nothing is deleted), then start every book in BOOKS at START with fresh stats.

  venv/bin/python tools/reset_books.py --dir ~/binance-bot --yes

Refuses while the binance-bot service is running or while any book holds an open trade (it never closes trades).
state.json keeps everything that is not a statistic (armed setups, universes, cooldowns, Telegram offset, ...);
retired books (POC RETURN, BOOK JUMP, BJ ABSORB, EMA20 PULLBACK) leave the live state and live only in the archive.
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


def reset(d, now=None, trade_header=None, cand_header=None):
    """Archive and reset the files in folder d. Returns the archive folder."""
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
    state["books"] = {n: {"start": START, "balance": START, "open_trade": None, "day": {}, "month": {},
                          "trades": 0, "wins": 0, "since": since} for n in BOOKS}
    state.update(paper_pnl=0.0, day={}, month={}, books_since=since, books_start_total=START * len(BOOKS),
                 reset_at=since, reset_archive=os.path.relpath(arch, d), paused_books=[])
    write_atomic(os.path.join(d, "state.json"), json.dumps(state, indent=1))
    write_atomic(os.path.join(d, "events.jsonl"), "")
    if trade_header:
        write_atomic(os.path.join(d, "trades.csv"), ",".join(trade_header) + "\n")
    if cand_header:
        write_atomic(os.path.join(d, "candidates.csv"), ",".join(cand_header) + "\n")
    return arch


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--yes", action="store_true")
    ap.add_argument("--service", default="binance-bot")
    a = ap.parse_args()
    d = os.path.abspath(os.path.expanduser(a.dir))
    sys.path.insert(0, d)
    import rsi1h  # noqa: E402  (the headers the bot writes)
    if subprocess.run(["systemctl", "is-active", "--quiet", a.service]).returncode == 0:
        raise SystemExit(f"{a.service} is running: stop it first")
    if not a.yes:
        raise SystemExit("add --yes")
    arch = reset(d, trade_header=rsi1h.Rsi1h.TRADE_HEAD + rsi1h.Rsi1h.TRADE_TAIL, cand_header=rsi1h.CAND_COLS)
    print(f"archived {', '.join(FILES)} -> {arch}; {len(BOOKS)} books at ${START:.0f}")


if __name__ == "__main__":
    main()
