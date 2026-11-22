# binance-bot

Paper trading bot on Binance USDT-M perpetuals (systemd service `binance-bot`, folder `~/binance-bot`).
`bot.py` holds the strategies, accounting, Telegram and logging; its docstring describes POC RETURN and the
other (switched-off) strategies in detail. `bookjump.py` is the BOOK JUMP strategy.

| File | What |
|---|---|
| `bot.py` | main bot: POC RETURN (+ RSI / breakout strategies, off in this deployment), books, Telegram, logs |
| `bookjump.py` | BOOK JUMP order-book penny-jumping, paper only, started by `bot.py` when `ENABLE_BOOKJUMP=1` |
| `.env` | settings and keys (never committed); see `.env.example` for every setting and its default |
| `trades.csv` | every closed trade, all strategies (`strategy` column = book name; `session` column for BOOK JUMP) |
| `events.jsonl` | event log (one JSON object per line) |
| `walls.csv` | BOOK JUMP: every wall seen |
| `state.json` | bot state; `books` holds every book incl. `BOOK JUMP` (balance, start, IST day stats by session, open positions, kill-switch state) |

Run: `venv/bin/python bot.py` (normally via systemd). Checks: `venv/bin/python bot.py --selftest`.
Tests: `venv/bin/python -m unittest discover -s tests -v` and `venv/bin/python -m pyflakes bot.py bookjump.py tests`.

Telegram commands: `/status /summary /pause /resume /killswitch`.

## BOOK JUMP (order-book penny-jumping, paper only)

Runs 24/7 next to POC RETURN, in its own threads, with its own book (`state.json` → `books["BOOK JUMP"]`, never
swept into or used to fund other books): POC is not affected
(`tests/test_poc_unchanged.py` checks that every POC function is identical to the version before BOOK JUMP).

- **Start**: `ENABLE_BOOKJUMP=1`. Refuses to start unless `DRY_RUN=1` and `EXCHANGE=binance`; the start-up Telegram
  message says whether it started and why not. Needs `websocket-client` (in `requirements.txt`).
- **Universe** (every `BJ_UNIVERSE_HOURS`): up to `BJ_MAX_SYMBOLS` USDT perpetuals with tick/price between
  `BJ_TICK_MIN_PCT` and `BJ_TICK_MAX_PCT` % and 24h quote volume > `BJ_MIN_VOL_USDT`, highest volume first.
  Symbols that drop out stay watched while BOOK JUMP has an order or position on them.
- **Data**: Binance futures WebSocket. Binance serves depth and trades on separate routes, so there are two
  connections: `wss://fstream.binance.com/public/stream` (`<sym>@depth20@100ms`) and `/market/stream`
  (`<sym>@aggTrade`). Each reconnects with backoff (1 s doubling to 60 s). No message on either for
  `BJ_STALE_SEC`: resting orders cancelled, no new entries, one Telegram note; not back within `BJ_STALE_KILL_SEC`:
  kill switch.
- **Wall**: on every book update, average level size (USDT) over the top 5 bids + top 5 asks. A level among the top 5
  is a wall if size >= `BJ_WALL_MULT` x average and (`BJ_WALL_RULE=and`, or `or`) size >= `BJ_WALL_MIN_USDT`.
  It stays a wall while its level (anywhere in the 20 visible levels) still passes the test; if price moves so far
  that the level is out of view it is kept as is. It must persist `BJ_PERSIST_SEC` at the same price to count as
  "seen" (logged) and tradable. Note: the average includes the wall, so 5x means the wall is at least as big as
  the other 9 levels together.
- **Entry**: paper post-only order one tick in front of the wall (bid wall: buy at wall + 1 tick, must be below the
  best ask; ask wall: sell at wall - 1 tick, above the best bid). One order/position per symbol, at most
  `BJ_MAX_OPEN` orders + positions, one attempt per wall. Size: `BJ_RISK_PCT` of the book balance /
  (entry - (wall - 1 tick)), notional capped at `BJ_MAX_NOTIONAL_X` x balance, rounded down to the step, skipped
  below min notional. Cancelled when the wall is pulled (lost > half its size) or drops below the wall test, after
  `BJ_ORDER_TIMEOUT_SEC`, or when price moves more than `BJ_AWAY_TICKS` ticks away.
- **Paper fill**: queue ahead = size already resting at our price when placed. Filled when a trade prints through
  our price, or when aggressive trades at our price (sells for a buy) exceed that queue. Filled at our price.
- **Exit** (all at the next trade price after the trigger, as a market order):
  before breakeven: the wall level disappears or falls below `BJ_WALL_KEEP_PCT` % of its size at the fill, or a
  trade prints through the wall price; at +`BJ_BE_PCT` % the stop moves to entry, then trails `BJ_TRAIL_PCT` %
  behind the best price since entry (only tightens); time stop after `BJ_TIME_STOP_SEC` if breakeven not reached.
- **Costs**: entry maker `BJ_MAKER_PCT` % + `BJ_GST_PCT` % GST; exit taker `BJ_TAKER_PCT` % + GST.
- **Kill switch**: `BJ_PAUSE_HOURS` pause after a `BJ_DAY_LOSS_PCT` % loss in one IST day, `BJ_MAX_LOSS_STREAK`
  losses in a row, or a stale feed; resting orders cancelled, open positions closed at the next trade. Telegram on
  pause and resume. The bot's `/pause` also stops new BOOK JUMP entries (open positions keep being managed).
- **Restart**: the book (balance, day stats, kill-switch pause, open positions) is restored from `state.json`; a position
  left open by a restart is closed at the first trade after start-up (`RESTART`). The first version's
  `bookjump_state.json` is migrated once and then no longer used.
- **Sessions**: every wall, order and trade is tagged `US` (Mon-Fri 09:30-16:00 New York, the bot's
  `in_us_session`) or `outside`.

### Logs
- `walls.csv`: one row per wall seen, written 300 s after it was confirmed: `time_ist` (when it had persisted
  `BJ_PERSIST_SEC`), `session`, `symbol`, `side`, `price`, `size_usdt`, `multiple` (at confirmation), `lifetime_s`
  (from first appearance), `outcome` (`pulled` = gone without a trade at its price, `touched` = traded at, then gone,
  `still there` at +300 s, `feed lost`), `mid_10s` / `mid_60s` / `mid_300s` (mid price that long after confirmation),
  plus `touched`, `broke_through` (a trade printed beyond it) and `bj_order` (`order` / `filled` if BOOK JUMP used it).
- `trades.csv`: BOOK JUMP trades with the existing columns (`strategy` = `BOOK JUMP`, `est_fees_usd` incl. GST,
  `exit_reason`: WALL_GONE / WALL_SHRANK / WALL_BROKEN / BE_STOP / TRAIL_STOP / TIME_STOP / KILL / RESTART) plus `session`.
- `events.jsonl`: `BJ_ORDER`, `BJ_CANCEL` (reason), `BJ_FILL`, `BJ_EXIT`, `BJ_KILL`, `BJ_FEED`, `BJ_UNIVERSE`, `BJ_REFUSED`.
- Telegram: fills and exits (one line each), kill switch pause/resume, feed stale/back, and at 00:00 IST a day summary
  split by session: walls seen, % pulled, % touched-and-held (touched, never traded through), orders, fills, trades,
  win rate, P&L. `/status` and the bot's summaries include a BOOK JUMP line (not part of the "ALL" total).

### Settings
All `BJ_*` settings with defaults are in `.env.example`.
