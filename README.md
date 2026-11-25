# binance-bot

Paper trading bot on Binance USDT-M perpetuals (systemd service `binance-bot`, folder `~/binance-bot`).
`bot.py` holds the strategies, accounting, Telegram and logging; its docstring describes POC RETURN and the
other (switched-off) strategies in detail. `rsi1h.py` is the 1H RSI LONG / 1H RSI SHORT strategy.
`bookjump.py` holds the retired BOOK JUMP and BJ ABSORB strategies (off unless `BJ_ENABLED=1`).

| File | What |
|---|---|
| `bot.py` | main bot: POC RETURN (+ RSI / breakout strategies, off in this deployment), books, Telegram, logs |
| `rsi1h.py` | 1H RSI LONG / 1H RSI SHORT (port of the Fyers 2H RSI divergence strategy), paper only, `RSI1H_ENABLED=1` (default) |
| `bookjump.py` | BOOK JUMP and BJ ABSORB (retired 23 Nov 2026), only started with `BJ_ENABLED=1` |
| `.env` | settings and keys (never committed); see `.env.example` for every setting and its default |
| `trades.csv` | every closed trade, all strategies (`strategy` column = book name; `session` / `shadow_exit_pnl` for the old BOOK JUMP rows; `setup` = 1H RSI divergence detail) |
| `events.jsonl` | event log (one JSON object per line) |
| `walls.csv` | BOOK JUMP's walls (kept; no new rows while it is off) |
| `candidates.csv` | every 1H RSI divergence signal at each 5m / 15m check, taken or not: rank, reason if not taken (ranked lower, book busy, cooldown, entry check, size, ...), arming RSI, reference and divergence candle (time, high/low, RSI), RSI gap, entry check vs the divergence candle's open, candle stop, stop used, stop %, size. Same columns as the fyers-bot's `data/candidates.csv` (since 25 Nov 2026; no extra API calls, so there are no rows while a book is busy) |
| `state.json` | bot state; `books` holds every book (`POC RETURN`, `1H RSI LONG`, `1H RSI SHORT`; the old `BOOK JUMP` / `BJ ABSORB` books stay, unused); `rsi1h` holds the 1H RSI universe size, armed coins and cooldowns |

- **Entry execution (since 25 Nov 2026, `execution.py`, every book except POC RETURN, which keeps its resting limit at the level)**: reference = best ask (long) / best bid (short) at the signal; cap = reference +/- `ENTRY_SLIP_CAP_PCT` (0.1%). IOC limit children of at most what rests at the best level, the first at the best price, each next 1 tick worse, ~1 s apart, never beyond the cap (`ENTRY_MAX_CHILDREN` 20 per attempt); one retry with a fresh book; keep what filled (stop and size bookkeeping on that), nothing = skipped "no fill within cap". Children are step-sized and at least the min notional (a too-small last child is merged). Size capped at `LIQ_CAP_PCT_OF_DEPTH` (20%) of the top-5 levels on our side; below the min notional = "too little liquidity". Paper mode simulates every child on the live `/fapi/v1/depth` (minus what this entry already took) instead of filling at the last price, so `RSI1H_SLIPPAGE_PCT` now only applies to exits. Exits stay market / stop orders; in paper mode their slippage is estimated by walking the live book (logged only, the P&L still uses the stop / candle price). trades.csv and candidates.csv get entry_ref, entry_avg, slippage % and $, child orders, intended vs filled quantity, liq_capped, and the exit's numbers.

Run: `venv/bin/python bot.py` (normally via systemd). Checks: `venv/bin/python bot.py --selftest`.
Tests: `venv/bin/python -m unittest discover -s tests -v` and `venv/bin/python -m pyflakes bot.py bookjump.py tests`.

Telegram commands: `/status /summary /pause /resume /killswitch`.

## 1H RSI LONG / 1H RSI SHORT (paper only)

Two $100 paper books next to POC RETURN (`RSI1H_*` settings in `.env.example`; full rules in `rsi1h.py`'s docstring).
Ported from the Fyers 2H RSI strategy (`fyers_bot/strategy.py` `_div_trigger`), without session or day logic.
- **Universe / arming** (every hour, ~8 s after each 1H close = :30 past the hour in IST): every TRADING USDT perpetual
  with 24h quote volume >= $500k (independent of `MIN_TURNOVER_USD`, which stays POC's). Closed 1H RSI(14) > 70 arms
  SHORT, < 30 arms LONG; a coin stays armed only while that holds. 1H candles once an hour per symbol (request weight
  1 each); 5m / 15m candles only for armed symbols; requests pause near Binance's 2400/min weight limit.
- **SHORT** (5m, ~8 s after each close, 45 min window): reference = the highest RED candle with close > EMA5 and
  low > EMA5; divergence = the candle just closed: red, higher high, RSI(14, 5m) >= 3 points lower, >= 2 candles
  between. **LONG** (15m, 120 min): the lowest GREEN candle with high < EMA5; the candle just closed: green, lower
  low, RSI(14, 15m) >= 3 points higher, >= 2 candles between.
- **Entry**: at the check (start of the next candle), paper fill at the last trade price, only if it is still below
  the red divergence candle's open (short) / above the green one's open (long); otherwise `RSI1H_SKIP`.
  Taker fee `FEE_PCT_PER_SIDE` on both sides; slippage `RSI1H_SLIPPAGE_PCT` (0, like the POC paper fills).
- **Stop**: 1 tick beyond the divergence candle's high / low, at least 0.5% (widened + `RSI1H_STOP_WIDENED`).
  **Exit**: no target; stop to entry at 1R, from 3R a whole-R trail 1R behind; checked on 1m candles like POC.
- **Size**: 5% of the book at the stop, 20x isolated cap, step-rounded; skipped if Binance's min notional can't be
  met within that. One open trade per book; several signals at one check: highest 24h quote volume first (`RSI1H_RANK_BY=volume`, default since 25 Nov 2026; `rsi` = most extreme 1H RSI first, the old order), the next one tried if one is skipped; 30-min cooldown on a coin after a short.
- **Logs**: events `RSI1H_SCAN` (universe, armed counts, newly armed / dropped), `RSI1H_SIGNAL` (both RSI values and
  the gap), `RSI1H_SKIP` (reason), `RSI1H_STOP_WIDENED`, then the bot's `ENTRY` (with `setup`) / `EXIT`; trades.csv
  rows with strategy `1H RSI LONG` / `1H RSI SHORT` and the `setup` column. `/status` shows both books, the universe
  size and the top armed coins; the day summary (`report`) includes both books.

POC RETURN is unchanged and still trades one position at a time (no new POC order while a POC trade is open).

## BOOK JUMP (retired 23 Nov 2026; `BJ_ENABLED=1` to run it again)

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
  `BJ_ORDER_TIMEOUT_SEC`, or when price moves more than max(`BJ_AWAY_PCT` %, `BJ_AWAY_TICKS` ticks) away.
- **Paper fill**: queue ahead = size already resting at our price when placed. Filled when a trade prints through
  our price, or when aggressive trades at our price (sells for a buy) exceed that queue. Filled at our price.
- **Exit** (all at the next trade price after the trigger, as a market order; the trade that fills us never
  triggers an exit): hard stop at max(`BJ_STOP_PCT` %, `BJ_STOP_TICKS` ticks) from entry, checked on trade prints
  (`HARD_STOP`; the same distance sizes the trade, so a full stop loses about `BJ_RISK_PCT` % plus fees);
  breakeven once the trade is >= `BJ_BE_PCT` % AND >= `BJ_BE_TICKS` ticks in profit: stop to entry, then it trails
  max(`BJ_TRAIL_PCT` %, `BJ_TRAIL_TICKS` ticks) behind the best price since entry (only tightens); time stop after
  `BJ_TIME_STOP_SEC` if breakeven not reached. Every `BJ_EXIT` event logs these settings, the distances used, the
  best price, the final stop, the fill trade's price and what triggered the exit.
- **Shadow wall exit** (logged, never acted on): the old exits - wall level gone (`WALL_GONE`), below
  `BJ_WALL_KEEP_PCT` % of its size at the fill (`WALL_SHRANK`, judged only from book snapshots after the fill by Binance
  event time) or a trade strictly beyond the wall (`WALL_BROKEN`), before breakeven. The first time one would have
  fired, `BJ_WALL_EXIT_SHADOW` records the time, the next trade's price (where the old rule exited), the net P&L
  then, the trigger and the volume traded at the wall since the fill; trades.csv gets it as `shadow_exit_pnl`.
- **Costs**: entry maker `BJ_MAKER_PCT` % + `BJ_GST_PCT` % GST; exit taker `BJ_TAKER_PCT` % + GST.
- **Pause**: no pause on losses (removed 23 Nov 2026). Only a stale feed pauses: no data for `BJ_STALE_KILL_SEC` ->
  `BJ_PAUSE_HOURS` pause of both books, resting orders cancelled, open positions closed at the next trade. Telegram
  on pause and resume. A pause from before a restart is cleared at start-up. The bot's `/pause` also stops new
  entries (open positions keep being managed).
- **Restart**: the book (balance, day stats, open positions) is restored from `state.json`; a position
  left open by a restart is closed at the first trade after start-up (`RESTART`). The first version's
  `bookjump_state.json` is migrated once and then no longer used.
- **Sessions**: every wall, order and trade is tagged `US` (Mon-Fri 09:30-16:00 New York, the bot's
  `in_us_session`) or `outside`.

## BJ ABSORB (wall absorption, paper only)

Second paper book (`state.json` -> `books["BJ ABSORB"]`, `BJA_START_BALANCE` $100, `BJA_RISK_PCT` 1%), started
with BOOK JUMP (`BJA_ENABLE=0` turns it off). Same feed, coins and wall rule (top-5 level >= 5x average AND >= $25k,
persisting 3 s), same fills, sizing and exits (hard stop, breakeven, trail, time stop, shadow wall exit, no loss
pause). It enters only after the wall has proved itself:
- **Setup**: from the first trade at a seen wall's price, count the volume traded at that price that hits it
  (bid wall: aggressive sells; ask wall: aggressive buys). One setup per wall.
- **Absorbed** when, within `BJA_WINDOW_SEC` (60 s) of the first touch, that volume >= `BJA_ABSORB_PCT` (30%) of the
  wall's size at the first touch, the wall is still >= `BJA_KEEP_PCT` (50%) of that size and nothing printed beyond
  it, and all of it is still true `BJA_CONFIRM_S` (2 s) later.
- **Dropped** (logged with the reason): `dropped_pulled` (level gone, or under 50% with most of the reduction
  cancelled rather than traded), `dropped_shrank` (under 50%, mostly traded into), `dropped_broke` (a trade beyond
  the wall), `timeout` (not absorbed within 60 s), `dropped_feed`; absorbed but not entered: `skipped_busy` (order or
  position on the coin), `skipped_max_open`, `skipped_paused`, `skipped_not_passive`, `skipped_size`.
- **Entry**: post-only limit 1 tick in front of the wall, same queue fill rule; cancelled if not filled within
  `BJA_ORDER_TIMEOUT_SEC` (10 s) or price moves more than `BJA_AWAY_TICKS` (3) ticks away. BOOK JUMP's own wall
  tracker ending the wall (e.g. absorbed below the 5x / $25k bar) does not cancel it.
- **Logs**: events `BJA_SETUP`, `BJA_ABSORBED`, `BJA_DROP` (reason), `BJA_ORDER`, `BJA_FILL`, `BJA_CANCEL`,
  `BJA_EXIT`, `BJA_WALL_EXIT_SHADOW`, `BJA_KILL`, all with the session tag; trades.csv rows with strategy
  `BJ ABSORB`; walls.csv `absorbed_usdt`, `absorbed_pct`, `absorb_result` (`entered` / `dropped_*` / `timeout` /
  `skipped_*`; empty if never touched). `/status`, the bot's summaries and its own 00:00 IST day summary show it as
  its own book.

### Logs
- `walls.csv`: one row per wall seen, written 300 s after it was confirmed: `time_ist` (when it had persisted
  `BJ_PERSIST_SEC`), `session`, `symbol`, `side`, `price`, `size_usdt`, `multiple` (at confirmation), `lifetime_s`
  (from first appearance), `outcome` (`pulled` = gone without a trade at its price, `touched` = traded at, then gone,
  `still there` at +300 s, `feed lost`), `mid_10s` / `mid_60s` / `mid_300s` (mid price that long after confirmation),
  plus `touched`, `broke_through` (a trade printed beyond it), `bj_order` (`order` / `filled` if BOOK JUMP used it)
  and `absorbed_usdt` / `absorbed_pct` / `absorb_result` (BJ ABSORB; a row waits until its setup has a result).
- `trades.csv`: BOOK JUMP trades with the existing columns (`strategy` = `BOOK JUMP`, `est_fees_usd` incl. GST,
  `exit_reason`: HARD_STOP / BE_STOP / TRAIL_STOP / TIME_STOP / KILL / RESTART) plus `session` and `shadow_exit_pnl`
  (net P&L had the old wall rule exited; empty if it never fired). Rows before 23 Nov have WALL_* reasons.
- `events.jsonl`: `BJ_ORDER`, `BJ_CANCEL` (reason), `BJ_FILL`, `BJ_EXIT`, `BJ_WALL_EXIT_SHADOW`, `BJ_KILL`, `BJ_FEED`,
  `BJ_UNIVERSE`, `BJ_REFUSED` (and the `BJA_*` events above).
- Telegram: fills and exits (one line each; the exit line adds what the old wall rule would have made), stale-feed
  pause/resume, feed stale/back, and at 00:00 IST a day summary per book split by session (BOOK JUMP: walls seen,
  % pulled, % touched-and-held, orders, fills, trades, win rate, P&L; BJ ABSORB: setups, absorbed, dropped, orders,
  fills, trades, win rate, P&L). `/status` and the bot's summaries include a BOOK JUMP and a BJ ABSORB line (not part
  of the "ALL" total).

### Settings
All `BJ_*` settings with defaults are in `.env.example`.
