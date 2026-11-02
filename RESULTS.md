# Results (2026-11-02, one laptop, Docker Desktop, 8 CPUs, 16 GB)

Run: a few hours with gaps (the host or Docker paused for about 50 minutes; the producer was down for a few minutes later). Pairs: BTCUSDT, ETHUSDT, SOLUSDT. Source: Binance public trade stream. The lake was rebuilt from Kafka once during the day (new schema), which is why the 4 rejected test messages show different reasons in places (see section 2).

## 1. Kafka accounting
- Last check: Kafka end offset 370,482; processed up to 369,090 (lag 1,392 at that moment); trades_clean 369,086; trades_rejected 4; missing 0; counted twice 0. PASS.
- Held after `docker kill` of Spark mid-stream and a restart about five minutes later. After the restart the lag swung between 95 and 4,485 records while it caught up (very roughly 70 to 75 records a second on `local[2]`, from five polls, not a benchmark).
- The check can show a false "missing 4" for a moment: a batch writes trades_clean first and trades_rejected after, so a reader in between sees the rejected rows not yet written. It was gone on the next read. See DECISIONS.md.

## 2. Rejections (drill 1, `src/drills/inject.py`)
Four messages sent by hand: a copy of a real trade id, a line of text, a trade with price -1, a trade dated an hour before it was received. Each landed in trades_rejected with a matching reason (duplicate, malformed, bad_values, too_late) and trades_clean did not change. The duplicate shows as `duplicate_seen` when the original was processed in an earlier batch and as `duplicate_in_batch` when the replay put both in the same batch.

## 3. Trade-id chain and the backfill
Binance trade ids are consecutive per pair, so a break means trades we did not receive.
- Before the backfill: 9 gaps, 153,714 missing trade ids (BTCUSDT 3, ETHUSDT 3, SOLUSDT 3). The biggest were the host pause (BTCUSDT 70,038 ids, ETHUSDT 54,374, SOLUSDT 11,834); the smallest were short drops of 30 (BTCUSDT), 42 (SOLUSDT) and 193 (ETHUSDT) trades inside one minute. The cause of those three is NOT CHECKED (a WebSocket reconnect is my guess).
- `src/backfill.py` fetched exactly those ids from the exchange's public `historicalTrades` endpoint and sent 153,714 trades to Kafka marked `source=backfill`: the same number as the missing ids. They went through the same checks as live trades.
- After the backfill: 0 minutes next to a break in the id chain.

## 4. Candles against Binance's own 1m klines
- Before the backfill: 93 complete minutes compared, 15 skipped for the breaks, 0 mismatches.
- After the backfill: 372 complete minutes compared, 0 skipped, 0 absent from the exchange data. Mismatches: open 0, high 0, low 0, close 0, volume 0, trade count 0. PASS.
- Earlier runs before the id-chain check existed showed mismatches next to the outage (for example 322 trades counted against 2,200 at the exchange). That was incomplete data, not wrong candles.

## Not measured
Throughput under load, long runs, how long the backfill took, memory growth of the Delta tables, cost of the trades_clean merge as the table grows, behaviour across the daily WebSocket disconnect, any pair beyond the three, and whether backfilled trades could ever differ from the live ones (they matched in the candle comparison).
