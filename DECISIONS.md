# Decisions and traps

- **No windowed aggregation with a watermark.** A Spark windowed aggregate drops rows that arrive after the watermark and says nothing, which would break "every record accounted for". Instead each micro-batch inserts trades and recomputes the candle for every minute it touched, so a late trade corrects its candle. Too-old trades are rejected on purpose, with a reason (`too_late`, older than 10 minutes by default).
- **Every Kafka record lands in exactly one table**, keyed by partition and offset. That is what lets the check say "none missing, none counted twice".
- **Duplicates are found after the merge, not before.** First version read all of trades_clean to look for seen trade ids, so each batch cost more as the table grew. Now the merge only inserts unseen ids and the batch's offsets are looked up afterwards, in this batch's offset range only. A re-run batch finds its own rows by offset, so a crash replay is not mistaken for a duplicate.
- **Raw topic, no cleaning in the producer.** The producer adds only the receive time. Cleaning lives in one place and the topic can be replayed.
- **Idempotent writes.** The rejected table append carries the batch id (`txnAppId`, `txnVersion`), so a retried batch does not write twice.

## Traps I hit
- **Spark field names are case-insensitive by default**, and Binance sends both `t` (trade id) and `T` (trade time). Error: ambiguous reference to field `t`. Fix: `spark.sql.caseSensitive=true`.
- **A lazy DataFrame read after its own write.** The "new trades" frame reads trades_clean, so after the merge it saw its own rows as already seen and was empty: the candle table stayed empty with no error. `.cache()` did not fix it (Delta invalidates the cache of a table it writes). Fix: collect the touched minutes to the driver before the merge.
- **The first and last minute of a run are partial** (the stream starts mid-minute). The comparison skips the first minute and anything within three minutes of now.
- **Minutes next to an outage look like mismatches** even though no trade inside them is wrong: the trades before or after the break are missing. The id-chain check now flags them and the comparison skips them.
- **Windows and Docker:** Git Bash rewrites `/opt/...` paths passed to `docker exec` (set `MSYS_NO_PATHCONV=1`); Windows Python cannot read `/tmp` paths from Git Bash; the producer starts before the broker is ready and recovers on its own.

## Backfill
- **Fetch exactly the missing ids, not a time range.** Ids are consecutive per pair, so a gap is a pair of ids, and `historicalTrades?fromId=` returns the real individual trades (checked against a stored trade: same id, price and quantity). Time ranges would risk double counting.
- **Backfill goes through Kafka and the same checks.** It is marked `source=backfill` and exempt from only one rule, `too_late` (it is old by design). A bad or duplicate backfilled trade is rejected like any other, and everything stays accounted for by offset.
- **Dry run by default** (`--go` sends). It refuses a gap over 2 million ids, which would be a bug or an outage worth a human look.
- **The tables can be rebuilt from Kafka** (retention is 72 hours), which is how the schema change (a `source` column) was applied.
- **Accounting is exact only at batch boundaries.** Clean rows are written first and rejected rows after, in separate Delta tables, so a reader in between can see 4 records "missing" for a moment. A real fix is one transaction across both writes, which Delta does not offer across tables; the check should retry once before failing.

## A mistake that lost the raw topic
The compose file mounted a volume at `/var/lib/kafka/data`, but the Kafka image writes its logs to `/tmp/kraft-combined-logs` unless `KAFKA_LOG_DIRS` is set. The volume stayed empty and the topic lived only inside the container, so `docker compose down` deleted about 370,000 records. The Delta tables (a separate bind mount) survived, but the raw topic could not be replayed any more. Fix: `KAFKA_LOG_DIRS=/var/lib/kafka/data`. Lesson: check where a stateful container really writes before trusting a volume, and test a restart before depending on it.
