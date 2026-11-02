"""Kafka `trades` -> Delta lake, with every Kafka record accounted for.

Each record ends in exactly ONE place: trades_clean or trades_rejected (with a reason).
One-minute candles are recomputed from trades_clean for the minutes a batch touched,
so a late trade fixes its candle instead of being dropped (a windowed aggregation with a
watermark drops late rows silently, which would break the count)."""
import os
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType

LAKE = "/data/lake"
CKPT = os.environ.get("CHECKPOINT", "/data/checkpoints/trades")
BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:29092")
LATE_MS = int(os.environ.get("LATENESS_MS", "600000"))  # a trade received this long after it happened is rejected as too_late

spark = SparkSession.builder.appName("tick-ledger").config("spark.sql.caseSensitive", "true").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.shuffle.partitions", "4")

CLEAN, REJ, CANDLES = f"{LAKE}/trades_clean", f"{LAKE}/trades_rejected", f"{LAKE}/candles_1m"
spark.sql(f"""CREATE TABLE IF NOT EXISTS delta.`{CLEAN}` (symbol STRING, trade_id BIGINT, price DECIMAL(18,8), qty DECIMAL(18,8),
  trade_ms BIGINT, trade_ts TIMESTAMP, recv_ms BIGINT, kafka_partition INT, kafka_offset BIGINT, source STRING) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS delta.`{REJ}` (reason STRING, raw STRING, symbol STRING, trade_id BIGINT,
  kafka_partition INT, kafka_offset BIGINT, rejected_at TIMESTAMP) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS delta.`{CANDLES}` (symbol STRING, minute_ts TIMESTAMP, open DECIMAL(18,8), high DECIMAL(18,8),
  low DECIMAL(18,8), close DECIMAL(18,8), volume DECIMAL(28,8), trade_count BIGINT, first_trade_id BIGINT, last_trade_id BIGINT,
  updated_at TIMESTAMP) USING DELTA""")

data = StructType([StructField("e", StringType()), StructField("E", LongType()), StructField("s", StringType()), StructField("t", LongType()),
                   StructField("p", StringType()), StructField("q", StringType()), StructField("T", LongType()),
                   StructField("m", BooleanType()), StructField("M", BooleanType())])
schema = StructType([StructField("recv_ms", LongType()), StructField("stream", StringType()), StructField("source", StringType()), StructField("data", data)])


def process(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    b = (batch_df.select(F.col("value").cast("string").alias("raw"), F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"))
         .withColumn("j", F.from_json("raw", schema)))
    b = (b.withColumn("symbol", F.col("j.data.s")).withColumn("trade_id", F.col("j.data.t"))
         .withColumn("price", F.col("j.data.p").cast("decimal(18,8)")).withColumn("qty", F.col("j.data.q").cast("decimal(18,8)"))
         .withColumn("trade_ms", F.col("j.data.T")).withColumn("recv_ms", F.col("j.recv_ms"))
         .withColumn("source", F.coalesce(F.col("j.source"), F.lit("live"))))  # "backfill" = trades fetched later from the exchange to fill a gap
    reason = (F.when(F.col("j").isNull() | F.col("symbol").isNull() | F.col("trade_id").isNull() | F.col("price").isNull()
                     | F.col("qty").isNull() | F.col("trade_ms").isNull() | F.col("recv_ms").isNull(), "malformed")
              .when((F.col("price") <= 0) | (F.col("qty") <= 0) | (F.col("trade_ms") <= 0), "bad_values")
              .when((F.col("source") != "backfill") & (F.col("recv_ms") - F.col("trade_ms") > LATE_MS), "too_late"))  # a backfilled trade is old by design
    b = b.withColumn("reason", reason)
    ok = b.filter(F.col("reason").isNull())
    w = Window.partitionBy("symbol", "trade_id").orderBy("kafka_partition", "kafka_offset")
    ok = ok.withColumn("rn", F.row_number().over(w))
    dup_batch = ok.filter("rn > 1").withColumn("reason", F.lit("duplicate_in_batch"))
    first = ok.filter("rn = 1")
    ins = first.select("symbol", "trade_id", "price", "qty", "trade_ms", (F.col("trade_ms") / 1000).cast("timestamp").alias("trade_ts"),
                       "recv_ms", "kafka_partition", "kafka_offset", "source")
    touched_rows = ins.select("symbol", F.date_trunc("minute", "trade_ts").alias("minute_ts")).distinct().collect()
    # Insert-only merge: a trade id already in trades_clean is simply not inserted. Which batch rows were NOT inserted is found afterwards
    # by looking for their Kafka offsets in the table, over this batch's offset range only (so the cost does not grow with the table).
    # Re-running a batch after a crash is safe: its rows are found by offset, so they are not mistaken for duplicates.
    (DeltaTable.forPath(spark, CLEAN).alias("t").merge(ins.alias("s"), "t.symbol = s.symbol AND t.trade_id = s.trade_id")
     .whenNotMatchedInsertAll().execute())
    lo, hi = b.agg(F.min("kafka_offset"), F.max("kafka_offset")).first()
    present = (spark.read.format("delta").load(CLEAN).filter((F.col("kafka_offset") >= lo) & (F.col("kafka_offset") <= hi))
               .select("kafka_partition", "kafka_offset"))
    dup_seen = first.join(present, ["kafka_partition", "kafka_offset"], "left_anti").withColumn("reason", F.lit("duplicate_seen"))
    rejected = (b.filter(F.col("reason").isNotNull()).select("reason", "raw", "symbol", "trade_id", "kafka_partition", "kafka_offset")
                .unionByName(dup_batch.select("reason", "raw", "symbol", "trade_id", "kafka_partition", "kafka_offset"))
                .unionByName(dup_seen.select("reason", "raw", "symbol", "trade_id", "kafka_partition", "kafka_offset"))
                .withColumn("rejected_at", F.current_timestamp()))
    (rejected.write.format("delta").mode("append").option("txnAppId", "rejected").option("txnVersion", batch_id).save(REJ))
    touched =spark.createDataFrame([(r["symbol"], r["minute_ts"]) for r in touched_rows], "symbol string, minute_ts timestamp")
    if not touched_rows:
        return
    lo_ts = min(r["minute_ts"] for r in touched_rows)  # files are written in time order, so Delta can skip files older than the oldest touched minute
    clean = (spark.read.format("delta").load(CLEAN).filter(F.col("trade_ts") >= F.lit(lo_ts))
             .withColumn("minute_ts", F.date_trunc("minute", "trade_ts")))
    agg = (clean.join(touched, ["symbol", "minute_ts"]).groupBy("symbol", "minute_ts")
           .agg(F.min_by("price", F.struct("trade_ms", "trade_id")).alias("open"), F.max("price").alias("high"), F.min("price").alias("low"),
                F.max_by("price", F.struct("trade_ms", "trade_id")).alias("close"), F.sum("qty").alias("volume"),
                F.count("*").alias("trade_count"), F.min("trade_id").alias("first_trade_id"), F.max("trade_id").alias("last_trade_id"))
           .withColumn("updated_at", F.current_timestamp()))
    (DeltaTable.forPath(spark, CANDLES).alias("t").merge(agg.alias("s"), "t.symbol = s.symbol AND t.minute_ts = s.minute_ts")
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    print(f"batch {batch_id}: kafka_records={batch_df.count()} minutes_touched={len(touched_rows)}", flush=True)


(spark.readStream.format("kafka").option("kafka.bootstrap.servers", BROKERS).option("subscribe", "trades")
 .option("startingOffsets", "earliest").option("maxOffsetsPerTrigger", 50000).load()
 .writeStream.foreachBatch(process).option("checkpointLocation", CKPT).trigger(processingTime="10 seconds").start().awaitTermination())
