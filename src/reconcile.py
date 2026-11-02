"""Reconcile the lake against Kafka and against the exchange. Prints a report; exit code 1 if any check fails.

1. Every Kafka record is in exactly one place: trades_clean or trades_rejected (by partition and offset).
2. Each minute's trades are contiguous (Binance trade ids are consecutive per pair), so a gap means trades we never received.
3. Our candles equal Binance's own 1m klines (open, high, low, close, volume, trade count) for complete minutes."""
import json, subprocess, sys, time, urllib.request
from decimal import Decimal
import duckdb

LAKE = sys.argv[1] if len(sys.argv) > 1 else str(__import__("pathlib").Path(__file__).resolve().parents[1] / "data" / "lake").replace("\\", "/")
con = duckdb.connect(); con.execute("INSTALL delta; LOAD delta;")
T = lambda n: f"delta_scan('{LAKE}/{n}')"
ok = True

def end_offsets():
    out = subprocess.run(["docker", "exec", "tl-kafka", "/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server", "localhost:9092", "--topic", "trades"],
                         capture_output=True, text=True, env={**__import__("os").environ, "MSYS_NO_PATHCONV": "1"}).stdout
    return {int(l.split(":")[1]): int(l.split(":")[2]) for l in out.strip().splitlines()}

print("== 1. Kafka accounting")
ends = end_offsets()
for p, end in sorted(ends.items()):
    clean = con.execute(f"select count(*), count(distinct kafka_offset) from {T('trades_clean')} where kafka_partition={p}").fetchone()
    rej = con.execute(f"select count(*), count(distinct kafka_offset) from {T('trades_rejected')} where kafka_partition={p}").fetchone()
    both = con.execute(f"select count(*) from {T('trades_clean')} c join {T('trades_rejected')} r using (kafka_partition, kafka_offset) where c.kafka_partition={p}").fetchone()[0]
    top = con.execute(f"select max(o) from (select kafka_offset o from {T('trades_clean')} where kafka_partition={p} union all select kafka_offset from {T('trades_rejected')} where kafka_partition={p})").fetchone()[0]
    processed = (top + 1) if top is not None else 0
    gaps = processed - (clean[1] + rej[1])
    dupes = (clean[0] - clean[1]) + (rej[0] - rej[1]) + both
    good = gaps == 0 and dupes == 0
    ok &= good
    print(f"partition {p}: kafka end offset {end}, processed up to {processed} (lag {end - processed}); clean {clean[0]}, rejected {rej[0]}; missing {gaps}, counted twice {dupes} -> {'PASS' if good else 'FAIL'}")
rej = con.execute(f"select reason, count(*) from {T('trades_rejected')} group by 1 order by 2 desc").fetchall()
print("rejected by reason:", rej or "none")

print("== 2. Trade-id chain (Binance ids are consecutive per pair, so a break means trades we never received)")
rows = con.execute(f"""
  with c as (select symbol, epoch_ms(minute_ts) ms, trade_count n, first_trade_id f, last_trade_id l,
                    lag(l) over w pl, lag(epoch_ms(minute_ts)) over w pms, lead(f) over w nf, lead(epoch_ms(minute_ts)) over w nms
             from {T('candles_1m')} window w as (partition by symbol order by minute_ts))
  select symbol, ms, n, l - f + 1 span,
         (n <> l - f + 1) inside_gap,
         (pms is not null and (pms <> ms - 60000 or pl + 1 <> f)) broken_before,
         (nms is not null and (nms <> ms + 60000 or l + 1 <> nf)) broken_after
  from c where n <> l - f + 1 or (pms is not null and (pms <> ms - 60000 or pl + 1 <> f)) or (nms is not null and (nms <> ms + 60000 or l + 1 <> nf))""").fetchall()
gap_minutes = {(s, ms) for s, ms, *_ in rows}
print(f"minutes next to a break in the id chain: {len(rows)}")
for s, ms, n, span, inside, before, after in rows[:6]:
    where = "inside" if inside else ("before" if before else "after")
    print(f"  {s} {time.strftime('%H:%M', time.gmtime(ms/1000))} counted {n}, id span {span}, break {where}")

print("== 3. Candles vs Binance klines")
now_ms = int(time.time() * 1000)
mine = con.execute(f"""select symbol, epoch_ms(minute_ts) ms, cast(open as varchar), cast(high as varchar), cast(low as varchar), cast(close as varchar),
                              cast(volume as varchar), trade_count from {T('candles_1m')} where epoch_ms(minute_ts) < {now_ms - 3*60000} order by symbol, ms""").fetchall()
first_ms = {}
for s, ms, *_ in mine: first_ms.setdefault(s, ms)
by_sym = {}
for r in mine: by_sym.setdefault(r[0], []).append(r)
compared = skipped_edge = skipped_gap = 0; bad = {"open": 0, "high": 0, "low": 0, "close": 0, "volume": 0, "trade_count": 0}; examples = []
for sym, rs in by_sym.items():
    rs = rs[1:]  # the first minute is partial: the stream started mid-minute
    if not rs: continue
    lo, hi = rs[0][1], rs[-1][1]
    k = json.load(urllib.request.urlopen(urllib.request.Request(
        f"https://api.binance.com/api/v3/klines?symbol={sym}&interval=1m&startTime={lo}&endTime={hi}&limit=1000", headers={"User-Agent": "Mozilla/5.0"}), timeout=30))
    ex = {int(x[0]): x for x in k}
    for s, ms, o, h, l, c, v, n in rs:
        if (s, ms) in gap_minutes: skipped_gap += 1; continue
        x = ex.get(ms)
        if x is None: skipped_edge += 1; continue
        compared += 1
        cmp = {"open": (Decimal(o), Decimal(x[1])), "high": (Decimal(h), Decimal(x[2])), "low": (Decimal(l), Decimal(x[3])),
               "close": (Decimal(c), Decimal(x[4])), "volume": (Decimal(v), Decimal(x[5])), "trade_count": (n, int(x[8]))}
        for f, (a, b) in cmp.items():
            if a != b:
                bad[f] += 1
                if len(examples) < 5: examples.append((s, time.strftime('%H:%M', time.gmtime(ms/1000)), f, str(a), str(b)))
print(f"compared {compared} complete minutes (skipped {skipped_gap} with missing trades, {skipped_edge} not in exchange data)")
print("mismatches:", bad)
for e in examples: print("  e.g.", e)
ok &= compared > 0 and sum(bad.values()) == 0
print("RESULT:", "PASS" if ok else "FAIL")
sys.exit(0 if ok else 1)
