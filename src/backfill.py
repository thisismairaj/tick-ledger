"""Fill gaps in the trade-id chain with the real trades from the exchange.

Binance trade ids are consecutive per pair, so any id missing between the lowest and highest id we hold is a trade we never received
(the producer was down, the host paused, a reconnect dropped a few). This finds those id ranges in trades_clean, fetches exactly those
trades from the public REST endpoint (/api/v3/historicalTrades?fromId=...), and puts them on the same Kafka topic marked source=backfill,
so they go through the same checks as live trades. Dry run by default; --go sends."""
import json, os, subprocess, sys, time, urllib.error, urllib.request
import duckdb

LAKE = str(__import__("pathlib").Path(__file__).resolve().parents[1] / "data" / "lake").replace("\\", "/")
GO = "--go" in sys.argv
MAX_IDS = 2_000_000  # refuse to fetch an absurd range (a bug, or a long outage that deserves a human look)
ENV = {**os.environ, "MSYS_NO_PATHCONV": "1"}

con = duckdb.connect(); con.execute("INSTALL delta; LOAD delta;")
gaps = con.execute(f"""
  select symbol, prev_id + 1 as from_id, trade_id - 1 as to_id
  from (select symbol, trade_id, lag(trade_id) over (partition by symbol order by trade_id) prev_id
        from (select distinct symbol, trade_id from delta_scan('{LAKE}/trades_clean')))
  where trade_id - prev_id > 1 order by symbol, from_id""").fetchall()
total = sum(t - f + 1 for _, f, t in gaps)
print(f"{len(gaps)} gaps, {total} missing trade ids")
for s, f, t in gaps: print(f"  {s}: ids {f} to {t} ({t - f + 1})")
if not GO:
    print("dry run: nothing sent (use --go)"); sys.exit(0)

def get(url, tries=6):
    for i in range(tries):
        try:
            return json.load(urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"}), timeout=30))
        except urllib.error.HTTPError as e:
            if e.code in (418, 429):
                wait = int(e.headers.get("Retry-After", "10")); print(f"rate limited, waiting {wait}s", flush=True); time.sleep(wait)
            else: raise
        except Exception as e:
            print("retry after", repr(e)[:80], flush=True); time.sleep(2 ** i)
    raise RuntimeError("giving up on " + url)

def send(lines):
    subprocess.run(["docker", "exec", "-i", "tl-kafka", "/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server", "localhost:9092", "--topic", "trades"],
                   input="\n".join(lines) + "\n", text=True, env=ENV, check=True, capture_output=True)

sent = 0
for sym, f, t in gaps:
    if t - f + 1 > MAX_IDS: print(f"skip {sym} {f}-{t}: too large"); continue
    nxt, buf = f, []
    while nxt <= t:
        rows = get(f"https://api.binance.com/api/v3/historicalTrades?symbol={sym}&fromId={nxt}&limit=1000")
        if not rows: print(f"no rows from id {nxt} for {sym}"); break
        for r in rows:
            if f <= r["id"] <= t:
                buf.append(json.dumps({"recv_ms": int(time.time() * 1000), "stream": "backfill", "source": "backfill",
                    "data": {"e": "trade", "E": r["time"], "s": sym, "t": r["id"], "p": r["price"], "q": r["qty"], "T": r["time"], "m": r["isBuyerMaker"], "M": r["isBestMatch"]}},
                    separators=(",", ":")))
        nxt = rows[-1]["id"] + 1
        if len(buf) >= 5000: send(buf); sent += len(buf); buf = []; print(f"{sym}: sent {sent} so far", flush=True)
        time.sleep(0.3)
    if buf: send(buf); sent += len(buf); buf = []
print(f"done: {sent} trades sent as source=backfill")
