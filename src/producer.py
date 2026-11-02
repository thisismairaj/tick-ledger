"""Binance public trade stream -> Kafka topic. Keeps every message exactly as received (plus the receive time).
Nothing is filtered, deduplicated or fixed here: that is the stream job's work, so the raw topic can be replayed."""
import asyncio, json, os, time
import websockets
from confluent_kafka import Producer

BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
TOPIC = os.environ.get("TOPIC", "trades")
PAIRS = os.environ.get("PAIRS", "btcusdt,ethusdt,solusdt").split(",")
URL = "wss://stream.binance.com:9443/stream?streams=" + "/".join(f"{p}@trade" for p in PAIRS)

producer = Producer({"bootstrap.servers": BROKERS, "enable.idempotence": True, "linger.ms": 20, "compression.type": "zstd"})
sent = failed = 0

def on_delivery(err, msg):
    global failed
    if err is not None:
        failed += 1

async def run():
    global sent
    backoff = 1
    while True:
        try:
            async with websockets.connect(URL, ping_interval=20, max_queue=None) as ws:
                print("connected", URL, flush=True)
                backoff = 1
                async for raw in ws:
                    recv_ms = int(time.time() * 1000)
                    msg = json.loads(raw)
                    key = msg["data"]["s"]
                    value = json.dumps({"recv_ms": recv_ms, "stream": msg["stream"], "data": msg["data"]}, separators=(",", ":"))
                    producer.produce(TOPIC, key=key, value=value, on_delivery=on_delivery)
                    producer.poll(0)
                    sent += 1
                    if sent % 1000 == 0:
                        print(f"sent={sent} failed={failed}", flush=True)
        except Exception as e:
            print(f"disconnected: {e!r}; retry in {backoff}s", flush=True)
            producer.flush(5)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

asyncio.run(run())
