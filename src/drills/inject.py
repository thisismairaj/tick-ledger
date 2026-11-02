"""Drill 1: put bad messages on the topic and check each lands in trades_rejected with the right reason, and no candle changes.
Sends: an exact copy of a real trade (duplicate), text that is not JSON (malformed), a trade with a negative price (bad_values),
and a trade that happened an hour before it was received (too_late)."""
import json, os, subprocess, time
ENV = {**os.environ, "MSYS_NO_PATHCONV": "1"}
def sh(*a, stdin=None):
    return subprocess.run(["docker", "exec", "-i", "tl-kafka", *a], input=stdin, capture_output=True, text=True, env=ENV).stdout

real = sh("/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "localhost:9092", "--topic", "trades", "--partition", "0", "--offset", "100", "--max-messages", "1").strip()
m = json.loads(real)
now = int(time.time() * 1000)
bad_price = json.loads(real); bad_price["data"]["t"] = 9_000_000_001; bad_price["data"]["p"] = "-1.00000000"; bad_price["recv_ms"] = now; bad_price["data"]["T"] = now
late = json.loads(real); late["data"]["t"] = 9_000_000_002; late["data"]["T"] = now - 3_600_000; late["recv_ms"] = now
lines = [real, "this is not json", json.dumps(bad_price, separators=(",", ":")), json.dumps(late, separators=(",", ":"))]
sh("/opt/kafka/bin/kafka-console-producer.sh", "--bootstrap-server", "localhost:9092", "--topic", "trades", stdin="\n".join(lines) + "\n")
print("sent 4 bad messages; duplicated trade id", m["data"]["t"], m["data"]["s"])
