
import hazelcast
import sys
import time

client_id = sys.argv[1] if len(sys.argv) > 1 else "1"

client = hazelcast.HazelcastClient(
    cluster_members=[
        "127.0.0.1:5701", 
        "127.0.0.1:5702", 
        "127.0.0.1:5703"
    ],
    cluster_name="dev",
    smart_routing=False, 
)

dist_map = client.get_map("counter-pessimistic").blocking()
dist_map.put_if_absent("key", 0)

print(f"[Client {client_id}] Starting 10K increments with pessimistic locking...")
start = time.perf_counter()

for i in range(10_000):
    dist_map.lock("key")
    try:
        value = dist_map.get("key")
        value += 1
        dist_map.put("key", value)
        if (i == 100):
            print(f"[Client {client_id}] Reached 1/100th of increments...")
    finally:
        dist_map.unlock("key")

elapsed = time.perf_counter() - start
print(f"[Client {client_id}] Done in {elapsed:.2f}s. Current value: {dist_map.get('key')}")
client.shutdown()
