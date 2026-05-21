import hazelcast
import sys

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
dist_map = client.get_map("counter-map").blocking()
dist_map.put_if_absent("key", 0)

print(f"[Client {client_id}] Starting 10K increments without locks...")
for i in range(10_000):
    value = dist_map.get("key")
    value += 1
    dist_map.put("key", value)

print(f"[Client {client_id}] Done. Current value: {dist_map.get('key')}")
client.shutdown()
