import hazelcast

client = hazelcast.HazelcastClient(
    cluster_members=[
        "127.0.0.1:5701", 
        "127.0.0.1:5702", 
        "127.0.0.1:5703"
    ],
    cluster_name="dev",
    smart_routing=False, 
)

dist_map = client.get_map("distributed-map")

print("Writing 1000 entries...")
futures = [dist_map.set(str(i), f"value-{i}") for i in range(1000)]
for f in futures:
    f.result()

print(f"Done. Total entries in map: {dist_map.size().result()}")
client.shutdown()