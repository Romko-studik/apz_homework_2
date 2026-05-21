import hazelcast
import sys
import time

mode = sys.argv[1] if len(sys.argv) > 1 else "writer"
reader_id = sys.argv[2] if len(sys.argv) > 2 else "1"

client = hazelcast.HazelcastClient(
    cluster_members=[
        "127.0.0.1:5701", 
        "127.0.0.1:5702", 
        "127.0.0.1:5703"
    ],
    cluster_name="dev",
    smart_routing=False, 
)

queue = client.get_queue("bounded-queue").blocking()

if mode == "writer":
    print("[Writer] Writing values 1..100 to queue (max size 10)...")
    for i in range(1, 101):
        queue.put(i)  # blocks if queue is full
        print(f"[Writer] Put {i}, queue size: {queue.size()}")
    print("[Writer] Done.")

elif mode == "reader":
    print(f"[Reader {reader_id}] Starting to read from queue...")
    count = 0
    while True:
        value = queue.poll(timeout=5)  # wait up to 5s for a value
        if value is None:
            print(f"[Reader {reader_id}] No more values, exiting.")
            break
        print(f"[Reader {reader_id}] Got: {value}")
        count += 1
    print(f"[Reader {reader_id}] Read {count} values total.")

client.shutdown()
