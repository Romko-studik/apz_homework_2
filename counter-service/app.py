import asyncio
import logging
import os
import time

import consul
import hazelcast
import uvicorn
from fastapi import FastAPI, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient

class NoHealthFilter(logging.Filter):
    def filter(self, record):
        return "/health" not in record.getMessage()

logging.getLogger("uvicorn.access").addFilter(NoHealthFilter())

app = FastAPI(title="Counter Service")

CONSUL_HOST = os.environ.get("CONSUL_HOST", "consul")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "counter_db")
MY_HOST = os.environ.get("MY_HOST", "counter-service")
MY_PORT = int(os.environ.get("PORT", 8002))

mongo_client = None
balances = None
hz_client = None
mq = None
consumer_task = None
consul_client = None


def get_consul_kv(key: str, default: str = "") -> str:
    _, data = consul_client.kv.get(key)
    if data:
        return data["Value"].decode()
    return default


async def consume_queue():
    print("[Counter] Queue consumer started")
    loop = asyncio.get_event_loop()
    while True:
        try:
            tx = await loop.run_in_executor(None, lambda: mq.poll(timeout=1).result())
            if tx is None:
                continue
            await balances.find_one_and_update(
                {"user_id": tx["user_id"]},
                {"$inc": {"balance": tx["amount"]}},
                upsert=True,
                return_document=True,
            )
            print(f"[Counter] Processed tx={tx['transaction_id']} user={tx['user_id']} amount={tx['amount']:+.2f}")
        except Exception as e:
            print(f"[Counter] Queue consumer error: {e}")
            await asyncio.sleep(1)


@app.on_event("startup")
async def startup():
    global mongo_client, balances, hz_client, mq, consumer_task, consul_client

    consul_client = consul.Consul(host=CONSUL_HOST)

    # Read MQ config from Consul KV
    hz_members = get_consul_kv("mq/hz-members", "hz1:5701,hz2:5701,hz3:5701").split(",")
    hz_cluster = get_consul_kv("mq/cluster-name", "dev")
    queue_name = get_consul_kv("mq/queue-name", "counter-queue")

    mongo_client = AsyncIOMotorClient(MONGO_URI)
    balances = mongo_client[MONGO_DB]["balances"]
    await balances.create_index("user_id", unique=True)
    print(f"[Counter] Connected to MongoDB at {MONGO_URI}")

    hz_client = hazelcast.HazelcastClient(
        cluster_members=hz_members,
        cluster_name=hz_cluster,
    )
    mq = hz_client.get_queue(queue_name)
    print(f"[Counter] Listening on queue '{queue_name}'")

    # Register with Consul
    consul_client.agent.service.register(
        name="counter-service",
        service_id="counter-service-1",
        address=MY_HOST,
        port=MY_PORT,
        check=consul.Check.http(
            f"http://{MY_HOST}:{MY_PORT}/health",
            interval="10s",
            timeout="5s",
        ),
    )
    print(f"[Counter] Registered with Consul as {MY_HOST}:{MY_PORT}")

    consumer_task = asyncio.create_task(consume_queue())


@app.on_event("shutdown")
async def shutdown():
    consumer_task.cancel()
    if consul_client:
        consul_client.agent.service.deregister("counter-service-1")
    hz_client.shutdown()
    mongo_client.close()


@app.get("/balance/{user_id}")
async def get_balance(user_id: str):
    doc = await balances.find_one({"user_id": user_id})
    if not doc:
        raise HTTPException(status_code=404, detail=f"User '{user_id}' not found")
    return {"user_id": user_id, "balance": doc["balance"]}


@app.get("/balances")
async def get_all_balances():
    docs = await balances.find({}, {"_id": 0, "user_id": 1, "balance": 1}).to_list(None)
    return {"balances": {d["user_id"]: d["balance"] for d in docs}}


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=MY_PORT)