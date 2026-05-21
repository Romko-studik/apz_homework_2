import asyncio
import logging
import os
import time

import hazelcast
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient

class NoHealthFilter(logging.Filter):
    def filter(self, record):
        return "/health" not in record.getMessage()

logging.getLogger("uvicorn.access").addFilter(NoHealthFilter())

app = FastAPI(title="Counter Service")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "counter_db")
HZ_MEMBERS = os.environ.get("HZ_MEMBERS", "hz1:5701,hz2:5701,hz3:5701").split(",")
CONFIG_SERVER = os.environ.get("CONFIG_SERVER", "http://config-server:8080")
MY_URL = os.environ.get("MY_URL", "http://counter-service:8002")

mongo_client = None
balances = None
hz_client = None
mq = None
consumer_task = None


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
    global mongo_client, balances, hz_client, mq, consumer_task

    mongo_client = AsyncIOMotorClient(MONGO_URI)
    balances = mongo_client[MONGO_DB]["balances"]
    await balances.create_index("user_id", unique=True)
    print(f"[Counter] Connected to MongoDB at {MONGO_URI}")

    hz_client = hazelcast.HazelcastClient(
        cluster_members=HZ_MEMBERS,
        cluster_name="dev",
    )
    mq = hz_client.get_queue("counter-queue")
    print(f"[Counter] Listening on counter-queue")

    async with httpx.AsyncClient() as client:
        await client.post(f"{CONFIG_SERVER}/register", json={
            "service": "counter-service",
            "url": MY_URL,
        })
    print(f"[Counter] Registered with config server as {MY_URL}")

    consumer_task = asyncio.create_task(consume_queue())


@app.on_event("shutdown")
async def shutdown():
    consumer_task.cancel()
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
    uvicorn.run(app, host="0.0.0.0", port=8002)