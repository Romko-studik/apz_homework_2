import logging
import os
import time

import hazelcast
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

class NoHealthFilter(logging.Filter):
    def filter(self, record):
        return "/health" not in record.getMessage()

logging.getLogger("uvicorn.access").addFilter(NoHealthFilter())

app = FastAPI(title="Logging Service")

INSTANCE_ID = os.environ.get("INSTANCE_ID", "1")
HZ_MEMBER = os.environ.get("HZ_MEMBER", "hz1:5701")

hz_client = None
dist_map = None


@app.on_event("startup")
def startup():
    global hz_client, dist_map
    hz_client = hazelcast.HazelcastClient(
        cluster_members=[HZ_MEMBER],
        cluster_name="dev",
        smart_routing=False,
    )
    dist_map = hz_client.get_map("transactions")
    print(f"[Logging-{INSTANCE_ID}] Connected to Hazelcast at {HZ_MEMBER}")


@app.on_event("shutdown")
def shutdown():
    if hz_client:
        hz_client.shutdown()


class Transaction(BaseModel):
    transaction_id: str
    timestamp: float
    user_id: str
    amount: float


# Changed to standard 'def' so FastAPI runs this in a background thread
@app.post("/log", status_code=201)
def log_transaction(tx: Transaction):
    t0 = time.perf_counter()
    # Use .result() to resolve the Hazelcast Future instead of await
    dist_map.set(tx.transaction_id, tx.dict()).result()
    processing_ms = (time.perf_counter() - t0) * 1000
    return {"status": "stored", "processing_ms": processing_ms}


@app.get("/logs")
def get_all_logs():
    values = dist_map.values().result()
    return {"transactions": list(values)}


@app.get("/logs/{user_id}")
def get_user_logs(user_id: str):
    values = dist_map.values().result()
    user_txs = [t for t in values if t["user_id"] == user_id]
    return {"user_id": user_id, "transactions": user_txs}


@app.get("/health")
def health():
    return {"status": "ok", "instance": INSTANCE_ID}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8001))
    uvicorn.run("app:app", host="0.0.0.0", port=port, workers=4)