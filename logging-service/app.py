import logging
import os
import time

import consul
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
CONSUL_HOST = os.environ.get("CONSUL_HOST", "consul")
MY_HOST = os.environ.get("MY_HOST", "logging-service-1")
MY_PORT = int(os.environ.get("PORT", 8001))

hz_client = None
dist_map = None
consul_client = None


def get_consul_kv(key: str, default: str = "") -> str:
    _, data = consul_client.kv.get(key)
    if data:
        return data["Value"].decode()
    return default


@app.on_event("startup")
def startup():
    global hz_client, dist_map, consul_client

    consul_client = consul.Consul(host=CONSUL_HOST)

    # Read Hazelcast config from Consul KV
    hz_member = get_consul_kv(f"hazelcast/member-{INSTANCE_ID}", f"hz{INSTANCE_ID}:5701")
    hz_cluster = get_consul_kv("hazelcast/cluster-name", "dev")

    hz_client = hazelcast.HazelcastClient(
        cluster_members=[hz_member],
        cluster_name=hz_cluster,
        smart_routing=False,
    )
    dist_map = hz_client.get_map("transactions")
    print(f"[Logging-{INSTANCE_ID}] Connected to Hazelcast at {hz_member}")

    # Register with Consul
    consul_client.agent.service.register(
        name="logging-service",
        service_id=f"logging-service-{INSTANCE_ID}",
        address=MY_HOST,
        port=MY_PORT,
        check=consul.Check.http(
            f"http://{MY_HOST}:{MY_PORT}/health",
            interval="10s",
            timeout="5s",
        ),
    )
    print(f"[Logging-{INSTANCE_ID}] Registered with Consul as {MY_HOST}:{MY_PORT}")


@app.on_event("shutdown")
def shutdown():
    if consul_client:
        consul_client.agent.service.deregister(f"logging-service-{INSTANCE_ID}")
    if hz_client:
        hz_client.shutdown()


class Transaction(BaseModel):
    transaction_id: str
    timestamp: float
    user_id: str
    amount: float


@app.post("/log", status_code=201)
def log_transaction(tx: Transaction):
    t0 = time.perf_counter()
    dist_map.set(tx.transaction_id, tx.dict()).result()
    processing_ms = (time.perf_counter() - t0) * 1000
    print(f"[Logging-{INSTANCE_ID}] Stored tx={tx.transaction_id} user={tx.user_id} amount={tx.amount:+.2f}")
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