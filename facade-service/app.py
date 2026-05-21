import asyncio
import logging
import os
import random
import time
import uuid

import consul
import hazelcast
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

class NoHealthFilter(logging.Filter):
    def filter(self, record):
        return "/health" not in record.getMessage()

logging.getLogger("uvicorn.access").addFilter(NoHealthFilter())

app = FastAPI(title="Facade Service")

CONSUL_HOST = os.environ.get("CONSUL_HOST", "consul")
MY_HOST = os.environ.get("MY_HOST", "facade-service")
MY_PORT = int(os.environ.get("PORT", 8000))

timing_stats = {
    "logging_network_total": 0.0,
    "counter_network_total": 0.0,
    "logging_processing_total": 0.0,
    "call_count": 0,
}

http_client: httpx.AsyncClient = None
logging_clients: dict = {}
hz_client = None
counter_queue = None
consul_client = None


def get_consul_kv(key: str, default: str = "") -> str:
    _, data = consul_client.kv.get(key)
    if data:
        return data["Value"].decode()
    return default


def discover_service(service_name: str) -> list:
    _, services = consul_client.health.service(service_name, passing=True)
    return [f"http://{s['Service']['Address']}:{s['Service']['Port']}" for s in services]


@app.on_event("startup")
async def startup():
    global http_client, logging_clients, hz_client, counter_queue, consul_client

    consul_client = consul.Consul(host=CONSUL_HOST)

    hz_members = get_consul_kv("mq/hz-members", "hz1:5701,hz2:5701,hz3:5701").split(",")
    hz_cluster = get_consul_kv("mq/cluster-name", "dev")
    queue_name = get_consul_kv("mq/queue-name", "counter-queue")

    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(10.0, connect=3.0, read=5.0),
        limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
    )
    hz_client = hazelcast.HazelcastClient(cluster_members=hz_members, cluster_name=hz_cluster)
    counter_queue = hz_client.get_queue(queue_name)
    print(f"[Facade] Connected to Hazelcast queue '{queue_name}'")

    consul_client.agent.service.register(
        name="facade-service",
        service_id="facade-service-1",
        address=MY_HOST,
        port=MY_PORT,
        check=consul.Check.http(f"http://{MY_HOST}:{MY_PORT}/health", interval="10s", timeout="5s"),
    )
    print(f"[Facade] Registered with Consul as {MY_HOST}:{MY_PORT}")


@app.on_event("shutdown")
async def shutdown():
    await http_client.aclose()
    for client in logging_clients.values():
        await client.aclose()
    consul_client.agent.service.deregister("facade-service-1")
    hz_client.shutdown()


class TransactionRequest(BaseModel):
    user_id: str
    amount: float


async def call_logging_service(path: str, method: str = "GET", **kwargs):
    urls = discover_service("logging-service")
    if not urls:
        raise HTTPException(status_code=502, detail="No logging service instances available")
    random.shuffle(urls)
    for url in urls:
        if url not in logging_clients:
            logging_clients[url] = httpx.AsyncClient(
                timeout=5.0,
                limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            )
        try:
            full_url = f"{url}{path}"
            resp = await logging_clients[url].post(full_url, **kwargs) if method == "POST" else await logging_clients[url].get(full_url, **kwargs)
            print(f"[Facade] {method} {path} -> {url}")
            return resp
        except (httpx.ConnectError, httpx.TimeoutException):
            print(f"[Facade] {url} unavailable, trying next...")
            continue
    raise HTTPException(status_code=502, detail="All logging service instances unavailable")


async def get_counter_url() -> str:
    urls = discover_service("counter-service")
    return urls[0] if urls else None


@app.post("/transaction", status_code=201)
async def post_transaction(req: TransactionRequest):
    transaction_id = str(uuid.uuid4())
    timestamp = time.time()
    payload = {"transaction_id": transaction_id, "timestamp": timestamp, "user_id": req.user_id, "amount": req.amount}

    loop = asyncio.get_event_loop()
    t0 = time.perf_counter()
    log_resp, _ = await asyncio.gather(
        call_logging_service("/log", method="POST", json=payload),
        loop.run_in_executor(None, lambda: counter_queue.put(payload).result()),
    )
    elapsed = time.perf_counter() - t0
    timing_stats["logging_network_total"] += elapsed
    timing_stats["counter_network_total"] += elapsed
    timing_stats["call_count"] += 1
    try:
        timing_stats["logging_processing_total"] += log_resp.json().get("processing_ms", 0) / 1000
    except Exception:
        pass

    print(f"[Facade] POST tx={transaction_id} user={req.user_id} amount={req.amount:+.2f} queued")
    return {"transaction_id": transaction_id, "status": "queued"}


@app.get("/user/{user_id}")
async def get_user(user_id: str):
    counter_url = await get_counter_url()
    logs_resp = await call_logging_service(f"/logs/{user_id}")
    transactions = logs_resp.json().get("transactions", [])
    if not counter_url:
        return {"user_id": user_id, "balance": None, "transactions": transactions}
    try:
        balance_resp = await http_client.get(f"{counter_url}/balance/{user_id}")
        balance = None if balance_resp.status_code == 404 else balance_resp.json()["balance"]
    except Exception:
        balance = None
    return {"user_id": user_id, "balance": balance, "transactions": transactions}


@app.get("/accounts")
async def get_accounts():
    counter_url = await get_counter_url()
    if not counter_url:
        return {"balances": None, "note": "Counter service unavailable - transactions are queued"}
    try:
        resp = await http_client.get(f"{counter_url}/balances")
        return {"balances": resp.json()["balances"]}
    except Exception:
        return {"balances": None, "note": "Counter service unavailable - transactions are queued"}


@app.get("/stats")
async def get_stats():
    count = timing_stats["call_count"]
    return {
        "call_count": count,
        "logging_network_avg_ms": round(timing_stats["logging_network_total"] / count * 1000, 2) if count else 0,
        "counter_queue_avg_ms": round(timing_stats["counter_network_total"] / count * 1000, 2) if count else 0,
        "logging_processing_avg_ms": round(timing_stats["logging_processing_total"] / count * 1000, 4) if count else 0,
    }


@app.delete("/stats")
async def reset_stats():
    for k in timing_stats:
        timing_stats[k] = 0
    return {"status": "reset"}


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=MY_PORT)