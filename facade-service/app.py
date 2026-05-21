import asyncio
import logging
import random
import time
import uuid

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

CONFIG_SERVER = "http://config-server:8080"
HZ_MEMBERS = ["hz1:5701", "hz2:5701", "hz3:5701"]

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


@app.on_event("startup")
async def startup():
    global http_client, logging_clients, hz_client, counter_queue
    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(10.0, connect=2.0, read=2.0),
        limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
    )
    hz_client = hazelcast.HazelcastClient(
        cluster_members=HZ_MEMBERS,
        cluster_name="dev",
    )
    counter_queue = hz_client.get_queue("counter-queue")
    print("[Facade] Connected to Hazelcast queue")


@app.on_event("shutdown")
async def shutdown():
    await http_client.aclose()
    for client in logging_clients.values():
        await client.aclose()
    hz_client.shutdown()


class TransactionRequest(BaseModel):
    user_id: str
    amount: float


async def get_logging_urls() -> list:
    resp = await http_client.get(f"{CONFIG_SERVER}/services/logging-service")
    return resp.json().get("urls", [])


async def get_counter_url() -> str:
    resp = await http_client.get(f"{CONFIG_SERVER}/services/counter-service")
    urls = resp.json().get("urls", [])
    return urls[0] if urls else None


async def call_logging_service(path: str, method: str = "GET", **kwargs):
    urls = await get_logging_urls()
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
            if method == "POST":
                resp = await logging_clients[url].post(full_url, **kwargs)
            else:
                resp = await logging_clients[url].get(full_url, **kwargs)
            print(f"[Facade] {method} {path} -> {url}")
            return resp
        except (httpx.ConnectError, httpx.TimeoutException):
            print(f"[Facade] {url} unavailable, trying next...")
            continue
    raise HTTPException(status_code=502, detail="All logging service instances unavailable")


@app.post("/transaction", status_code=201)
async def post_transaction(req: TransactionRequest):
    transaction_id = str(uuid.uuid4())
    timestamp = time.time()
    payload = {
        "transaction_id": transaction_id,
        "timestamp": timestamp,
        "user_id": req.user_id,
        "amount": req.amount,
    }

    t0_log = time.perf_counter()
    t0_mq = time.perf_counter()

    loop = asyncio.get_event_loop()
    log_resp, _ = await asyncio.gather(
        call_logging_service("/log", method="POST", json=payload),
        loop.run_in_executor(None, lambda: counter_queue.put(payload).result()),
    )

    log_time = time.perf_counter() - t0_log
    mq_time = time.perf_counter() - t0_mq

    timing_stats["logging_network_total"] += log_time
    timing_stats["counter_network_total"] += mq_time
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
        return {"user_id": user_id, "balance": None, "transactions": transactions,
                "note": "Counter service unavailable - transactions are queued"}
    try:
        balance_resp = await http_client.get(f"{counter_url}/balance/{user_id}")
        if balance_resp.status_code == 404:
            return {"user_id": user_id, "balance": None, "transactions": transactions}
        balance = balance_resp.json()["balance"]
    except (httpx.ConnectError, httpx.TimeoutException, httpx.ReadError):
        return {"user_id": user_id, "balance": None, "transactions": transactions,
                "note": "Counter service unavailable - transactions are queued"}

    return {"user_id": user_id, "balance": balance, "transactions": transactions}


@app.get("/accounts")
async def get_accounts():
    counter_url = await get_counter_url()
    if not counter_url:
        return {"balances": None, "note": "Counter service unavailable"}
    try:
        resp = await http_client.get(f"{counter_url}/balances")
        return {"balances": resp.json()["balances"]}
    except (httpx.ConnectError, httpx.TimeoutException, httpx.ReadError):
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
    uvicorn.run(app, host="0.0.0.0", port=8000)