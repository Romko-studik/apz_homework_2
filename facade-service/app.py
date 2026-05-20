import asyncio
import time
import uuid

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import logging


class NoHealthFilter(logging.Filter):
    def filter(self, record):
        return "/health" not in record.getMessage()

logging.getLogger("uvicorn.access").addFilter(NoHealthFilter())

app = FastAPI(title="Facade Service")

LOGGING_SERVICE_URL = "http://logging-service:8001"
COUNTER_SERVICE_URL = "http://counter-service:8002"

http_client: httpx.AsyncClient = None


@app.on_event("startup")
async def startup():
    global http_client
    http_client = httpx.AsyncClient(
        timeout=30.0,
        limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
    )


@app.on_event("shutdown")
async def shutdown():
    await http_client.aclose()

timing_stats = {
    "logging_network_total": 0.0,
    "counter_network_total": 0.0,
    "logging_processing_total": 0.0,
    "counter_processing_total": 0.0,
    "call_count": 0,
}


class TransactionRequest(BaseModel):
    user_id: str
    amount: float


async def timed(coro):
    t0 = time.perf_counter()
    resp = await coro
    return resp, time.perf_counter() - t0


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

    (log_resp, log_time), (counter_resp, counter_time) = await asyncio.gather(
        timed(http_client.post(f"{LOGGING_SERVICE_URL}/log", json=payload)),
        timed(http_client.post(f"{COUNTER_SERVICE_URL}/transaction", json=payload)),
    )

    timing_stats["logging_network_total"] += log_time
    timing_stats["counter_network_total"] += counter_time
    timing_stats["logging_processing_total"] += log_resp.json().get("processing_ms", 0) / 1000
    timing_stats["counter_processing_total"] += counter_resp.json().get("processing_ms", 0) / 1000
    timing_stats["call_count"] += 1

    if log_resp.status_code not in (200, 201):
        raise HTTPException(status_code=502, detail="Logging service error")
    if counter_resp.status_code not in (200, 201):
        raise HTTPException(status_code=502, detail="Counter service error")

    balance = counter_resp.json()["balance"]
    print(f"[FACADE] POST tx={transaction_id} user={req.user_id} amount={req.amount:+.2f} -> balance={balance:.2f}")
    return {"transaction_id": transaction_id, "balance": balance}


@app.get("/user/{user_id}")
async def get_user(user_id: str):
    balance_resp, logs_resp = await asyncio.gather(
        http_client.get(f"{COUNTER_SERVICE_URL}/balance/{user_id}"),
        http_client.get(f"{LOGGING_SERVICE_URL}/logs/{user_id}"),
    )

    if balance_resp.status_code == 404:
        raise HTTPException(status_code=404, detail=f"User '{user_id}' not found")

    balance = balance_resp.json()["balance"]
    transactions = logs_resp.json().get("transactions", [])
    return {"user_id": user_id, "balance": balance, "transactions": transactions}


@app.get("/accounts")
async def get_accounts():
    resp = await http_client.get(f"{COUNTER_SERVICE_URL}/balances")
    return {"balances": resp.json()["balances"]}


@app.get("/stats")
async def get_stats():
    count = timing_stats["call_count"]
    return {
        "call_count": count,
        "logging_network_avg_ms": round(timing_stats["logging_network_total"] / count * 1000, 2) if count else 0,
        "counter_network_avg_ms": round(timing_stats["counter_network_total"] / count * 1000, 2) if count else 0,
        "logging_processing_avg_ms": round(timing_stats["logging_processing_total"] / count * 1000, 2) if count else 0,
        "counter_processing_avg_ms": round(timing_stats["counter_processing_total"] / count * 1000, 2) if count else 0,
    }


@app.delete("/stats")
async def reset_stats():
    timing_stats["logging_network_total"] = 0.0
    timing_stats["counter_network_total"] = 0.0
    timing_stats["logging_processing_total"] = 0.0
    timing_stats["counter_processing_total"] = 0.0
    timing_stats["call_count"] = 0
    return {"status": "reset"}


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)