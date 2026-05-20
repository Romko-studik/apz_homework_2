import time
import logging
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn


class NoHealthFilter(logging.Filter):
    def filter(self, record):
        return "/health" not in record.getMessage()

logging.getLogger("uvicorn.access").addFilter(NoHealthFilter())

app = FastAPI(title="Logging Service")

transactions: dict = {}


class Transaction(BaseModel):
    transaction_id: str
    timestamp: float
    user_id: str
    amount: float


@app.post("/log", status_code=201)
async def log_transaction(tx: Transaction):
    t0 = time.perf_counter()
    transactions[tx.transaction_id] = tx.dict()
    processing_ms = (time.perf_counter() - t0) * 1000
    print(f"[LOG] Stored transaction: {tx.transaction_id} user={tx.user_id} amount={tx.amount:+.2f}")
    return {"status": "stored", "processing_ms": processing_ms}


@app.get("/logs")
async def get_all_logs():
    return {"transactions": list(transactions.values())}


@app.get("/logs/{user_id}")
async def get_user_logs(user_id: str):
    user_txs = [t for t in transactions.values() if t["user_id"] == user_id]
    return {"user_id": user_id, "transactions": user_txs}


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)