import time
import asyncio
import logging
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn


class NoHealthFilter(logging.Filter):
    def filter(self, record):
        return "/health" not in record.getMessage()

logging.getLogger("uvicorn.access").addFilter(NoHealthFilter())

app = FastAPI(title="Counter Service")

balances: dict = {}
_lock = asyncio.Lock()


class Transaction(BaseModel):
    transaction_id: str
    timestamp: float
    user_id: str
    amount: float


@app.post("/transaction")
async def apply_transaction(tx: Transaction):
    t0 = time.perf_counter()
    async with _lock:
        balances[tx.user_id] = balances.get(tx.user_id, 0.0) + tx.amount
        new_balance = balances[tx.user_id]
    processing_ms = (time.perf_counter() - t0) * 1000
    print(f"[COUNTER] user={tx.user_id} amount={tx.amount:+.2f} balance={new_balance:.2f}")
    return {"balance": new_balance, "processing_ms": processing_ms}


@app.get("/balance/{user_id}")
async def get_balance(user_id: str):
    if user_id not in balances:
        raise HTTPException(status_code=404, detail=f"User '{user_id}' not found")
    return {"user_id": user_id, "balance": balances[user_id]}


@app.get("/balances")
async def get_all_balances():
    return {"balances": dict(balances)}


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)