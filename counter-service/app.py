import logging
import os
import time

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from motor.motor_asyncio import AsyncIOMotorClient

class NoHealthFilter(logging.Filter):
    def filter(self, record):
        return "/health" not in record.getMessage()

logging.getLogger("uvicorn.access").addFilter(NoHealthFilter())

app = FastAPI(title="Counter Service")

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.environ.get("MONGO_DB", "counter_db")

mongo_client = None
balances = None


@app.on_event("startup")
async def startup():
    global mongo_client, balances
    mongo_client = AsyncIOMotorClient(MONGO_URI)
    balances = mongo_client[MONGO_DB]["balances"]
    
    # CRITICAL FIX: Creates a unique index on user_id to prevent full collection scans
    await balances.create_index("user_id", unique=True)
    
    print(f"[Counter] Connected to MongoDB at {MONGO_URI} and ensured indexes.")


@app.on_event("shutdown")
async def shutdown():
    mongo_client.close()


class Transaction(BaseModel):
    transaction_id: str
    timestamp: float
    user_id: str
    amount: float


@app.post("/transaction")
async def apply_transaction(tx: Transaction):
    t0 = time.perf_counter()
    doc = await balances.find_one_and_update(
        {"user_id": tx.user_id},
        {"$inc": {"balance": tx.amount}},
        upsert=True,
        return_document=True,
    )
    new_balance = doc["balance"]
    processing_ms = (time.perf_counter() - t0) * 1000
    # Removed print statement
    return {"balance": new_balance, "processing_ms": processing_ms}


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
    uvicorn.run("app:app", host="0.0.0.0", port=8002, workers=4)