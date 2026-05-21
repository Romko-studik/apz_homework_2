import logging
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
app = FastAPI(title="Config Server")

registry: dict = defaultdict(list)


class ServiceRegistration(BaseModel):
    service: str
    url: str


@app.post("/register", status_code=201)
async def register(reg: ServiceRegistration):
    if reg.url not in registry[reg.service]:
        registry[reg.service].append(reg.url)
    print(f"[Config] Registered {reg.service} -> {reg.url}")
    print(f"[Config] Registry: {dict(registry)}")
    return {"status": "registered"}


@app.get("/services/{service}")
async def get_service(service: str):
    return {"service": service, "urls": registry.get(service, [])}


@app.get("/services")
async def get_all():
    return {"registry": dict(registry)}


@app.get("/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)