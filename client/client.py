import argparse
import asyncio
import time
import httpx

FACADE_URL = "http://localhost:8000"


async def run(user_id: str, amount: float, n: int):
    async with httpx.AsyncClient(timeout=30.0) as client:
        start = time.perf_counter()
        for _ in range(n):
            await client.post(f"{FACADE_URL}/transaction", json={"user_id": user_id, "amount": amount})
        elapsed = time.perf_counter() - start

    print(f"Sent {n} requests in {elapsed:.2f}s — {n/elapsed:.1f} req/s")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user",   default="alice")
    parser.add_argument("--amount", type=float, default=1.0)
    parser.add_argument("--n",      type=int,   default=100)
    parser.add_argument("--url",    default=FACADE_URL)
    args = parser.parse_args()

    global FACADE_URL
    FACADE_URL = args.url

    asyncio.run(run(args.user, args.amount, args.n))


if __name__ == "__main__":
    main()