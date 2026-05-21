#!/usr/bin/env python3
import argparse
import asyncio
import time
import httpx

FACADE_URL = "http://localhost:8000"
N = 10000
CLIENTS = 10
SEMAPHORE = asyncio.Semaphore(200) 


async def worker(user_id: str, n: int):
    async with httpx.AsyncClient(timeout=60.0) as client:
        for _ in range(n):
            async with SEMAPHORE:
                for attempt in range(5):
                    try:
                        await client.post(
                            f"{FACADE_URL}/transaction",
                            json={"user_id": user_id, "amount": 1}
                        )
                        break
                    except Exception:
                        await asyncio.sleep(0.1 * (attempt + 1))


async def run_scenario(scenario: int):
    print(f"\nScenario {scenario} — {CLIENTS} clients x {N:,} transactions")

    if scenario == 1:
        tasks = [worker(f"user_{i}", N) for i in range(CLIENTS)]
    else:
        tasks = [worker("shared_user", N) for _ in range(CLIENTS)]

    start = time.perf_counter()
    await asyncio.gather(*tasks)
    elapsed = time.perf_counter() - start

    total = CLIENTS * N
    print(f"Total time : {elapsed:.2f}s")
    print(f"Req/s      : {total/elapsed:.1f}")

    async with httpx.AsyncClient(timeout=10.0) as client:
        stats = (await client.get(f"{FACADE_URL}/stats")).json()
        accounts = (await client.get(f"{FACADE_URL}/accounts")).json()

    print(f"Logging  network avg : {stats['logging_network_avg_ms']:.2f}ms")
    print(f"Counter  network avg : {stats['counter_network_avg_ms']:.2f}ms")
    print(f"Logging  processing avg : {stats['logging_processing_avg_ms']:.4f}ms")
    print(f"Counter  processing avg : {stats['counter_processing_avg_ms']:.4f}ms")

    balances = accounts["balances"]
    print("\nCorrectness check:")
    if scenario == 1:
        for i in range(CLIENTS):
            uid = f"user_{i}"
            bal = balances.get(uid, "MISSING")
            print(f"  {uid}: {bal} {'CORRECT' if bal == float(N) else 'WRONG'}")
    else:
        bal = balances.get("shared_user", "MISSING")
        print(f"  shared_user: {bal} {'CORRECT' if bal == float(N * CLIENTS) else 'WRONG'}")


def main():
    global FACADE_URL
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenario", type=int, choices=[1, 2], required=True)
    parser.add_argument("--url", default="http://localhost:8000")
    args = parser.parse_args()
    FACADE_URL = args.url
    asyncio.run(run_scenario(args.scenario))


if __name__ == "__main__":
    main()