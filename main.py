import asyncio
import random
from pool import AsyncWorkerPool, AsyncTask

async def simulate_api_call(user_id: int):
    # Simulate slight network jitter
    await asyncio.sleep(0.05)
    if user_id == 7: # Simulate intermittent flaky failure
        if random.random() < 0.6:
            raise ConnectionResetError("Upstream socket reset")
    return f"Processed invoice for user #{user_id}"

async def main():
    print("=" * 60)
    print("⚡ Ige AsyncIO Worker Pool — Bounded Concurrency & Retries")
    print("=" * 60)

    # Concurrency limit of 3 simultaneous workers
    pool = AsyncWorkerPool(concurrency=3)
    await pool.start()
    print("\n[1] Started 3 concurrent background workers.")

    tasks = []
    print("\n[2] Enqueueing 10 asynchronous background jobs...")
    for i in range(1, 11):
        t = AsyncTask(task_id=f"job_{i}", fn=simulate_api_call, args=(i,), max_retries=2)
        tasks.append(t)
        await pool.submit(t)

    print("All 10 jobs enqueued. Processing with bounded concurrency...\n")
    await pool.stop()

    print("[3] Job Execution Report:")
    for t in tasks:
        status = "COMPLETED" if t.completed else f"FAILED ({t.error})"
        retries = f"(Retries: {t.retries})" if t.retries > 0 else ""
        print(f" - {t.task_id}: {status} {retries} -> Result: {t.result}")

    print(f"\n[4] Pool Statistics: {pool.processed_count} completed, {pool.failure_count} permanent failures.")
    print("Zero Redis/Celery required. Lightweight, resilient Python async execution.")

if __name__ == "__main__":
    asyncio.run(main())
