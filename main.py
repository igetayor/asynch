"""
CLI runner and demonstration harness for the Ige Async Worker Pool.
Allows running benchmark simulations directly or serving the web dashboard.
"""

import sys
import argparse
import asyncio
import random
from pool import AsyncWorkerPool, AsyncTask

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


async def simulate_api_call(user_id: int):
    # Simulate slight network jitter
    await asyncio.sleep(0.06)
    if user_id % 7 == 0:
        if random.random() < 0.65:
            raise ConnectionResetError("Upstream payment gateway TCP reset")
    return f"Processed billing transaction for entity #{user_id}"


async def run_benchmark(concurrency: int = 4, total_tasks: int = 15):
    print("=" * 65)
    print("[*] Ige AsyncIO Worker Pool -- Bounded Concurrency & Telemetry")
    print("=" * 65)

    pool = AsyncWorkerPool(concurrency=concurrency)
    await pool.start()
    print(f"\n[1] Initialized {concurrency} concurrent coroutine workers.")

    tasks = []
    print(f"\n[2] Enqueueing {total_tasks} asynchronous background jobs...")
    for i in range(1, total_tasks + 1):
        # Alternate priorities
        priority = 1 if i % 5 == 0 else 2
        t = AsyncTask(
            task_id=f"job_{i:03d}",
            name=f"Transaction #{i:03d}",
            fn=simulate_api_call,
            args=(i,),
            priority=priority,
            max_retries=2,
            backoff_base=0.1
        )
        tasks.append(t)
        await pool.submit(t)

    print(f"All {total_tasks} jobs enqueued. Processing with bounded concurrency...\n")
    await pool.stop(drain=True)

    print("[3] Job Execution Summary:")
    for t in tasks:
        status = "COMPLETED" if t.status.value == "COMPLETED" else f"FAILED ({t.error})"
        retries = f"(Retried: {t.retries}x)" if t.retries > 0 else ""
        priority_lbl = f"[P{t.priority}]"
        print(f" - {priority_lbl} {t.task_id} ({t.name}): {status} {retries} -> {t.execution_duration_ms:.1f}ms")

    stats = pool.get_stats()
    print(f"\n[4] Execution Telemetry:")
    print(f"   - Successfully Completed: {stats['processed_count']}")
    print(f"   - Permanent Failures:     {stats['failure_count']}")
    print(f"   - Total Retry Attempts:   {stats['retry_count']}")
    print("\n[+] Zero Redis or Celery dependencies required. Clean, resilient Python concurrency.")


def main():
    parser = argparse.ArgumentParser(description="Ige Async Worker Pool Runner")
    parser.add_argument("--concurrency", "-c", type=int, default=4, help="Number of worker slots")
    parser.add_argument("--tasks", "-n", type=int, default=12, help="Number of jobs to dispatch")
    parser.add_argument("--serve", "-s", action="store_true", help="Launch embedded web server on port 8091")
    parser.add_argument("--port", "-p", type=int, default=8091, help="Port to listen on in serve mode")

    args = parser.parse_args()

    if args.serve:
        from server import start_server
        start_server(args.port)
    else:
        asyncio.run(run_benchmark(concurrency=args.concurrency, total_tasks=args.tasks))


if __name__ == "__main__":
    main()
