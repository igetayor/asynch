"""
Entrypoint for the Async Worker Pool.

Usage:
  python -m cmd.main                  # Run CLI benchmark (12 tasks, 4 workers)
  python -m cmd.main --serve          # Launch web dashboard on port 8091
  python -m cmd.main -c 8 -n 30      # Benchmark with 8 workers, 30 tasks
"""

import sys
import argparse
import asyncio
import random

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


async def _simulate_api_call(user_id: int):
    await asyncio.sleep(0.06)
    if user_id % 7 == 0:
        if random.random() < 0.65:
            raise ConnectionResetError("Upstream payment gateway TCP reset")
    return f"Processed billing transaction for entity #{user_id}"


async def run_benchmark(concurrency: int = 4, total_tasks: int = 15):
    from backend.core.pool import AsyncWorkerPool, AsyncTask

    print("=" * 65)
    print("[*] Ige AsyncIO Worker Pool -- Bounded Concurrency & Telemetry")
    print("=" * 65)

    pool = AsyncWorkerPool(concurrency=concurrency)
    await pool.start()
    print(f"\n[1] Initialized {concurrency} concurrent coroutine workers.")

    tasks = []
    print(f"\n[2] Enqueueing {total_tasks} asynchronous background jobs...")
    for i in range(1, total_tasks + 1):
        priority = 1 if i % 5 == 0 else 2
        t = AsyncTask(
            task_id=f"job_{i:03d}",
            name=f"Transaction #{i:03d}",
            fn=_simulate_api_call,
            args=(i,),
            priority=priority,
            max_retries=2,
            backoff_base=0.1
        )
        tasks.append(t)
        await pool.submit(t)

    print(f"All {total_tasks} jobs enqueued. Processing...\n")
    await pool.stop(drain=True)

    print("[3] Job Execution Summary:")
    for t in tasks:
        status = "COMPLETED" if t.status.value == "COMPLETED" else f"FAILED ({t.error})"
        retries = f"(Retried: {t.retries}x)" if t.retries > 0 else ""
        print(f" - [P{t.priority}] {t.task_id} ({t.name}): {status} {retries} -> {t.execution_duration_ms:.1f}ms")

    stats = pool.get_stats()
    print(f"\n[4] Execution Telemetry:")
    print(f"   - Successfully Completed: {stats['processed_count']}")
    print(f"   - Permanent Failures:     {stats['failure_count']}")
    print(f"   - Total Retry Attempts:   {stats['retry_count']}")
    print("\n[+] Zero Redis or Celery dependencies. Clean, resilient Python concurrency.")


def main():
    parser = argparse.ArgumentParser(description="Ige Async Worker Pool")
    parser.add_argument("--concurrency", "-c", type=int, default=4)
    parser.add_argument("--tasks", "-n", type=int, default=12)
    parser.add_argument("--serve", "-s", action="store_true")
    parser.add_argument("--port", "-p", type=int, default=8091)
    args = parser.parse_args()

    if args.serve:
        from backend.api.server import start_server
        start_server(args.port)
    else:
        asyncio.run(run_benchmark(concurrency=args.concurrency, total_tasks=args.tasks))


if __name__ == "__main__":
    main()
