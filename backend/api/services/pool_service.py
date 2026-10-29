"""
PoolService — application service layer for the Async Worker Pool.

Owns the pool singleton lifecycle, task submission, telemetry, and reset operations.
Controllers and CLI runners call only this layer — they never touch the pool directly.
"""

import asyncio
import random
import threading
from typing import List

from backend.core.pool import AsyncWorkerPool, AsyncTask


class PoolService:
    """
    Manages a shared AsyncWorkerPool instance. Thread-safe: the asyncio loop
    runs in a dedicated background thread, and all submissions are dispatched
    via run_coroutine_threadsafe.
    """

    def __init__(self, concurrency: int = 4):
        self._pool = AsyncWorkerPool(concurrency=concurrency)
        self._loop = asyncio.new_event_loop()
        self._started = False

    def start(self) -> None:
        """Boot the async pool in a daemon background thread."""
        if self._started:
            return
        t = threading.Thread(target=self._run_loop, daemon=True)
        t.start()
        self._started = True

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._pool.start())
        self._loop.run_forever()

    # ------------------------------------------------------------------ #
    #  Task Management                                                     #
    # ------------------------------------------------------------------ #
    def submit_tasks(self, count: int, task_type: str, failure_rate: float,
                     priority: int, duration: float) -> List[str]:
        """Create and submit <count> tasks; returns list of task IDs."""
        submitted_ids = []
        for _ in range(count):
            will_fail = random.random() < failure_rate
            task = AsyncTask(
                name=f"{task_type} #{random.randint(100, 999)}",
                fn=self._simulated_workload,
                args=(task_type, duration + random.uniform(-0.2, 0.4), will_fail),
                priority=priority,
                max_retries=2,
                backoff_base=0.15,
            )
            asyncio.run_coroutine_threadsafe(self._pool.submit(task), self._loop)
            submitted_ids.append(task.task_id)
        return submitted_ids

    @staticmethod
    async def _simulated_workload(task_type: str, duration: float, should_fail: bool):
        """Simulates realistic I/O work, network requests, or DB transactions."""
        await asyncio.sleep(duration)
        if should_fail:
            raise ConnectionResetError(f"Simulated transient error during {task_type}")
        return f"Successfully processed {task_type} (latency: {duration:.2f}s)"

    # ------------------------------------------------------------------ #
    #  Telemetry & Control                                                 #
    # ------------------------------------------------------------------ #
    def get_stats(self) -> dict:
        stats = self._pool.get_stats()
        stats["recent_tasks"] = [t.to_dict() for t in self._pool.task_history[-15:]]
        return stats

    def reset_telemetry(self) -> None:
        self._pool.processed_count = 0
        self._pool.failure_count = 0
        self._pool.retry_count = 0
        self._pool.task_history.clear()
