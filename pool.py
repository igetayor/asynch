import asyncio
import logging
from typing import Callable, Any, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class AsyncTask:
    def __init__(self, task_id: str, fn: Callable, args: tuple = (), max_retries: int = 3, backoff_base: float = 0.1):
        self.task_id = task_id
        self.fn = fn
        self.args = args
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.retries = 0
        self.result = None
        self.error = None
        self.completed = False

class AsyncWorkerPool:
    """
    High-throughput in-memory async worker pool.
    Features:
    - Bounded worker concurrency via asyncio.Queue & asyncio.Semaphore
    - Exponential backoff retries on failure
    - Graceful cancellation & queue draining
    """
    def __init__(self, concurrency: int = 4):
        self.concurrency = concurrency
        self.queue: asyncio.Queue[AsyncTask] = asyncio.Queue()
        self.workers: list[asyncio.Task] = []
        self.running = False
        self.processed_count = 0
        self.failure_count = 0

    async def start(self):
        self.running = True
        self.workers = [
            asyncio.create_task(self._worker_loop(i)) for i in range(self.concurrency)
        ]

    async def submit(self, task: AsyncTask):
        await self.queue.put(task)

    async def _worker_loop(self, worker_id: int):
        while self.running:
            try:
                task = await self.queue.get()
            except asyncio.CancelledError:
                break

            await self._process_task(worker_id, task)
            self.queue.task_done()

    async def _process_task(self, worker_id: int, task: AsyncTask):
        while task.retries <= task.max_retries:
            try:
                if asyncio.iscoroutinefunction(task.fn):
                    task.result = await task.fn(*task.args)
                else:
                    task.result = task.fn(*task.args)
                task.completed = True
                self.processed_count += 1
                return
            except Exception as e:
                task.retries += 1
                task.error = e
                if task.retries <= task.max_retries:
                    delay = task.backoff_base * (2 ** (task.retries - 1))
                    await asyncio.sleep(delay)
                else:
                    self.failure_count += 1
                    return

    async def stop(self):
        """Gracefully wait for pending items and shut down workers."""
        self.running = False
        await self.queue.join()
        for w in self.workers:
            w.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)
