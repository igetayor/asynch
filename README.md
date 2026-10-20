# Ige Async Worker Pool — Pure Python Task Orchestrator

A high-performance, asynchronous background task manager implemented with pure Python standard library `asyncio`. Built to process in-memory tasks with bounded concurrency, exponential backoff retries, and graceful cancellation without requiring heavy external dependencies like Redis or Celery.

## Features
- **Bounded Worker Concurrency**: Uses internal `asyncio.Queue` and task pools to cap CPU/network exhaustion.
- **Automated Exponential Backoff**: Retries failed coroutines or synchronous functions with backoff delays.
- **Graceful Draining**: Waits for enqueued tasks to complete cleanly upon shutdown.

## Quick Start
```bash
# Run demo
python main.py

# Run test suite
pytest test_pool.py -v
```
