import asyncio
import pytest
from pool import AsyncWorkerPool, AsyncTask

@pytest.mark.asyncio
async def test_worker_pool_concurrency_and_completion():
    pool = AsyncWorkerPool(concurrency=2)
    await pool.start()

    results = []
    async def sample_job(val):
        await asyncio.sleep(0.01)
        results.append(val)
        return val * 2

    t1 = AsyncTask("t1", sample_job, (5,))
    t2 = AsyncTask("t2", sample_job, (10,))

    await pool.submit(t1)
    await pool.submit(t2)
    await pool.stop()

    assert t1.completed is True
    assert t1.result == 10
    assert t2.completed is True
    assert t2.result == 20
    assert len(results) == 2

@pytest.mark.asyncio
async def test_exponential_backoff_retry():
    pool = AsyncWorkerPool(concurrency=1)
    await pool.start()

    attempts = 0
    def flaky_job():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ValueError("Transient error")
        return "success"

    t = AsyncTask("flaky", flaky_job, max_retries=3, backoff_base=0.01)
    await pool.submit(t)
    await pool.stop()

    assert t.completed is True
    assert t.result == "success"
    assert t.retries == 2
