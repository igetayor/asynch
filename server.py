"""
Zero-dependency HTTP server for the Async Worker Pool.
Serves static frontend assets and provides a JSON REST API for task dispatching and telemetry.
"""

import os
import json
import asyncio
import threading
import random
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from pool import AsyncWorkerPool, AsyncTask, TaskStatus

PORT = 8091
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

# Shared global pool instance
pool = AsyncWorkerPool(concurrency=4)
loop = asyncio.new_event_loop()


def run_async_loop(l):
    asyncio.set_event_loop(l)
    l.run_until_complete(pool.start())
    l.run_forever()


async def simulated_workload(task_type: str, duration: float, should_fail: bool):
    """Simulates realistic I/O work, network requests, or database transactions."""
    await asyncio.sleep(duration)
    if should_fail:
        raise ConnectionResetError(f"Simulated transient error during {task_type}")
    return f"Successfully processed {task_type} (latency: {duration:.2f}s)"


class PoolHTTPRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=STATIC_DIR, **kwargs)

    def _set_headers(self, status=200, content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_OPTIONS(self):
        self._set_headers(200)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/stats":
            stats = pool.get_stats()
            # Attach recent history (last 15 tasks)
            stats["recent_tasks"] = [t.to_dict() for t in pool.task_history[-15:]]
            self._set_headers(200)
            self.wfile.write(json.dumps(stats).encode("utf-8"))
        elif parsed.path == "/" or parsed.path == "/index.html":
            # Check if static/index.html exists, otherwise fallback to project root index.html
            root_index = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
            target = os.path.join(STATIC_DIR, "index.html") if os.path.exists(os.path.join(STATIC_DIR, "index.html")) else root_index
            if os.path.exists(target):
                self._set_headers(200, "text/html")
                with open(target, "rb") as f:
                    self.wfile.write(f.read())
            else:
                super().do_GET()
        else:
            super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {}

        if parsed.path == "/api/tasks/submit":
            count = int(payload.get("count", 1))
            task_type = payload.get("type", "Data Processing")
            failure_rate = float(payload.get("failure_rate", 0.2))
            priority = int(payload.get("priority", 2))
            duration = float(payload.get("duration", 0.8))

            submitted_ids = []
            for i in range(count):
                will_fail = random.random() < failure_rate
                t = AsyncTask(
                    name=f"{task_type} #{random.randint(100, 999)}",
                    fn=simulated_workload,
                    args=(task_type, duration + random.uniform(-0.2, 0.4), will_fail),
                    priority=priority,
                    max_retries=2,
                    backoff_base=0.15
                )
                asyncio.run_coroutine_threadsafe(pool.submit(t), loop)
                submitted_ids.append(t.task_id)

            self._set_headers(200)
            self.wfile.write(json.dumps({"success": True, "submitted_count": count, "ids": submitted_ids}).encode("utf-8"))

        elif parsed.path == "/api/pool/clear":
            # Resets telemetry counters
            pool.processed_count = 0
            pool.failure_count = 0
            pool.retry_count = 0
            pool.task_history.clear()
            self._set_headers(200)
            self.wfile.write(json.dumps({"success": True, "message": "Telemetry counters reset"}).encode("utf-8"))
        else:
            self._set_headers(404)
            self.wfile.write(json.dumps({"error": "Endpoint not found"}).encode("utf-8"))


def start_server(port=PORT):
    # Start AsyncIO pool thread
    t = threading.Thread(target=run_async_loop, args=(loop,), daemon=True)
    t.start()

    server = HTTPServer(("0.0.0.0", port), PoolHTTPRequestHandler)
    print(f"🚀 Async Worker Pool Server running at http://127.0.0.1:{port}")
    print(f"   REST API: GET /api/stats | POST /api/tasks/submit | POST /api/pool/clear")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        server.server_close()


if __name__ == "__main__":
    start_server()
