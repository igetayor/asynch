"""
HTTP server bootstrap for the Async Worker Pool.

Wires: PoolService → PoolController → HTTP handler → HTTPServer.
Frontend assets are served from frontend/.
"""

import os
import json
import sys
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse

from backend.api.services.pool_service import PoolService
from backend.api.controllers.pool_controller import PoolController

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

PORT = 8091
FRONTEND_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "frontend")

_service = PoolService(concurrency=4)
_controller = PoolController(service=_service)


class PoolHTTPRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=FRONTEND_DIR, **kwargs)

    def _set_headers(self, status=200, content_type="application/json"):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def _send_json(self, status: int, data):
        self._set_headers(status)
        self.wfile.write(json.dumps(data).encode("utf-8"))

    def _read_json_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length).decode("utf-8") if length > 0 else "{}"
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}

    def do_OPTIONS(self):
        self._set_headers(200)

    def do_GET(self):
        if urlparse(self.path).path == "/api/stats":
            status, data = _controller.get_stats()
            self._send_json(status, data)
        else:
            super().do_GET()

    def do_POST(self):
        path = urlparse(self.path).path
        body = self._read_json_body()

        if path == "/api/tasks/submit":
            status, data = _controller.submit_tasks(body)
            self._send_json(status, data)
        elif path == "/api/pool/clear":
            status, data = _controller.clear_telemetry()
            self._send_json(status, data)
        else:
            self._send_json(404, {"error": "Endpoint not found"})

    def log_message(self, fmt, *args):
        pass


def start_server(port: int = PORT):
    _service.start()
    server = HTTPServer(("0.0.0.0", port), PoolHTTPRequestHandler)
    print(f"[*] Async Worker Pool Server  →  http://127.0.0.1:{port}")
    print(f"    API: GET /api/stats | POST /api/tasks/submit | POST /api/pool/clear")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nGraceful shutdown.")
        server.server_close()
