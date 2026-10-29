"""
PoolController — HTTP controller for the Async Worker Pool REST endpoints.

Parses request payloads, delegates to PoolService, and returns typed responses.
Zero business logic or asyncio concerns live here.
"""

from backend.api.services.pool_service import PoolService


class PoolController:
    """Maps HTTP requests to PoolService calls."""

    def __init__(self, service: PoolService):
        self._service = service

    # ------------------------------------------------------------------ #
    #  GET /api/stats                                                      #
    # ------------------------------------------------------------------ #
    def get_stats(self) -> tuple[int, dict]:
        return 200, self._service.get_stats()

    # ------------------------------------------------------------------ #
    #  POST /api/tasks/submit                                              #
    # ------------------------------------------------------------------ #
    def submit_tasks(self, body: dict) -> tuple[int, dict]:
        count = int(body.get("count", 1))
        task_type = body.get("type", "Data Processing")
        failure_rate = float(body.get("failure_rate", 0.2))
        priority = int(body.get("priority", 2))
        duration = float(body.get("duration", 0.8))

        ids = self._service.submit_tasks(count, task_type, failure_rate, priority, duration)
        return 200, {"success": True, "submitted_count": count, "ids": ids}

    # ------------------------------------------------------------------ #
    #  POST /api/pool/clear                                                #
    # ------------------------------------------------------------------ #
    def clear_telemetry(self) -> tuple[int, dict]:
        self._service.reset_telemetry()
        return 200, {"success": True, "message": "Telemetry counters reset"}
