"""The only HTTP of the worker (norm 5.7.1): GET /health with the last run of every job. It is 200
only when every check answers (RabbitMQ) and no job has gone three of its periods (plus the run
timeout of grace) without a successful run; otherwise 503 with the common envelope and one detail
per problem, so the container's health check and the operator see a stuck worker. Standard
library server; anything else is 404 with the common envelope."""

import json
import threading
import uuid
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from worker.adapter.inbound.scheduler import Scheduler


def serve_health(
    scheduler: Scheduler,
    port: int,
    version: str = "2.0.0",
    checks: dict[str, Callable[[], bool]] | None = None,
) -> ThreadingHTTPServer:
    checks = checks or {}

    def health(correlation: str) -> tuple[int, dict]:
        problems = [{"field": name, "message": f"{name} is not reachable"} for name, ok in checks.items() if not ok()]
        problems += [
            {"field": job, "message": f"{job} has not succeeded for {age:.0f}s"}
            for job, age in scheduler.stale().items()
        ]
        if problems:
            message = "; ".join(p["message"] for p in problems)
            return 503, {
                "error": "SERVICE_UNAVAILABLE",
                "message": message,
                "traceId": correlation,
                "details": problems,
            }
        return 200, {
            "status": "ok",
            "service": "qone-worker",
            "version": version,
            "checks": {name: "ok" for name in checks},
            "jobs": scheduler.last,
        }

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - the stdlib's name
            correlation = self.headers.get("X-Correlation-Id") or str(uuid.uuid4())
            if self.path == "/health":
                status, body = health(correlation)
            else:
                status, body = 404, {"error": "NOT_FOUND", "message": "resource not found", "traceId": correlation}
            data = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("X-Correlation-Id", correlation)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):  # the scheduler's JSON lines are the log
            pass

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)  # noqa: S104 - expose only on the platform network
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
