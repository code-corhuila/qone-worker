"""The only HTTP of the worker (norm 5.7.1): GET /health with the last run of every job and, with
the metrics, GET /metrics for Prometheus (HU-OBS-001; the scrape itself is not counted). Standard
library server; anything else is 404 with the common envelope."""

import json
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from worker.adapter.inbound.metrics import WorkerMetrics
from worker.adapter.inbound.scheduler import Scheduler


def serve_health(
    scheduler: Scheduler, port: int, version: str = "2.0.0", metrics: WorkerMetrics | None = None
) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802 - the stdlib's name
            if metrics is not None and self.path == "/metrics":
                data = metrics.exposition()
                self.send_response(200)
                self.send_header("Content-Type", metrics.content_type)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            started = time.perf_counter()
            correlation = self.headers.get("X-Correlation-Id") or str(uuid.uuid4())
            route = "/health" if self.path == "/health" else "unmatched"
            if self.path == "/health":
                status, body = (
                    200,
                    {"status": "ok", "service": "qone-worker", "version": version, "jobs": scheduler.last},
                )
            else:
                status, body = 404, {"error": "NOT_FOUND", "message": "resource not found", "traceId": correlation}
            data = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("X-Correlation-Id", correlation)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            if metrics is not None:
                metrics.request_served("GET", route, status, started)

        def log_message(self, *args):  # the scheduler's JSON lines are the log
            pass

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)  # noqa: S104 - expose only on the platform network
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
