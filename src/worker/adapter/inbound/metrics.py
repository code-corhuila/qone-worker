"""The worker's metrics (HU-OBS-001, ADR-010), served at GET /metrics on the health port for the
Prometheus of qone-infra (`qone-worker:8000/metrics`). The scheduler hands every finished run to
`run_finished`; the application only reports what it did, this adapter measures it. One registry
per instance, so tests never share a series."""

import time

from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Histogram, generate_latest

from worker.application.port.inbound.job import RunReport

SERVICE = "qone-worker"
BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2, 3, 5, 10)
EXPIRE_RESERVATIONS = "expire-reservations"


class WorkerMetrics:
    content_type = CONTENT_TYPE_LATEST

    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self._runs = Counter(
            "worker_job_runs", "Runs of every job by outcome (ok/failed)", ["job", "outcome"], registry=self.registry
        )
        self._relayed = Counter(
            "worker_events_relayed", "Events published from each domain outbox", ["source"], registry=self.registry
        )
        self._expired = Counter(
            "worker_reservations_expired", "Seat reservations expired through CA-13", registry=self.registry
        )
        self._requests = Counter(
            "http_server_requests",
            "HTTP requests served, by route and status class",
            ["service", "method", "route", "status_class"],
            registry=self.registry,
        )
        self._duration = Histogram(
            "http_server_request_duration_seconds",
            "Time to serve an HTTP request, by route",
            ["service", "method", "route"],
            buckets=BUCKETS,
            registry=self.registry,
        )

    def run_finished(self, job: str, report: RunReport | None) -> None:
        """`report` is None when the run raised."""
        self._runs.labels(job, "failed" if report is None else "ok").inc()
        if report is None:
            return
        for source, published in report.processed_by:
            self._relayed.labels(source).inc(published)
        if job == EXPIRE_RESERVATIONS and report.processed:
            self._expired.inc(report.processed)

    def request_served(self, method: str, route: str, status: int, started: float) -> None:
        self._requests.labels(SERVICE, method, route, f"{status // 100}xx").inc()
        self._duration.labels(SERVICE, method, route).observe(time.perf_counter() - started)

    def exposition(self) -> bytes:
        return generate_latest(self.registry)
