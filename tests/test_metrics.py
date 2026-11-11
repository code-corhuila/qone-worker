"""HU-OBS-001: the worker's metrics on its health port (GET /metrics, Prometheus text): runs per job
and outcome, events relayed per source outbox, reservations expired, and the requests of its own
HTTP like every other service."""

import urllib.error
import urllib.request

from tests.fakes import Clock, ListOutbox, Publisher, event
from worker.adapter.inbound.health import serve_health
from worker.adapter.inbound.metrics import WorkerMetrics
from worker.adapter.inbound.scheduler import Scheduler
from worker.application.port.inbound.job import RunReport
from worker.application.usecase.relay_outbox import RelayOutbox


class Job:
    def __init__(self, name, processed=0, fail=False):
        self.name, self.processed, self.fail = name, processed, fail

    def run(self, context):
        if self.fail:
            raise RuntimeError("boom")
        return RunReport(self.name, self.processed, 0)


def sample(text: str, name: str, *labels: str) -> float:
    for line in text.splitlines():
        if line.startswith(name) and all(label in line for label in labels):
            return float(line.rsplit(" ", 1)[1])
    raise AssertionError(f"no {name} sample with {labels}")


def test_runs_are_counted_per_job_and_outcome_and_expired_reservations_are_summed():
    metrics = WorkerMetrics()
    jobs = [Job("relay-outbox"), Job("broken", fail=True), Job("expire-reservations", processed=3)]
    scheduler = Scheduler(jobs, 5, 20, on_run=metrics.run_finished)
    scheduler.run_once()
    scheduler.run_once()
    text = metrics.exposition().decode()
    assert sample(text, "worker_job_runs_total", 'job="relay-outbox"', 'outcome="ok"') == 2
    assert sample(text, "worker_job_runs_total", 'job="broken"', 'outcome="failed"') == 2
    assert sample(text, "worker_reservations_expired_total") == 6


def test_events_relayed_are_counted_per_source_outbox():
    catalog = ListOutbox("catalog", [event(n=1), event(n=2)])
    billing = ListOutbox("billing", [event("billing", "TuitionPaid", n=3)])
    metrics = WorkerMetrics()
    relay = RelayOutbox([catalog, billing], Publisher(), Clock(), batch_size=10)
    Scheduler([relay], 5, 20, on_run=metrics.run_finished).run_once()
    text = metrics.exposition().decode()
    assert sample(text, "worker_events_relayed_total", 'source="catalog"') == 2
    assert sample(text, "worker_events_relayed_total", 'source="billing"') == 1


def test_metrics_on_the_health_port_count_its_own_requests_but_not_the_scrape():
    metrics = WorkerMetrics()
    server = serve_health(Scheduler([], 5, 20), 0, metrics=metrics)
    base = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        urllib.request.urlopen(f"{base}/health").read()
        try:
            urllib.request.urlopen(f"{base}/nothing/42")
        except urllib.error.HTTPError:
            pass
        urllib.request.urlopen(f"{base}/metrics").read()
        with urllib.request.urlopen(f"{base}/metrics") as r:
            assert r.status == 200 and r.headers["Content-Type"].startswith("text/plain")
            text = r.read().decode()
    finally:
        server.shutdown()
        server.server_close()
    labels = ('service="qone-worker"', 'method="GET"')
    assert sample(text, "http_server_requests_total", *labels, 'route="/health"', 'status_class="2xx"') == 1
    assert sample(text, "http_server_requests_total", 'route="unmatched"', 'status_class="4xx"') == 1
    assert sample(text, "http_server_request_duration_seconds_count", 'route="/health"') == 1
    assert 'route="/metrics"' not in text and "/nothing/42" not in text
    assert "# TYPE worker_job_runs_total counter" in text
