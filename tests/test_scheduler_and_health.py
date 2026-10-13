"""The scheduler gives every run its own correlation and deadline and survives a failing job;
the health endpoint shows the last runs and answers 404 with the envelope elsewhere."""

import json
import urllib.error
import urllib.request

from worker.adapter.inbound.health import serve_health
from worker.adapter.inbound.scheduler import Scheduler
from worker.application.port.inbound.job import RunReport


class Job:
    def __init__(self, name, fail=False):
        self.name, self.fail, self.contexts = name, fail, []

    def run(self, context):
        self.contexts.append(context)
        if self.fail:
            raise RuntimeError("boom")
        return RunReport(self.name, 2, 1)


def test_every_run_has_its_own_correlation_and_deadline_and_a_failing_job_does_not_stop_the_others():
    ok, bad = Job("relay-outbox"), Job("broken", fail=True)
    scheduler = Scheduler([bad, ok], every_seconds=1, run_timeout_seconds=20, monotonic=lambda: 100.0)
    scheduler.run_once()
    scheduler.run_once()
    assert len(ok.contexts) == 2 and ok.contexts[0].correlation_id != ok.contexts[1].correlation_id
    assert ok.contexts[0].deadline == 120.0 and ok.contexts[0].correlation_id.startswith("worker-relay-outbox-")
    assert scheduler.last["relay-outbox"]["processed"] == 2 and scheduler.last["broken"]["ok"] is False


def test_the_health_endpoint():
    scheduler = Scheduler([Job("relay-outbox")], every_seconds=1, run_timeout_seconds=1)
    scheduler.run_once()
    server = serve_health(scheduler, 0)
    port = server.server_address[1]
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health") as r:
            body = json.loads(r.read())
        assert body["service"] == "qone-worker" and body["jobs"]["relay-outbox"]["failed"] == 1
        request = urllib.request.Request(f"http://127.0.0.1:{port}/nothing", headers={"X-Correlation-Id": "smoke-1"})
        try:
            urllib.request.urlopen(request)
        except urllib.error.HTTPError as error:
            assert error.code == 404 and json.loads(error.read())["traceId"] == "smoke-1"
    finally:
        server.shutdown()
