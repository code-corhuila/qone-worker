"""HU-PLT-003, a resilient worker: /health is 503 with the common envelope when RabbitMQ does not
answer or a job has not succeeded for three of its periods (plus the run timeout of grace), and
the worker stops gracefully on SIGTERM and SIGINT: no new run starts, the current one finishes,
the health server, the pools and the AMQP connection are closed."""

import json
import signal
import socket
import threading
import urllib.error
import urllib.request

import pytest
from apps.worker.__main__ import install_signals, shutdown

from worker.adapter.inbound.health import serve_health
from worker.adapter.inbound.scheduler import Scheduler
from worker.adapter.outbound.rabbitmq import broker_reachable
from worker.application.port.inbound.job import RunReport


class Job:
    def __init__(self, name, every_seconds=None, fail=False):
        self.name, self.fail, self.runs = name, fail, 0
        if every_seconds is not None:
            self.every_seconds = every_seconds

    def run(self, context):
        self.runs += 1
        if self.fail:
            raise RuntimeError("boom")
        return RunReport(self.name, 0, 0)


def get(port, path="/health"):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


@pytest.fixture
def served():
    servers = []

    def serve(scheduler, **checks):
        server = serve_health(scheduler, 0, checks=checks)
        servers.append(server)
        return server.server_address[1]

    yield serve
    for server in servers:
        server.shutdown()
        server.server_close()


def test_health_is_200_with_the_checks_and_the_jobs_when_all_is_well(served):
    scheduler = Scheduler([Job("relay-outbox")], every_seconds=5, run_timeout_seconds=20)
    scheduler.run_once()
    status, body = get(served(scheduler, rabbitmq=lambda: True))
    assert status == 200 and body["status"] == "ok" and body["checks"] == {"rabbitmq": "ok"}
    assert body["service"] == "qone-worker" and body["jobs"]["relay-outbox"]["ok"] is True


def test_health_is_503_with_the_envelope_when_rabbitmq_does_not_answer(served):
    scheduler = Scheduler([Job("relay-outbox")], every_seconds=5, run_timeout_seconds=20)
    status, body = get(served(scheduler, rabbitmq=lambda: False))
    assert status == 503 and body["error"] == "SERVICE_UNAVAILABLE" and body["traceId"]
    assert body["details"] == [{"field": "rabbitmq", "message": "rabbitmq is not reachable"}]


def test_a_job_without_success_for_three_periods_makes_health_503(served):
    now = [0.0]
    sweep = Job("expire-reservations", every_seconds=60)
    scheduler = Scheduler([sweep], every_seconds=5, run_timeout_seconds=20, monotonic=lambda: now[0])
    port = served(scheduler, rabbitmq=lambda: True)
    scheduler.run_once()
    sweep.fail = True
    now[0] = 200.0  # 3 x 60 s + 20 s of grace since the last success: still healthy
    scheduler.run_once()
    assert get(port)[0] == 200
    now[0] = 201.0
    status, body = get(port)
    assert status == 503 and body["details"][0]["field"] == "expire-reservations"
    sweep.fail = False
    now[0] = 260.0
    scheduler.run_once()
    assert get(port)[0] == 200


def test_a_job_that_never_succeeded_is_judged_from_the_start():
    now = [0.0]
    scheduler = Scheduler([Job("relay-outbox", fail=True)], 5, 20, monotonic=lambda: now[0])
    scheduler.run_once()
    assert scheduler.stale() == {}
    now[0] = 36.0
    assert scheduler.stale() == {"relay-outbox": 36.0}


def test_stop_lets_the_current_run_finish_and_starts_no_other_job():
    scheduler = Scheduler([], 5, 20)

    class Stopping(Job):
        def run(self, context):
            scheduler.stop()  # SIGTERM arrives while this run is in progress
            return super().run(context)

    first, second = Stopping("relay-outbox"), Job("expire-reservations")
    scheduler._jobs = [first, second]
    thread = threading.Thread(target=scheduler.serve_forever)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive() and first.runs == 1 and second.runs == 0


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGINT])
def test_sigterm_and_sigint_stop_the_scheduler(signum):
    scheduler = Scheduler([], 5, 20)
    previous = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        install_signals(scheduler)
        signal.raise_signal(signum)
        assert scheduler.stopped
    finally:
        for s, handler in previous.items():
            signal.signal(s, handler)


def test_shutdown_closes_the_server_and_every_resource_even_if_one_fails():
    closed = []

    class Resource:
        def __init__(self, name, broken=False):
            self.name, self.broken = name, broken

        def close(self):
            closed.append(self.name)
            if self.broken:
                raise RuntimeError("already gone")

    server = serve_health(Scheduler([], 5, 20), 0)
    shutdown(server, [Resource("catalog-outbox", broken=True), Resource("rabbitmq")])
    assert closed == ["catalog-outbox", "rabbitmq"]
    with pytest.raises(OSError):
        socket.create_connection(server.server_address, timeout=1)


def test_the_broker_probe_answers_false_quickly_when_nothing_listens():
    assert broker_reachable("amqp://guest:guest@127.0.0.1:1/%2F", timeout_seconds=0.5) is False
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    try:
        port = listener.getsockname()[1]
        assert broker_reachable(f"amqp://guest:guest@127.0.0.1:{port}/%2F", timeout_seconds=0.5) is True
    finally:
        listener.close()
