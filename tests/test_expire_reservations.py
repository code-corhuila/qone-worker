"""HU-PLT-003, core level: the job `expire-reservations` asks the catalog (CA-13) for bounded
batches with its run's correlation id until a batch comes back short or the deadline passes;
a catalog that does not answer is a failed run that the next run tries again (Annex D)."""

import pytest

from tests.fakes import Clock
from worker.adapter.inbound.scheduler import Scheduler
from worker.application.port.inbound.job import RunContext, RunReport
from worker.application.usecase.expire_reservations import ExpireReservations


class Catalog:
    def __init__(self, due=0, failing=0, down=False):
        self.due, self.failing, self.down = due, failing, down
        self.calls: list[tuple[int, str]] = []

    def expire_overdue(self, limit, correlation_id):
        self.calls.append((limit, correlation_id))
        if self.down:
            raise ConnectionError("catalog not reachable after 3 attempts")
        failed = min(self.failing, limit)
        expired = min(self.due, limit - failed)
        self.due -= expired
        return expired, failed


def context(deadline=1000.0):
    return RunContext("worker-expire-reservations-1", deadline)


def test_overdue_reservations_are_expired_in_bounded_batches_with_the_run_correlation():
    catalog = Catalog(due=5)
    report = ExpireReservations(catalog, Clock(), batch_size=2).run(context())
    assert report == RunReport("expire-reservations", 5, 0)
    assert catalog.calls == [(2, "worker-expire-reservations-1")] * 3


def test_failures_reported_by_the_catalog_are_counted_and_the_run_does_not_loop_on_them():
    catalog = Catalog(due=1, failing=1)
    report = ExpireReservations(catalog, Clock(), batch_size=2).run(context())
    assert report == RunReport("expire-reservations", 1, 1) and len(catalog.calls) == 1


def test_a_run_past_its_deadline_asks_for_nothing_more():
    catalog = Catalog(due=10)
    report = ExpireReservations(catalog, Clock(), batch_size=2).run(context(deadline=1.5))
    assert report.timed_out and report.processed == 2 and len(catalog.calls) == 1


def test_a_catalog_that_does_not_answer_fails_the_run_and_the_next_run_tries_again():
    catalog = Catalog(due=1, down=True)
    job = ExpireReservations(catalog, Clock(), batch_size=100, every_seconds=60)
    with pytest.raises(ConnectionError):
        job.run(context())
    now = [0.0]
    scheduler = Scheduler([job], every_seconds=5, run_timeout_seconds=20, monotonic=lambda: now[0])
    scheduler.run_once()
    assert scheduler.last["expire-reservations"]["ok"] is False
    catalog.down = False
    now[0] = 60.0
    scheduler.run_once()
    assert scheduler.last["expire-reservations"]["processed"] == 1
    assert catalog.calls[-1][1].startswith("worker-expire-reservations-")
    assert catalog.calls[-1][1] != catalog.calls[-2][1]
