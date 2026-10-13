"""The advisor's maintenance jobs (09-microservices/services/06-advisor-api, 06-data/models.md): a
PROPOSED plan expires 10 minutes after its proposal (INV-ADV-003) and the LLM cache keeps 7 days.
Bounded batches until nothing is due or the run's deadline passes; an unreachable database is a
failed run, never a crash. Plus the scheduler's own period per job."""

from tests.fakes import Clock
from worker.adapter.inbound.scheduler import Scheduler
from worker.application.port.inbound.job import RunContext, RunReport
from worker.application.usecase.advisor_maintenance import ExpirePlans, PruneLlmCache


class Maintenance:
    def __init__(self, due_plans=0, old_entries=0, broken=False):
        self.due_plans, self.old_entries, self.broken = due_plans, old_entries, broken
        self.calls: list[tuple] = []

    def expire_plans(self, limit):
        self.calls.append(("expire", limit))
        if self.broken:
            raise ConnectionError("advisor-db down")
        done = min(limit, self.due_plans)
        self.due_plans -= done
        return done

    def prune_llm_cache(self, older_than_days, limit):
        self.calls.append(("prune", older_than_days, limit))
        done = min(limit, self.old_entries)
        self.old_entries -= done
        return done


def context(deadline=1000.0):
    return RunContext("worker-run-1", deadline)


def test_due_plans_are_expired_in_bounded_batches_until_none_is_left():
    store = Maintenance(due_plans=5)
    report = ExpirePlans(store, Clock(), batch_size=2).run(context())
    assert report == RunReport("expire-plans", 5, 0)
    assert store.calls == [("expire", 2), ("expire", 2), ("expire", 2)]


def test_nothing_due_is_one_cheap_query():
    store = Maintenance()
    assert ExpirePlans(store, Clock(), batch_size=100).run(context()).processed == 0
    assert store.calls == [("expire", 100)]


def test_the_deadline_stops_the_run_and_the_rest_waits_for_the_next_one():
    store = Maintenance(due_plans=10)
    report = ExpirePlans(store, Clock(), batch_size=2).run(context(deadline=2.5))
    assert report.timed_out and report.processed == 4 and store.due_plans == 6


def test_an_unreachable_database_is_a_failed_run():
    report = ExpirePlans(Maintenance(broken=True), Clock(), batch_size=2).run(context())
    assert (report.processed, report.failed) == (0, 1)


def test_cache_entries_older_than_seven_days_are_pruned():
    store = Maintenance(old_entries=3)
    report = PruneLlmCache(store, Clock(), batch_size=500, keep_days=7).run(context())
    assert report == RunReport("prune-llm-cache", 3, 0)
    assert store.calls[0] == ("prune", 7, 500)


class Job:
    def __init__(self, name, every=None):
        self.name, self.runs = name, 0
        if every is not None:
            self.every_seconds = every

    def run(self, context):
        self.runs += 1
        return RunReport(self.name, 0, 0)


def test_each_job_runs_on_its_own_period_and_the_first_tick_runs_them_all():
    now = [0.0]
    relay, expire, prune = Job("relay-outbox"), Job("expire-plans", every=30), Job("prune-llm-cache", every=3600)
    scheduler = Scheduler([relay, expire, prune], every_seconds=5, run_timeout_seconds=20, monotonic=lambda: now[0])
    for tick in range(0, 65, 5):  # one minute of 5 s ticks
        now[0] = float(tick)
        scheduler.run_once()
    assert (relay.runs, expire.runs, prune.runs) == (13, 3, 1)
