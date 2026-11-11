"""The scheduler (Annex D inbound adapter): runs every job on its period (every tick, or its own
`every_seconds`), gives each run a fresh X-Correlation-Id and a deadline, writes one log line per
run with its traceId, keeps the last report of each job for the health endpoint and hands every
finished run to `on_run` (the metrics). A job that raises is logged and runs again next time.
`stop` (SIGTERM, SIGINT) lets the run in progress finish and starts no other; `stale` names the
jobs without a successful run for three of their periods plus the run timeout of grace
(HU-PLT-003)."""

import logging
import threading
import time
import uuid
from collections.abc import Callable

from worker.application.port.inbound.job import Job, RunContext, RunReport

log = logging.getLogger("worker.scheduler")


class Scheduler:
    def __init__(
        self,
        jobs: list[Job],
        every_seconds: float,
        run_timeout_seconds: float,
        monotonic: Callable[[], float] = time.monotonic,
        on_run: Callable[[str, RunReport | None], None] | None = None,
    ) -> None:
        self._on_run = on_run or (lambda job, report: None)
        self._jobs = jobs
        self._every = every_seconds
        self._timeout = run_timeout_seconds
        self._monotonic = monotonic
        self.last: dict[str, dict] = {}
        self._next_due: dict[str, float] = {}
        self._stop = threading.Event()
        self._started = monotonic()
        self._succeeded: dict[str, float] = {}

    def run_once(self) -> list[RunReport]:
        reports = []
        for job in self._jobs:
            if self._stop.is_set():  # shutting down: the run in progress finished, no other starts
                break
            period = getattr(job, "every_seconds", None)
            if period is not None:
                now = self._monotonic()
                if now < self._next_due.get(job.name, now):
                    continue
                self._next_due[job.name] = now + period
            context = RunContext(
                correlation_id=f"worker-{job.name}-{uuid.uuid4()}", deadline=self._monotonic() + self._timeout
            )
            try:
                report = job.run(context)
            except Exception:  # noqa: BLE001 - a failed run is logged; the next one tries again
                log.exception("run failed", extra={"traceId": context.correlation_id, "job": job.name})
                self.last[job.name] = {"ok": False, "traceId": context.correlation_id}
                self._on_run(job.name, None)
                continue
            reports.append(report)
            self._succeeded[job.name] = self._monotonic()
            self._on_run(job.name, report)
            self.last[job.name] = {
                "ok": True,
                "processed": report.processed,
                "failed": report.failed,
                "traceId": context.correlation_id,
            }
            if report.processed or report.failed or report.timed_out:
                log.info(
                    "run",
                    extra={
                        "traceId": context.correlation_id,
                        "job": job.name,
                        "processed": report.processed,
                        "failed": report.failed,
                        "timedOut": report.timed_out,
                    },
                )
        return reports

    def serve_forever(self) -> None:
        while not self._stop.is_set():
            self.run_once()
            self._stop.wait(self._every)

    def stop(self) -> None:
        self._stop.set()

    @property
    def stopped(self) -> bool:
        return self._stop.is_set()

    def stale(self) -> dict[str, float]:
        """Seconds since the last success (or the start) of every job past 3 periods + the run timeout."""
        now = self._monotonic()
        late = {}
        for job in self._jobs:
            period = getattr(job, "every_seconds", None) or self._every
            age = now - self._succeeded.get(job.name, self._started)
            if age > 3 * period + self._timeout:
                late[job.name] = round(age, 1)
        return late
