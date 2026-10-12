"""The scheduler (Annex D inbound adapter): runs every job on its period, gives each run a fresh
X-Correlation-Id and a deadline, writes one log line per run with its traceId, and keeps the last
report of each job for the health endpoint. A job that raises is logged and runs again next time."""

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
    ) -> None:
        self._jobs = jobs
        self._every = every_seconds
        self._timeout = run_timeout_seconds
        self._monotonic = monotonic
        self.last: dict[str, dict] = {}
        self._stop = threading.Event()

    def run_once(self) -> list[RunReport]:
        reports = []
        for job in self._jobs:
            context = RunContext(
                correlation_id=f"worker-{job.name}-{uuid.uuid4()}", deadline=self._monotonic() + self._timeout
            )
            try:
                report = job.run(context)
            except Exception:  # noqa: BLE001 - a failed run is logged; the next one tries again
                log.exception("run failed", extra={"traceId": context.correlation_id, "job": job.name})
                self.last[job.name] = {"ok": False, "traceId": context.correlation_id}
                continue
            reports.append(report)
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
