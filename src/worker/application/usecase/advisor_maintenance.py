"""The advisor's maintenance jobs (09-microservices/services/06-advisor-api, 06-data/models.md):
a PROPOSED plan whose 10 minutes passed becomes EXPIRED, so nobody can confirm it and the portal
says so (INV-ADV-003); LLM cache entries older than 7 days are deleted. Both work in bounded
batches until nothing is due or the run's deadline passes; what is left waits for the next run.
Both are idempotent: an expired plan is no longer PROPOSED and a deleted entry is gone."""

import logging

from worker.application.port.inbound.job import RunContext, RunReport
from worker.application.port.outbound.ports import AdvisorMaintenance, MonotonicClock

log = logging.getLogger("worker.advisor")


class _Batched:
    name = ""

    def __init__(self, clock: MonotonicClock, batch_size: int, every_seconds: float) -> None:
        self._clock = clock
        self._batch = batch_size
        self.every_seconds = every_seconds

    def _step(self, limit: int) -> int:
        raise NotImplementedError

    def run(self, context: RunContext) -> RunReport:
        processed, timed_out = 0, False
        try:
            while True:
                if self._clock.now() >= context.deadline:
                    timed_out = True
                    break
                done = self._step(self._batch)
                processed += done
                if done < self._batch:
                    break
        except Exception as error:  # noqa: BLE001 - an unreachable database is a failed run, retried next time
            log.error(
                "advisor database not reachable",
                extra={"traceId": context.correlation_id, "job": self.name, "detail": str(error)},
            )
            return RunReport(self.name, processed, 1, timed_out)
        return RunReport(self.name, processed, 0, timed_out)


class ExpirePlans(_Batched):
    name = "expire-plans"

    def __init__(
        self, store: AdvisorMaintenance, clock: MonotonicClock, batch_size: int = 100, every_seconds: float = 30.0
    ) -> None:
        super().__init__(clock, batch_size, every_seconds)
        self._store = store

    def _step(self, limit: int) -> int:
        return self._store.expire_plans(limit)


class PruneLlmCache(_Batched):
    name = "prune-llm-cache"

    def __init__(
        self,
        store: AdvisorMaintenance,
        clock: MonotonicClock,
        batch_size: int = 500,
        keep_days: int = 7,
        every_seconds: float = 3600.0,
    ) -> None:
        super().__init__(clock, batch_size, every_seconds)
        self._store = store
        self._days = keep_days

    def _step(self, limit: int) -> int:
        return self._store.prune_llm_cache(self._days, limit)
