"""The sweep of abandoned enrollment sagas (HU-PLT-003): a seat reservation still RESERVED after its
15 minutes goes back to its section. The rule lives in the catalog (CA-13); this job only asks for
it, through the catalog's API and never its database (Annex D rule 8), in batches of `BATCH_SIZE`
until a batch comes back short or the run's deadline passes. Safe to run twice: an expired
reservation is no longer overdue. A catalog that does not answer raises, so the scheduler logs the
run as failed with its correlation id and the next run tries again."""

from worker.application.port.inbound.job import RunContext, RunReport
from worker.application.port.outbound.ports import CatalogApi, MonotonicClock


class ExpireReservations:
    name = "expire-reservations"

    def __init__(
        self, catalog: CatalogApi, clock: MonotonicClock, batch_size: int = 100, every_seconds: float = 60.0
    ) -> None:
        self._catalog = catalog
        self._clock = clock
        self._batch = batch_size
        self.every_seconds = every_seconds

    def run(self, context: RunContext) -> RunReport:
        processed = failed = 0
        timed_out = False
        while True:
            if self._clock.now() >= context.deadline:
                timed_out = True
                break
            expired, missed = self._catalog.expire_overdue(self._batch, context.correlation_id)
            processed += expired
            failed += missed
            if expired < self._batch:  # a short batch: nothing more is due (failures wait for the next run)
                break
        return RunReport(self.name, processed, failed, timed_out)
