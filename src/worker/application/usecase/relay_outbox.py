"""The outbox relay (norm 5.3.11, ADR-008 point 4): every committed change of billing, catalog and
enrollment reaches RabbitMQ. Safe to run twice: the rows are claimed with SKIP LOCKED and marked
`published_at` only after the broker confirms; consumers keep an inbox by event id, so a row
published again after a crash is skipped downstream. A failing event is counted, logged and left
for the next run; it does not stop the batch (Annex D rules 1 to 3)."""

import logging

from worker.application.port.inbound.job import RunContext, RunReport
from worker.application.port.outbound.ports import MonotonicClock, Outbox, Publisher
from worker.domain.event import OutboxEvent

log = logging.getLogger("worker.relay")


class RelayOutbox:
    def __init__(self, outboxes: list[Outbox], publisher: Publisher, clock: MonotonicClock, batch_size: int) -> None:
        self.name = "relay-outbox"
        self._outboxes = outboxes
        self._publisher = publisher
        self._clock = clock
        self._batch = batch_size

    def run(self, context: RunContext) -> RunReport:
        processed = failed = 0
        timed_out = False
        for outbox in self._outboxes:
            if self._clock.now() >= context.deadline:
                timed_out = True
                break

            def publish(event: OutboxEvent) -> bool:
                nonlocal timed_out
                if self._clock.now() >= context.deadline:
                    timed_out = True
                    return False
                try:
                    self._publisher.publish(event, context.correlation_id)
                    return True
                except Exception as error:  # noqa: BLE001 - one failing event never stops the batch
                    log.warning(
                        "event not published",
                        extra={"traceId": context.correlation_id, "event": str(event.id), "detail": str(error)},
                    )
                    return False

            try:
                done, missed = outbox.relay_batch(self._batch, publish)
            except Exception as error:  # noqa: BLE001 - one unreachable outbox never stops the others
                log.error(
                    "outbox not reachable",
                    extra={"traceId": context.correlation_id, "domain": outbox.domain, "detail": str(error)},
                )
                failed += 1
                continue
            processed += done
            failed += missed
        return RunReport(self.name, processed, failed, timed_out)
