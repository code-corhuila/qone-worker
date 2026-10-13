"""Fakes for the relay: an outbox over a list, a publisher that can fail some events, a clock."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

from worker.domain.event import OutboxEvent


def event(domain="catalog", event_type="SeatReserved", n=0) -> OutboxEvent:
    return OutboxEvent(
        uuid4(), domain, "section", UUID(int=n), event_type, {"n": n}, datetime(2026, 10, 13, 12, 0, n, tzinfo=UTC)
    )


class ListOutbox:
    def __init__(self, domain: str, events: list[OutboxEvent], broken: bool = False) -> None:
        self.domain = domain
        self.pending = list(events)
        self.published: list[OutboxEvent] = []
        self.asked: list[int] = []
        self.broken = broken

    def relay_batch(self, limit, publish):
        if self.broken:
            raise ConnectionError("outbox down")
        self.asked.append(limit)
        batch, done, missed = self.pending[:limit], 0, 0
        for e in batch:
            if publish(e):
                self.published.append(e)
                self.pending.remove(e)
                done += 1
            else:
                missed += 1
        return done, missed


class Publisher:
    def __init__(self, failing: set | None = None) -> None:
        self.failing = failing or set()
        self.sent: list[tuple[OutboxEvent, str]] = []

    def publish(self, event, correlation_id):
        if event.id in self.failing:
            raise RuntimeError("broker nack")
        self.sent.append((event, correlation_id))


class Clock:
    def __init__(self, ticks=None) -> None:
        self.t = 0.0
        self.step = 1.0

    def now(self) -> float:
        self.t += self.step
        return self.t
