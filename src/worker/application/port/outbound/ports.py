"""What the relay needs: an outbox it can claim a bounded batch from and mark as published, a
publisher that confirms delivery, and a monotonic clock for the run's deadline."""

from collections.abc import Callable
from typing import Protocol

from worker.domain.event import OutboxEvent


class Outbox(Protocol):
    domain: str

    def relay_batch(self, limit: int, publish: Callable[[OutboxEvent], bool]) -> tuple[int, int]:
        """Claims at most `limit` unpublished events in order (SKIP LOCKED: two overlapping runs
        never take the same row), calls `publish` for each and marks `published_at` on the
        confirmed ones in the same transaction. Returns (published, failed)."""


class Publisher(Protocol):
    def publish(self, event: OutboxEvent, correlation_id: str) -> None:
        """Publishes to the exchange and waits for the broker's confirmation; raises on failure."""


class MonotonicClock(Protocol):
    def now(self) -> float: ...
