"""An event waiting in a domain's outbox (06-data/models.md, shared shape): its id is the event id
consumers keep in their inbox, so publishing it twice is harmless downstream."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

EXCHANGE = "qampus.events"


@dataclass(frozen=True)
class OutboxEvent:
    id: UUID
    domain: str
    aggregate_type: str
    aggregate_id: UUID
    event_type: str
    payload: dict[str, Any]
    occurred_at: datetime

    @property
    def routing_key(self) -> str:
        """`<domain>.<EventType>`, e.g. `catalog.SeatReserved`: consumers bind by domain or by type."""
        return f"{self.domain}.{self.event_type}"

    def message(self) -> dict[str, Any]:
        return {
            "eventId": str(self.id),
            "eventType": self.event_type,
            "domain": self.domain,
            "aggregateType": self.aggregate_type,
            "aggregateId": str(self.aggregate_id),
            "occurredAt": self.occurred_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "payload": self.payload,
        }
