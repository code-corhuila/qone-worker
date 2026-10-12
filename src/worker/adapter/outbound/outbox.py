"""A domain's outbox over PostgreSQL: the one documented exception to "only through the API"
(09-microservices/08-worker), scoped to the outbox table. A batch is claimed with
`FOR UPDATE SKIP LOCKED` so two overlapping runs never take the same row, and `published_at` is
set in the same transaction only for the events the broker confirmed."""

import re
from collections.abc import Callable

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from worker.domain.event import OutboxEvent

SCHEMA = re.compile(r"^[a-z]+$")


class PostgresOutbox:
    def __init__(self, domain: str, url: str, pool_size: int = 2, statement_timeout_ms: int = 5000) -> None:
        if not SCHEMA.match(domain):
            raise ValueError("the domain names its schema: lower-case letters only")
        self.domain = domain
        self._pool = ConnectionPool(
            conninfo=url,
            min_size=0,
            max_size=pool_size,
            timeout=5.0,
            kwargs={"options": f"-c statement_timeout={statement_timeout_ms}", "row_factory": dict_row},
            open=True,
        )

    def relay_batch(self, limit: int, publish: Callable[[OutboxEvent], bool]) -> tuple[int, int]:
        with self._pool.connection() as conn, conn.transaction():
            rows = conn.execute(
                f"SELECT id, aggregate_type, aggregate_id, event_type, payload, occurred_at FROM {self.domain}.outbox "
                "WHERE published_at IS NULL ORDER BY occurred_at, id LIMIT %s FOR UPDATE SKIP LOCKED",
                (limit,),
            ).fetchall()
            confirmed = []
            for row in rows:
                event = OutboxEvent(
                    row["id"],
                    self.domain,
                    row["aggregate_type"],
                    row["aggregate_id"],
                    row["event_type"],
                    row["payload"],
                    row["occurred_at"],
                )
                if publish(event):
                    confirmed.append(row["id"])
            if confirmed:
                conn.execute(f"UPDATE {self.domain}.outbox SET published_at = now() WHERE id = ANY(%s)", (confirmed,))
        return len(confirmed), len(rows) - len(confirmed)

    def close(self) -> None:
        self._pool.close()
