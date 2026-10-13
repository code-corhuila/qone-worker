"""Integration (Annex D): the relay against a real outbox (TEST_DATABASE_URL, any Qampus domain
database: billing, catalog or enrollment, named by TEST_OUTBOX_DOMAIN) and a real RabbitMQ
(TEST_RABBITMQ_URL). Skipped, not failed, without them."""

import json
import os
import uuid

import pytest

DB = os.environ.get("TEST_DATABASE_URL", "")
DOMAIN = os.environ.get("TEST_OUTBOX_DOMAIN", "catalog")
MQ = os.environ.get("TEST_RABBITMQ_URL", "")
EVENT = {
    "billing": ("invoice", "InvoiceIssued"),
    "catalog": ("section", "SeatReserved"),
    "enrollment": ("enrollment", "EnrollmentConfirmed"),
}[DOMAIN]


@pytest.mark.skipif(not (DB and MQ), reason="TEST_DATABASE_URL and TEST_RABBITMQ_URL not set")
def test_the_relay_publishes_a_real_outbox_to_a_real_broker_once():
    import pika
    import psycopg

    from worker.adapter.outbound.outbox import PostgresOutbox
    from worker.adapter.outbound.rabbitmq import RabbitPublisher
    from worker.application.port.inbound.job import RunContext
    from worker.application.usecase.relay_outbox import RelayOutbox

    aggregate = uuid.uuid4()
    with psycopg.connect(DB, autocommit=True) as conn:
        conn.execute(f"UPDATE {DOMAIN}.outbox SET published_at = now() WHERE published_at IS NULL")
        for n in range(3):
            conn.execute(
                f"INSERT INTO {DOMAIN}.outbox (aggregate_type, aggregate_id, event_type, payload) VALUES (%s, %s, %s, "
                "%s)",
                (EVENT[0], aggregate, EVENT[1], json.dumps({"n": n})),
            )
    channel = pika.BlockingConnection(pika.URLParameters(MQ)).channel()
    channel.exchange_declare(exchange="qampus.events", exchange_type="topic", durable=True)
    queue = channel.queue_declare(queue="", exclusive=True).method.queue
    channel.queue_bind(queue=queue, exchange="qampus.events", routing_key=f"{DOMAIN}.#")

    outbox = PostgresOutbox(DOMAIN, DB)
    relay = RelayOutbox(
        [outbox], RabbitPublisher(MQ), clock=type("C", (), {"now": staticmethod(lambda: 0.0)})(), batch_size=2
    )
    first = relay.run(RunContext("it-run-1", deadline=10.0))
    second = relay.run(RunContext("it-run-2", deadline=10.0))
    third = relay.run(RunContext("it-run-3", deadline=10.0))
    outbox.close()
    assert (first.processed, second.processed, third.processed) == (2, 1, 0), "bounded batches, nothing published twice"
    received = []
    for _ in range(3):
        method, properties, body = channel.basic_get(queue=queue, auto_ack=True)
        assert method is not None
        received.append((method.routing_key, properties.correlation_id, json.loads(body)["payload"]["n"]))
    assert received == [
        (f"{DOMAIN}.{EVENT[1]}", "it-run-1", 0),
        (f"{DOMAIN}.{EVENT[1]}", "it-run-1", 1),
        (f"{DOMAIN}.{EVENT[1]}", "it-run-2", 2),
    ]
    assert channel.basic_get(queue=queue, auto_ack=True)[0] is None
