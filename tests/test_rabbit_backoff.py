"""The AMQP publish retry uses the same bounded backoff with full jitter as the HTTP client, and
reopens the channel after a broken one (no broker needed: the channel is a fake)."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pika.exceptions import AMQPConnectionError

from worker.adapter.outbound.rabbitmq import RabbitPublisher
from worker.domain.event import OutboxEvent


class Channel:
    def __init__(self, failures):
        self.failures, self.published = failures, []

    def basic_publish(self, **kw):
        if self.failures:
            self.failures -= 1
            raise AMQPConnectionError("broker gone")
        self.published.append(kw)


def publisher(failures, attempts=3):
    waits, opened = [], []
    channel = Channel(failures)
    p = RabbitPublisher(
        "amqp://guest:guest@127.0.0.1:1/%2F",
        attempts=attempts,
        backoff_seconds=0.2,
        sleep=lambda s: None,
        rand=lambda low, high: waits.append((low, high)) or 0,
    )

    def fake_open():
        opened.append(1)
        return channel

    p._open = fake_open
    return p, channel, waits, opened


EVENT = OutboxEvent(uuid4(), "catalog", "section", uuid4(), "SeatReleased", {}, datetime(2026, 11, 1, tzinfo=UTC))


def test_a_broken_channel_is_retried_with_backoff_and_reopened():
    p, channel, waits, opened = publisher(failures=2)
    p.publish(EVENT, "worker-relay-outbox-1")
    assert len(channel.published) == 1 and len(opened) == 3
    assert waits == [(0, 0.4), (0, 0.8)]
    assert channel.published[0]["routing_key"] == "catalog.SeatReleased"


def test_the_publish_retries_are_bounded():
    p, channel, waits, _ = publisher(failures=5, attempts=3)
    with pytest.raises(AMQPConnectionError):
        p.publish(EVENT, "worker-relay-outbox-2")
    assert channel.published == [] and len(waits) == 2
