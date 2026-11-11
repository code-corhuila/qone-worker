"""The publisher over RabbitMQ (pika): a durable topic exchange `qampus.events`, persistent messages
with the event id as message id and the run's correlation id, and publisher confirms, so an event
counts as published only when the broker took it. A dropped connection is reopened and the publish
retried a bounded number of times with exponential backoff and full jitter (Annex D rule 7)."""

import json
import random
import time
from collections.abc import Callable

import pika
from pika.exceptions import AMQPError

from worker.adapter.outbound.retry import retrying
from worker.domain.event import EXCHANGE, OutboxEvent


class RabbitPublisher:
    def __init__(
        self,
        url: str,
        attempts: int = 3,
        backoff_seconds: float = 0.2,
        sleep: Callable[[float], None] = time.sleep,
        rand: Callable[[float, float], float] = random.uniform,
    ) -> None:
        self._attempts = attempts
        self._backoff = backoff_seconds
        self._sleep = sleep
        self._rand = rand
        self._params = pika.URLParameters(url)
        self._params.socket_timeout = 5
        self._params.blocked_connection_timeout = 10
        self._connection = None
        self._channel = None

    def _open(self):
        if self._channel is None or self._channel.is_closed:
            self._connection = pika.BlockingConnection(self._params)
            self._channel = self._connection.channel()
            self._channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
            self._channel.confirm_delivery()
        return self._channel

    def publish(self, event: OutboxEvent, correlation_id: str) -> None:
        body = json.dumps(event.message(), ensure_ascii=False).encode("utf-8")
        properties = pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent,
            message_id=str(event.id),
            correlation_id=correlation_id,
            type=event.event_type,
        )

        def attempt() -> None:
            try:
                self._open().basic_publish(
                    exchange=EXCHANGE, routing_key=event.routing_key, body=body, properties=properties
                )
            except AMQPError:
                self._channel = None  # reopened by the next attempt
                raise

        retrying(attempt, self._attempts, lambda e: isinstance(e, AMQPError), self._backoff, self._sleep, self._rand)

    def ping(self) -> bool:
        try:
            self._open()
            return True
        except AMQPError:
            return False
