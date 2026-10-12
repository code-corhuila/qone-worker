"""The publisher over RabbitMQ (pika): a durable topic exchange `qampus.events`, persistent messages
with the event id as message id and the run's correlation id, and publisher confirms, so an event
counts as published only when the broker took it. A dropped connection is reopened once."""

import json

import pika
from pika.exceptions import AMQPError

from worker.domain.event import EXCHANGE, OutboxEvent


class RabbitPublisher:
    def __init__(self, url: str) -> None:
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
        for attempt in (1, 2):
            try:
                self._open().basic_publish(
                    exchange=EXCHANGE, routing_key=event.routing_key, body=body, properties=properties
                )
                return
            except AMQPError:
                self._channel = None
                if attempt == 2:
                    raise

    def ping(self) -> bool:
        try:
            self._open()
            return True
        except AMQPError:
            return False
