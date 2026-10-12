"""`python -m apps.worker`: the composition root (Annex D). Reads the configuration, declares every
limit, builds the relay over the outboxes that are configured, serves the health endpoint and runs
the scheduler until SIGTERM."""

import json
import logging
import os
import signal
import sys
import time

from worker.adapter.inbound.health import serve_health
from worker.adapter.inbound.scheduler import Scheduler
from worker.adapter.outbound.outbox import PostgresOutbox
from worker.adapter.outbound.rabbitmq import RabbitPublisher
from worker.application.usecase.relay_outbox import RelayOutbox

DOMAINS = ("billing", "catalog", "enrollment")


class MonotonicClock:
    def now(self) -> float:
        return time.monotonic()


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        line = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key in ("traceId", "job", "processed", "failed", "timedOut", "domain", "event", "detail"):
            if hasattr(record, key):
                line[key] = getattr(record, key)
        if record.exc_info:
            line["exception"] = self.formatException(record.exc_info)
        return json.dumps(line, ensure_ascii=False)


def main() -> None:
    env = os.environ
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.getLogger().handlers[:] = [handler]
    logging.getLogger().setLevel(env.get("LOG_LEVEL", "info").upper())
    logging.getLogger("pika").setLevel(logging.WARNING)
    log = logging.getLogger("worker")

    rabbit = env.get("RABBITMQ_URL", "").strip()
    if not rabbit:
        raise SystemExit("RABBITMQ_URL is required: the relay publishes to qampus.events")
    batch = int(env.get("BATCH_SIZE", "100"))
    every = float(env.get("RELAY_EVERY_SECONDS", "5"))
    timeout = float(env.get("RUN_TIMEOUT_SECONDS", "20"))
    outboxes = [
        PostgresOutbox(d, env[f"{d.upper()}_OUTBOX_URL"]) for d in DOMAINS if env.get(f"{d.upper()}_OUTBOX_URL")
    ]
    log.info(
        "relay over %s outbox(es): %s; batch %s, every %ss, run timeout %ss",
        len(outboxes),
        [o.domain for o in outboxes],
        batch,
        every,
        timeout,
    )

    scheduler = Scheduler([RelayOutbox(outboxes, RabbitPublisher(rabbit), MonotonicClock(), batch)], every, timeout)
    server = serve_health(scheduler, int(env.get("PORT", "8000")))
    signal.signal(signal.SIGTERM, lambda *_: scheduler.stop())
    try:
        scheduler.serve_forever()
    finally:
        server.shutdown()


if __name__ == "__main__":
    main()
