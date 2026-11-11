"""`python -m apps.worker`: the composition root (Annex D). Reads the configuration, declares every
limit, builds the relay over the outboxes that are configured, with the advisor's database its two
maintenance jobs and, with a SERVICE_TOKEN, the sweep of overdue seat reservations through the
catalog's API; serves the health endpoint and runs the scheduler until SIGTERM or SIGINT, then
shuts down gracefully: the run in progress finishes, the health server, the pools and the AMQP
connection are closed."""

import json
import logging
import os
import signal
import sys
import time

from worker.adapter.inbound.health import serve_health
from worker.adapter.inbound.metrics import WorkerMetrics
from worker.adapter.inbound.scheduler import Scheduler
from worker.adapter.outbound.advisor import PostgresAdvisorMaintenance
from worker.adapter.outbound.catalog import CatalogHttpClient
from worker.adapter.outbound.outbox import PostgresOutbox
from worker.adapter.outbound.rabbitmq import RabbitPublisher, broker_reachable
from worker.application.usecase.advisor_maintenance import ExpirePlans, PruneLlmCache
from worker.application.usecase.expire_reservations import ExpireReservations
from worker.application.usecase.relay_outbox import RelayOutbox

DOMAINS = ("billing", "catalog", "enrollment", "advisor")
# Explicit limits (Annex D), declared here with their values.
RETRY_BACKOFF_SECONDS = 0.2  # full jitter: up to 0.4 s before the 2nd attempt, 0.8 s before the 3rd
PUBLISH_ATTEMPTS = 3
CATALOG_SWEEP_MAX = 500  # the largest `limit` CA-13 accepts
BROKER_CHECK_TIMEOUT_SECONDS = 2.0
log = logging.getLogger("worker")


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


def install_signals(scheduler: Scheduler) -> None:
    """SIGTERM (docker stop) and SIGINT (Ctrl+C) stop scheduling; the run in progress finishes."""
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: scheduler.stop())


def shutdown(server, resources: list) -> None:
    """Closes the health server and every pool or connection; one failing close never skips the rest."""
    server.shutdown()
    server.server_close()
    for resource in resources:
        try:
            resource.close()
        except Exception as error:  # noqa: BLE001 - shutting down: log it and close the others
            log.warning("close failed", extra={"detail": f"{type(resource).__name__}: {error}"})
    log.info("worker stopped")


def main() -> None:
    env = os.environ
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.getLogger().handlers[:] = [handler]
    logging.getLogger().setLevel(env.get("LOG_LEVEL", "info").upper())
    logging.getLogger("pika").setLevel(logging.WARNING)

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

    publisher = RabbitPublisher(rabbit, PUBLISH_ATTEMPTS, RETRY_BACKOFF_SECONDS)
    resources: list = [*outboxes, publisher]
    jobs = [RelayOutbox(outboxes, publisher, MonotonicClock(), batch)]
    advisor_url = env.get("ADVISOR_MAINTENANCE_URL", "").strip() or env.get("ADVISOR_OUTBOX_URL", "").strip()
    if advisor_url:
        advisor = PostgresAdvisorMaintenance(advisor_url)
        resources.append(advisor)
        expire_every = float(env.get("EXPIRE_PLANS_EVERY_SECONDS", "30"))
        prune_every = float(env.get("PRUNE_LLM_CACHE_EVERY_SECONDS", "3600"))
        keep_days = int(env.get("LLM_CACHE_KEEP_DAYS", "7"))
        jobs += [
            ExpirePlans(advisor, MonotonicClock(), batch, expire_every),
            PruneLlmCache(advisor, MonotonicClock(), batch, keep_days, prune_every),
        ]
        log.info(
            "advisor jobs: expire plans every %ss, prune the LLM cache older than %s days every %ss",
            expire_every,
            keep_days,
            prune_every,
        )
    token = env.get("SERVICE_TOKEN", "").strip()
    if token:
        catalog_url = env.get("CATALOG_API_URL", "http://qone-catalog-api:8000").strip()
        sweep_every = float(env.get("EXPIRE_RESERVATIONS_EVERY_SECONDS", "60"))
        http_timeout = float(env.get("HTTP_TIMEOUT_SECONDS", "5"))
        http_attempts = int(env.get("HTTP_ATTEMPTS", "3"))
        catalog = CatalogHttpClient(catalog_url, token, http_timeout, http_attempts, RETRY_BACKOFF_SECONDS)
        jobs.append(ExpireReservations(catalog, MonotonicClock(), min(batch, CATALOG_SWEEP_MAX), sweep_every))
        log.info(
            "expire-reservations every %ss through %s; http timeout %ss, %s attempts",
            sweep_every,
            catalog_url,
            http_timeout,
            http_attempts,
        )
    else:
        log.warning("SERVICE_TOKEN not set: the expire-reservations job does not run")
    metrics = WorkerMetrics()
    scheduler = Scheduler(jobs, every, timeout, on_run=metrics.run_finished)
    checks = {"rabbitmq": lambda: broker_reachable(rabbit, BROKER_CHECK_TIMEOUT_SECONDS)}
    server = serve_health(scheduler, int(env.get("PORT", "8000")), checks=checks, metrics=metrics)
    install_signals(scheduler)
    try:
        scheduler.serve_forever()
    finally:
        shutdown(server, resources)


if __name__ == "__main__":
    main()
