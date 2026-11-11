"""The catalog as the worker sees it (Annex D, outbound adapter): CA-13 over HTTP with the standard
library. Every call carries the worker's own SERVICE token and the run's X-Correlation-Id, has its
own timeout, and is retried a bounded number of times with backoff and full jitter, only when a
retry can help: network errors and timeouts, 429 and 5xx. A 4xx is the caller's error and is
raised at once. The error keeps the catalog's envelope (`error`, `message`, `traceId`)."""

import json
import logging
import random
import time
import urllib.error
import urllib.request
from collections.abc import Callable

from worker.adapter.outbound.retry import retrying

EXPIRATIONS = "/api/v1/catalog/reservations/expirations"
log = logging.getLogger("worker.catalog")


class CatalogError(Exception):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status

    @property
    def retryable(self) -> bool:
        return self.status is None or self.status == 429 or self.status >= 500


class CatalogHttpClient:
    def __init__(
        self,
        base_url: str,
        service_token: str,
        timeout_seconds: float = 5.0,
        attempts: int = 3,
        backoff_seconds: float = 0.2,
        sleep: Callable[[float], None] = time.sleep,
        rand: Callable[[float, float], float] = random.uniform,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._token = service_token
        self._timeout = timeout_seconds
        self._attempts = attempts
        self._backoff = backoff_seconds
        self._sleep = sleep
        self._rand = rand

    def expire_overdue(self, limit: int, correlation_id: str) -> tuple[int, int]:
        def on_retry(attempt: int, error: Exception, wait: float) -> None:
            log.warning(
                "catalog call failed; retrying",
                extra={"traceId": correlation_id, "detail": f"attempt {attempt}: {error}; next in {wait:.2f}s"},
            )

        body = retrying(
            lambda: self._post(EXPIRATIONS, {"limit": limit}, correlation_id),
            self._attempts,
            lambda error: isinstance(error, CatalogError) and error.retryable,
            self._backoff,
            self._sleep,
            self._rand,
            on_retry,
        )
        meta = body["meta"]
        return int(meta["expired"]), int(meta["failed"])

    def _post(self, path: str, payload: dict, correlation_id: str) -> dict:
        request = urllib.request.Request(
            self._base + path,
            data=json.dumps(payload).encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Bearer {self._token}",
                "X-Correlation-Id": correlation_id,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:  # noqa: S310 - fixed http(s) base
                return json.loads(response.read())
        except urllib.error.HTTPError as error:
            raise _from_envelope(error) from None
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            reason = getattr(error, "reason", error)
            raise CatalogError(f"catalog not reachable: {reason}") from error


def _from_envelope(error: urllib.error.HTTPError) -> CatalogError:
    try:
        envelope = json.loads(error.read() or b"{}")
    except (ValueError, OSError):
        envelope = {}
    code = envelope.get("error", "")
    message = envelope.get("message", error.reason)
    return CatalogError(
        f"catalog answered {error.code} {code}: {message} (traceId {envelope.get('traceId', '-')})", error.code
    )
