"""HU-PLT-003, adapter level: the HTTP client of CA-13 against a scripted local server. Token,
correlation, timeout, the `{data, meta}` answer, retries with exponential backoff and full
jitter only for network errors, 429 and 5xx; a 4xx is never retried (Annex D rules 4 to 7)."""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from worker.adapter.outbound.catalog import CatalogError, CatalogHttpClient

PATH = "/api/v1/catalog/reservations/expirations"


class Catalog:
    """A scripted catalog: each request takes the next (status, body, delay) of the script."""

    def __init__(self, script):
        self.script = list(script)
        self.requests: list[dict] = []
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802 - the stdlib's name
                length = int(self.headers.get("Content-Length", "0"))
                outer.requests.append(
                    {"path": self.path, "headers": dict(self.headers), "body": json.loads(self.rfile.read(length))}
                )
                status, body, delay = outer.script.pop(0) if outer.script else (500, {}, 0)
                time.sleep(delay)
                data = json.dumps(body).encode()
                try:
                    self.send_response(status)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                except OSError:  # the client gave up (timeout): nobody reads the answer
                    pass

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()


OK = (200, {"data": [{"id": "r-1"}, {"id": "r-2"}], "meta": {"expired": 2, "failed": 1}}, 0)
DOWN = (503, {"error": "SERVICE_UNAVAILABLE", "message": "database is not reachable", "traceId": "t-503"}, 0)
BUSY = (429, {"error": "TOO_MANY_REQUESTS", "message": "slow down", "traceId": "t-429"}, 0)


def client_for(catalog, timeout=2.0, **kw):
    waits: list[tuple[float, float]] = []
    slept: list[float] = []

    def rand(low, high):
        waits.append((low, high))
        return high / 2

    client = CatalogHttpClient(
        catalog.url, "service-token-1", timeout_seconds=timeout, sleep=slept.append, rand=rand, **kw
    )
    return client, waits, slept


@pytest.fixture
def make():
    made = []

    def build(*script):
        catalog = Catalog(script)
        made.append(catalog)
        return catalog

    yield build
    for catalog in made:
        catalog.close()


def test_a_call_carries_the_token_the_correlation_and_the_limit_and_reads_meta(make):
    catalog = make(OK)
    client, _, slept = client_for(catalog)
    assert client.expire_overdue(100, "worker-expire-reservations-abc") == (2, 1)
    request = catalog.requests[0]
    assert request["path"] == PATH and request["body"] == {"limit": 100}
    assert request["headers"]["Authorization"] == "Bearer service-token-1"
    assert request["headers"]["X-Correlation-Id"] == "worker-expire-reservations-abc"
    assert slept == []


def test_503_and_429_are_retried_with_exponential_backoff_and_full_jitter(make):
    catalog = make(DOWN, BUSY, OK)
    client, waits, slept = client_for(catalog, attempts=3, backoff_seconds=0.2)
    assert client.expire_overdue(10, "c-1") == (2, 1)
    assert len(catalog.requests) == 3
    assert waits == [(0, 0.4), (0, 0.8)] and slept == [0.2, 0.4]
    assert {r["headers"]["X-Correlation-Id"] for r in catalog.requests} == {"c-1"}


def test_the_retries_are_bounded_and_the_error_keeps_the_catalog_trace_id(make):
    catalog = make(DOWN, DOWN, DOWN, OK)
    client, _, slept = client_for(catalog, attempts=3)
    with pytest.raises(CatalogError) as caught:
        client.expire_overdue(10, "c-2")
    assert len(catalog.requests) == 3 and len(slept) == 2
    assert caught.value.status == 503 and "t-503" in str(caught.value)


@pytest.mark.parametrize("status", [400, 401, 403, 422])
def test_a_4xx_is_never_retried(make, status):
    catalog = make((status, {"error": "X", "message": "caller error", "traceId": "t-4xx"}, 0), OK)
    client, _, slept = client_for(catalog, attempts=3)
    with pytest.raises(CatalogError) as caught:
        client.expire_overdue(10, "c-3")
    assert caught.value.status == status and len(catalog.requests) == 1 and slept == []


def test_a_timeout_is_retried_like_a_network_error(make):
    catalog = make((200, OK[1], 1.0), OK)
    client, _, slept = client_for(catalog, timeout=0.3, attempts=2)
    assert client.expire_overdue(10, "c-4") == (2, 1)
    assert len(catalog.requests) == 2 and len(slept) == 1


def test_a_catalog_that_does_not_listen_is_a_retried_network_error():
    client = CatalogHttpClient("http://127.0.0.1:1", "t", timeout_seconds=0.5, attempts=2, sleep=lambda s: None)
    with pytest.raises(CatalogError) as caught:
        client.expire_overdue(10, "c-5")
    assert caught.value.status is None and "not reachable" in str(caught.value)
