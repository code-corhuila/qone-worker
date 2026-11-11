"""Annex D, configuration: `.env.example` names every variable the composition root reads, and the
compose fragment that qone-infra includes passes the ones of the expire-reservations job."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAIN = (ROOT / "apps" / "worker" / "__main__.py").read_text(encoding="utf-8")


def read_variables() -> set[str]:
    names = set(re.findall(r"""env(?:\.get\(|\[)\s*["']([A-Z][A-Z0-9_]+)["']""", MAIN))
    if "_OUTBOX_URL" in MAIN:
        domains = re.search(r"DOMAINS = \(([^)]*)\)", MAIN).group(1)
        names |= {f"{d.upper()}_OUTBOX_URL" for d in re.findall(r'"([a-z]+)"', domains)}
    return names


def example_variables() -> set[str]:
    lines = (ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
    return {line.split("=", 1)[0].strip() for line in lines if "=" in line and not line.lstrip().startswith("#")}


def test_the_env_example_lists_every_variable_the_worker_reads():
    read = read_variables()
    assert {"RABBITMQ_URL", "CATALOG_API_URL", "SERVICE_TOKEN", "PORT"} <= read
    assert read - example_variables() == set()


def test_the_compose_passes_the_variables_of_the_expire_reservations_job():
    compose = (ROOT / "deploy" / "compose.yml").read_text(encoding="utf-8")
    for name in ("CATALOG_API_URL", "EXPIRE_RESERVATIONS_EVERY_SECONDS", "HTTP_TIMEOUT_SECONDS", "HTTP_ATTEMPTS"):
        assert f"{name}: ${{{name}:-" in compose, name
    assert "SERVICE_TOKEN: ${WORKER_SERVICE_TOKEN:?WORKER_SERVICE_TOKEN is required}" in compose
