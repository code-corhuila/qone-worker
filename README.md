# qone-worker

> Asynchronous and scheduled jobs

Part of the **Qampus (Quorum One)** distributed system — team `quorum-one`, Grupo 1.
Governance and documentation live in [`qone-docs`](https://github.com/code-corhuila/qone-docs).

## Purpose

Everything that happens without anyone asking (course norm 5.7, Annex D). Its first job is the
**outbox relay** of ADR-008 point 4 and norm 5.3.11: every event billing, catalog, enrollment and advisor
write to their outbox in the transaction of a change reaches the `qampus.events` topic exchange of
RabbitMQ with the routing key `<domain>.<EventType>` (`catalog.SeatReserved`, `billing.TuitionPaid`).
Consumers (the enrollment projections, the advisor) keep an inbox by event id. Python 3.12,
hexagonal like an `-api`; the inbound adapter is a scheduler, not HTTP, and the only HTTP is a
health endpoint.

Piece page: `09-microservices/services/08-worker/README.md`. Story: `code-corhuila/qone-docs#48` (HU-PLT-001).

## Rules it keeps (Annex D)

- Safe to run twice: rows claimed with `FOR UPDATE SKIP LOCKED`, `published_at` set only after the
  broker confirms; a duplicate publish after a crash is skipped by the consumers' inbox.
- A failing event is counted, logged and left for the next run; an unreachable outbox does not
  stop the others.
- Bounded runs: `BATCH_SIZE` events per outbox, `RUN_TIMEOUT_SECONDS` per run, one
  `X-Correlation-Id` per run on every message and log line.
- The outbox relay is the one documented exception to "only through the API"; it touches the
  outbox table and nothing else.

## The advisor's maintenance

With the advisor's database (`ADVISOR_MAINTENANCE_URL`, or the `ADVISOR_OUTBOX_URL` the relay
already has) two more jobs run, both named in `09-microservices/services/06-advisor-api` and
`06-data/models.md`:

| Job | Every | What it does |
|---|---|---|
| `expire-plans` | `EXPIRE_PLANS_EVERY_SECONDS` (30) | a `PROPOSED` plan past its `expiresAt` (10 minutes after the proposal) becomes `EXPIRED` (INV-ADV-003) |
| `prune-llm-cache` | `PRUNE_LLM_CACHE_EVERY_SECONDS` (3600) | deletes `llm_cache` entries older than `LLM_CACHE_KEEP_DAYS` (7) |

Both claim rows with `SKIP LOCKED`, work in batches of `BATCH_SIZE` until nothing is due or the
run's deadline passes, and appear in `/health` with their last run.

## How to run it

```bash
python -m venv .venv && . .venv/bin/activate      # Windows: .venv\\Scripts\\activate
pip install -e ".[dev]"
ruff check .
python -m pytest                                   # with TEST_DATABASE_URL, TEST_OUTBOX_DOMAIN and TEST_RABBITMQ_URL for the relay test
cp .env.example .env
python -m apps.worker                              # health at http://localhost:8000/health
```

Configuration: `RABBITMQ_URL` (required), one `<DOMAIN>_OUTBOX_URL` per outbox to relay (billing,
catalog, enrollment, advisor; a missing one is simply not relayed), `RELAY_EVERY_SECONDS` (5),
`BATCH_SIZE` (100), `RUN_TIMEOUT_SECONDS` (20). Container image and compose in `deploy/`; the
compose fragment is what `qone-infra` includes next to its RabbitMQ.

## Branching

Three permanent branches. **None of them accepts a direct commit** — you enter through a child
branch and leave through a Pull Request.

```
develop  <--PR--  feat/... fix/... chore/...
qa       <--PR--  qa/...
main     <--PR--  release/...  hotfix/...
```

Promotion happens **by re-application** (`git cherry-pick -x`), never by merging one permanent
branch into another: `merge develop -> qa` and `merge qa -> main` do not exist in this model.

`main` requires **1 approval from `ariel5253`**. On `develop` and `qa` the team sets its own review
rule.

Full policy: `00-governance/branching-policy.md` in `qone-docs`.
