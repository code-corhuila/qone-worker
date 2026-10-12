# qone-worker

> Asynchronous and scheduled jobs

Part of the **Qampus (Quorum One)** distributed system — team `quorum-one`, Grupo 1.
Governance and documentation live in [`qone-docs`](https://github.com/code-corhuila/qone-docs).

## Purpose

Everything that happens without anyone asking (course norm 5.7, Annex D). Its first job is the
**outbox relay** of ADR-008 point 4 and norm 5.3.11: every event billing, catalog and enrollment
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

## How to run it

```bash
python -m venv .venv && . .venv/bin/activate      # Windows: .venv\\Scripts\\activate
pip install -e ".[dev]"
ruff check .
python -m pytest
```

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
