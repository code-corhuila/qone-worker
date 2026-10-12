## User story

code-corhuila/qone-docs#NN

## What changes and why

## How it was tested

- [ ] `ci.yml` green: ruff, pytest (jobs with fakes, adapters, architecture)
- [ ] Integration against PostgreSQL and RabbitMQ run locally with `TEST_DATABASE_URL` and `TEST_RABBITMQ_URL`

## Promotion trail

Only towards `qa` or `main`: the re-applied commits, each with its `(cherry picked from commit <sha>)` line.

## Checklist

- [ ] No secrets, no `.env`
- [ ] Every job is safe to run twice; a failing item does not stop the batch
- [ ] Other domains reached only through their API, except the documented outbox relay
