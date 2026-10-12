#!/usr/bin/env sh
# Smoke checks of the worker image: health with the relay job, 404 with the envelope elsewhere.
#   deploy/smoke.sh http://localhost:8013
set -eu
BASE="${1:-http://localhost:8013}"
fail() { echo "FAIL: $1" >&2; exit 1; }
[ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/health")" = "200" ] || fail "/health is not 200"
curl -s "$BASE/health" | grep -q '"service": "qone-worker"' || fail "health does not name the worker"
[ "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/anything")" = "404" ] || fail "an unknown path is not 404"
curl -s -H 'X-Correlation-Id: smoke-1' "$BASE/anything" | grep -q '"traceId": "smoke-1"' || fail "404 has no envelope with the correlation id"
echo "OK: worker smoke checks passed against $BASE"
