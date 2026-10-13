"""The advisor's maintenance over PostgreSQL (schema `advisor` of qone-advisor-db), scoped to the
two rows the documentation gives the worker: `plan` (PROPOSED past `expires_at` becomes EXPIRED,
index ix_plan_proposed_expires) and `llm_cache` (older than 7 days, index ix_llm_cache_created_at).
Rows are claimed with SKIP LOCKED so an overlapping run, or the advisor confirming the same plan,
never waits on this job; the plan's own CHECK keeps an expired plan without a confirmation."""

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


class PostgresAdvisorMaintenance:
    def __init__(self, url: str, pool_size: int = 1, statement_timeout_ms: int = 5000) -> None:
        self._pool = ConnectionPool(
            conninfo=url,
            min_size=0,
            max_size=pool_size,
            timeout=5.0,
            kwargs={"options": f"-c statement_timeout={statement_timeout_ms}", "row_factory": dict_row},
            open=True,
        )

    def expire_plans(self, limit: int) -> int:
        with self._pool.connection() as conn, conn.transaction():
            rows = conn.execute(
                "WITH due AS (SELECT id FROM advisor.plan WHERE status = 'PROPOSED' AND expires_at <= now() "
                "ORDER BY expires_at LIMIT %s FOR UPDATE SKIP LOCKED) "
                "UPDATE advisor.plan p SET status = 'EXPIRED', updated_at = now() FROM due WHERE p.id = due.id "
                "RETURNING p.id",
                (limit,),
            ).fetchall()
        return len(rows)

    def prune_llm_cache(self, older_than_days: int, limit: int) -> int:
        with self._pool.connection() as conn, conn.transaction():
            rows = conn.execute(
                "WITH old AS (SELECT key FROM advisor.llm_cache WHERE created_at < now() - make_interval(days => %s) "
                "LIMIT %s FOR UPDATE SKIP LOCKED) "
                "DELETE FROM advisor.llm_cache c USING old WHERE c.key = old.key RETURNING c.key",
                (older_than_days, limit),
            ).fetchall()
        return len(rows)

    def close(self) -> None:
        self._pool.close()
