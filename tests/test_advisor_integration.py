"""Integration (Annex D): the advisor's maintenance against a throwaway qone-advisor-db
(TEST_ADVISOR_DATABASE_URL). It inserts and removes its own rows only. Skipped without it."""

import os
import uuid

import pytest

URL = os.environ.get("TEST_ADVISOR_DATABASE_URL", "")
pytestmark = pytest.mark.skipif(not URL, reason="TEST_ADVISOR_DATABASE_URL not set")
STUDENT = "11111111-1111-4111-8111-111111111111"
SECTION = "55555555-5555-4555-8555-555555555510"


def test_only_due_proposed_plans_expire_and_only_old_cache_entries_go():
    import psycopg

    from worker.adapter.outbound.advisor import PostgresAdvisorMaintenance

    conversation = uuid.uuid4()
    due, fresh, executing = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    old_key, new_key = uuid.uuid4().hex * 2, uuid.uuid4().hex * 2
    with psycopg.connect(URL, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO advisor.conversation (id, user_id, user_role, model) VALUES (%s, %s, 'STUDENT', 'm')",
            (conversation, STUDENT),
        )
        insert = (
            "INSERT INTO advisor.plan (id, conversation_id, student_id, section_id, summary, status, confirmation_id, "
            "expires_at, created_at) VALUES (%s, %s, %s, %s, '{}', %s, %s, now() + %s::interval, now() - interval '20 "
            "minutes')"
        )
        conn.execute(insert, (due, conversation, STUDENT, SECTION, "PROPOSED", None, "-1 minute"))
        conn.execute(insert, (fresh, conversation, STUDENT, SECTION, "PROPOSED", None, "5 minutes"))
        conn.execute(insert, (executing, conversation, STUDENT, SECTION, "EXECUTING", uuid.uuid4(), "-1 minute"))
        conn.execute(
            "INSERT INTO advisor.llm_cache (key, response, created_at) VALUES (%s, '{}', now() - interval '8 days')",
            (old_key,),
        )
        conn.execute(
            "INSERT INTO advisor.llm_cache (key, response, created_at) VALUES (%s, '{}', now() - interval '6 days')",
            (new_key,),
        )
    store = PostgresAdvisorMaintenance(URL)
    try:
        assert store.expire_plans(100) >= 1
        assert store.expire_plans(100) == 0, "an expired plan is not expired twice"
        assert store.prune_llm_cache(7, 500) >= 1
        with psycopg.connect(URL) as conn:
            status = dict(
                conn.execute(
                    "SELECT id, status FROM advisor.plan WHERE conversation_id = %s", (conversation,)
                ).fetchall()
            )
            keys = {
                r[0]
                for r in conn.execute("SELECT key FROM advisor.llm_cache WHERE key IN (%s, %s)", (old_key, new_key))
            }
        assert status == {due: "EXPIRED", fresh: "PROPOSED", executing: "EXECUTING"}
        assert keys == {new_key}
    finally:
        store.close()
        with psycopg.connect(URL, autocommit=True) as conn:
            conn.execute("DELETE FROM advisor.plan WHERE conversation_id = %s", (conversation,))
            conn.execute("DELETE FROM advisor.conversation WHERE id = %s", (conversation,))
            conn.execute("DELETE FROM advisor.llm_cache WHERE key IN (%s, %s)", (old_key, new_key))
