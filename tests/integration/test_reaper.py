"""
Reaper test — verifies that a RUNNING job whose heartbeat goes stale is reclassified
as FAILED by JobReaper. We don't actually crash a node here (single-container setup);
instead we manually insert a RUNNING row with an old heartbeat and let the reaper sweep.
"""
import time
import pytest


def test_reaper_marks_stale_running_jobs_failed(clean_db, db):
    with db.cursor() as cur:
        # Insert a RUNNING job with a heartbeat far in the past.
        cur.execute("""
            INSERT INTO lseg_jobs (status, node_id, business_date, input_dir,
                                   created_at, started_at, last_heartbeat_at)
            VALUES ('RUNNING', 'fake-node', '20260425', '/test-data',
                    NOW() - INTERVAL 4 HOUR, NOW() - INTERVAL 4 HOUR,
                    NOW() - INTERVAL 4 HOUR)
        """)
        cur.execute("SELECT LAST_INSERT_ID()")
        job_id = cur.fetchone()[0]

    # Reaper runs every 60s with default config; wait up to 90s.
    deadline = time.time() + 90
    final_status = None
    while time.time() < deadline:
        with db.cursor() as cur:
            cur.execute("SELECT status, error_message FROM lseg_jobs WHERE id=%s", (job_id,))
            final_status, msg = cur.fetchone()
        if final_status == "FAILED":
            assert "reaped" in (msg or "").lower()
            return
        time.sleep(5)
    pytest.fail(f"Reaper did not mark stale job FAILED within 90s; last status={final_status}")
