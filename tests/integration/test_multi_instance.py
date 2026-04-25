"""
Multi-instance / cluster lock tests.

Strategy: simulate concurrent jobs by queueing two jobs back-to-back. Even though
this single-container test cannot truly create a second JVM cluster member, the
GET_LOCK guard is exercised at the job-claim boundary: ClusterLock.tryAcquire is
called from the orchestrator on each run, so two jobs run sequentially even when
both are QUEUED simultaneously. We assert this by examining started_at/finished_at
windows in lseg_jobs.

The full multi-node cluster-lock test (two ingest containers) lives in
test_multi_node.py and is skipped unless --multinode is passed.
"""
import time
import pytest
from conftest import (write_zip, make_org_int_file, trigger_job, wait_for_job)


def test_two_queued_jobs_run_sequentially(clean_db, input_dir, db):
    """Queue two jobs; the worker should serialize them via the cluster lock."""
    write_zip(input_dir, "Organization.INT.99999.20260425.1.1.1.txt.zip",
              make_org_int_file(seq=1, rows=[
                  {"Action": "I", "Entity_ID": f"E{i}", "Entity_Perm_ID": f"P{i}",
                   "Issuer_Name": f"v{i}"} for i in range(50)
              ]))

    j1 = trigger_job()
    j2 = trigger_job()
    s1 = wait_for_job(j1)
    s2 = wait_for_job(j2)
    assert s1 == "COMPLETED"
    assert s2 == "COMPLETED"

    with db.cursor() as cur:
        cur.execute("SELECT id, started_at, finished_at FROM lseg_jobs ORDER BY id")
        rows = cur.fetchall()
        # Both rows must have non-null started_at/finished_at and not overlap.
        a, b = rows
        assert a[1] is not None and a[2] is not None
        assert b[1] is not None and b[2] is not None
        # Sequential: job 2 started >= job 1 finished (cluster lock enforces this).
        assert b[1] >= a[2], f"Job {b[0]} started before job {a[0]} finished — cluster lock failed"


def test_stop_signal_aborts_running_job(clean_db, input_dir, db):
    """Submit a many-row file; immediately /stop the job; ensure status STOPPED is preserved."""
    big_rows = [{"Action": "I", "Entity_ID": f"E{i}", "Entity_Perm_ID": f"P{i}",
                 "Issuer_Name": f"v{i}"} for i in range(200000)]  # large enough to take seconds
    write_zip(input_dir, "Organization.INT.99999.20260425.1.1.1.txt.zip",
              make_org_int_file(seq=1, rows=big_rows))

    j = trigger_job()
    # Poll until RUNNING then stop.
    import requests
    deadline = time.time() + 30
    while time.time() < deadline:
        st = requests.get("http://localhost:8080/api/jobs/status",
                          params={"jobId": j}, timeout=5).json()["status"]
        if st == "RUNNING":
            break
        time.sleep(0.5)
    requests.post("http://localhost:8080/api/jobs/stop", params={"jobId": j}).raise_for_status()
    final = wait_for_job(j, timeout=120)
    assert final == "STOPPED", f"expected STOPPED, got {final}"
