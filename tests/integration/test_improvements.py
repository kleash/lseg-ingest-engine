import os
import time
import pytest
import requests
from conftest import (write_zip, make_org_int_file, trigger_job, 
                      wait_for_job, INPUT_DIR_HOST, APP_URL, get_logs)

def test_mandatory_business_date(clean_db):
    """Verify that triggering a job without businessDate fails."""
    r = requests.post(f"{APP_URL}/api/jobs/trigger", params={"inputDir": "/test-data"})
    assert r.status_code == 200
    assert "failed, business date is required" in r.json().get("result", "")

def test_audit_optimization_skips_old_records(clean_db, input_dir, db):
    """Verify that loadSuccessFileNames only considers records from the last month."""
    fname = "Organization.INT.99999.20260425.1.1.1.txt.zip"
    write_zip(input_dir, fname, make_org_int_file(seq=1, rows=[{"Entity_ID": "E1"}]))
    
    # Run once to mark as SUCCESS
    job1 = trigger_job()
    assert wait_for_job(job1) == "COMPLETED"
    
    with db.cursor() as cur:
        cur.execute("SELECT status FROM lseg_file_audit WHERE file_name=%s", (fname,))
        assert cur.fetchone()[0] == "SUCCESS"
        
        # Manually backdate the finished_at to 2 months ago
        cur.execute("UPDATE lseg_file_audit SET finished_at = DATE_SUB(NOW(), INTERVAL 2 MONTH) WHERE file_name=%s", (fname,))
    
    # Run again - should NOT be skipped because it's older than 1 month
    # Note: Idempotency (ON DUPLICATE KEY UPDATE) will just overwrite the same row in lseg_orgs
    job2 = trigger_job()
    assert wait_for_job(job2) == "COMPLETED"
    
    logs = get_logs(500)
    # If it was skipped, we would see "Skipping already-ingested file"
    # If it was NOT skipped, we see "Ingestion started: file=..."
    assert f"Ingestion started: file={fname}" in logs
    assert f"Skipping already-ingested file {fname}" not in logs

def test_progress_logging(clean_db, input_dir):
    """Verify that progress is logged for large files."""
    # We need > 5000 rows to trigger progress logging (default checkRows=5000)
    rows = [{"Entity_ID": f"E{i}", "Issuer_Name": "Name"} for i in range(5001)]
    fname = "Organization.INT.99999.20260425.1.1.1.txt.zip"
    write_zip(input_dir, fname, make_org_int_file(seq=1, rows=rows))
    
    job = trigger_job()
    assert wait_for_job(job) == "COMPLETED"
    
    logs = get_logs(1000)
    assert "Progress: parsed=5000" in logs
