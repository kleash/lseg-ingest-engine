import os
import time
import zipfile
import io
import pytest
from conftest import (write_zip, make_org_int_file, trigger_job, 
                      wait_for_job, INPUT_DIR_HOST, db)

def test_delete_file_mid_ingestion(clean_db, input_dir, db):
    """SABOTAGE: Delete file while it's being read.
    On POSIX (Linux/macOS), the open file handle survives the deletion (unlink),
    so the ingestion should actually succeed!
    """
    # Create a very large file to give us time to delete it
    rows = [{"Entity_ID": f"E{i}", "Issuer_Name": "Name"} for i in range(100000)]
    fname = "Organization.INT.99999.20260425.1.1.1.txt.zip"
    write_zip(input_dir, fname, make_org_int_file(seq=1, rows=rows))
    
    job = trigger_job()
    
    # Wait a tiny bit for it to start
    time.sleep(1)
    
    # Sabotage!
    try:
        os.remove(os.path.join(input_dir, fname))
        print(f"File {fname} deleted mid-ingest")
    except FileNotFoundError:
        # If it's already gone, it might have been archived.
        pass
    
    status = wait_for_job(job)
    # On POSIX, this should stay COMPLETED because the handle is still valid.
    assert status == "COMPLETED"
    
    with db.cursor() as cur:
        cur.execute("SELECT status FROM lseg_file_audit WHERE file_name=%s", (fname,))
        res = cur.fetchone()
        assert res[0] == "SUCCESS"

def test_oversized_data_skipped_row(clean_db, input_dir, db):
    """Verify that a row with data too long for VARCHAR(255) is skipped but file succeeds."""
    long_name = "A" * 1000
    rows = [
        {"Entity_ID": "E1", "Issuer_Name": "Good"},
        {"Entity_ID": "E2", "Issuer_Name": long_name}, # This should fail
        {"Entity_ID": "E3", "Issuer_Name": "AlsoGood"}
    ]
    fname = "Organization.INT.99999.20260425.1.1.1.txt.zip"
    write_zip(input_dir, fname, make_org_int_file(seq=1, rows=rows))
    
    job = trigger_job()
    assert wait_for_job(job) == "COMPLETED"
    
    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM lseg_orgs")
        assert cur.fetchone()[0] == 2 # E1 and E3
        
        cur.execute("SELECT skipped_rows, status FROM lseg_file_audit WHERE file_name=%s", (fname,))
        skipped, status = cur.fetchone()
        assert status == "SUCCESS"
        assert skipped == 1

def test_missing_metadata_row(clean_db, input_dir, db):
    """File with only header and data (no metadata) should fail sanity."""
    body = "Action|Entity_ID|Entity_Perm_ID|Issuer_Name|Company_Short_Name|\nI|E1|P1|Name|Short|"
    fname = "Organization.INT.99999.20260425.1.1.1.txt.zip"
    write_zip(input_dir, fname, body)
    
    job = trigger_job()
    assert wait_for_job(job) == "COMPLETED"
    
    with db.cursor() as cur:
        cur.execute("SELECT status, error_message FROM lseg_file_audit WHERE file_name=%s", (fname,))
        status, msg = cur.fetchone()
        assert status == "SKIPPED_SANITY"
        assert "metadata row missing" in msg
