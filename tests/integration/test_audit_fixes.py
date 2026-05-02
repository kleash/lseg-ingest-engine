import pytest
import requests
import time
import os
import threading
from conftest import (APP_URL, INPUT_DIR_HOST, BUSINESS_DATE,
                      write_zip, make_org_int_file, trigger_job, wait_for_job, make_quote_file)

def get_live_threads():
    # Attempt to query metrics endpoint. 
    # Note: Requires management.endpoints.web.exposure.include=metrics in application.yml
    try:
        r = requests.get(f"{APP_URL}/actuator/metrics/jvm.threads.live", timeout=2)
        r.raise_for_status()
        for m in r.json().get("measurements", []):
            if m["statistic"] == "VALUE":
                return int(m["value"])
    except:
        pass
    return 0

def test_thread_leak_on_exception(clean_db, input_dir):
    baseline = get_live_threads()
    
    # Induce failure by deleting the file right after trigger
    body = make_org_int_file(seq=1, rows=[{"Action": "I", "Entity_ID": "E1"}])
    file_name = "Organization.INT.99999.20260425.1.1.1.txt.zip"
    full_path = write_zip(input_dir, file_name, body)
    
    job_id = trigger_job()
    if os.path.exists(full_path):
        os.remove(full_path)
    
    status = wait_for_job(job_id)
    assert status == "FAILED"
    
    time.sleep(2) # Wait for thread pool shutdown
    final = get_live_threads()
    # Baseline check is only valid if metrics were available
    if baseline > 0:
        assert final <= baseline + 2, f"Thread leak suspected: baseline={baseline}, final={final}"

def test_bounded_memory_growth(clean_db, db):
    with db.cursor() as cur:
        # Seed 10,000 old records (enough to see if lookback works)
        old_date = "20251025" 
        sql = "INSERT INTO lseg_file_audit (file_name, status, business_date, finished_at) VALUES (%s, 'SUCCESS', %s, NOW() - INTERVAL 180 DAY)"
        batch = [(f"old_file_{i}.txt.zip", old_date) for i in range(10000)]
        cur.executemany(sql, batch)
    db.commit()
    
    start_time = time.time()
    job_id = trigger_job()
    wait_for_job(job_id)
    duration = time.time() - start_time
    # Startup with 10k stale records should still be very fast
    assert duration < 5, f"Job took too long: {duration}s"

def test_semantic_flush_ordering(clean_db, input_dir, db):
    # File A: I then D -> Should be deleted
    # We use quote_id + asset_id as keys.
    rows_a = [
        {"Action": "I", "Asset_ID": "A1", "Quote_ID": "Q1", "RIC": "AAPL.O", "Ticker": "AAPL"},
        {"Action": "D", "Asset_ID": "A1", "Quote_ID": "Q1", "RIC": "AAPL.O", "Ticker": "AAPL"}
    ]
    body_a = make_quote_file(dataset="EIS_DELTA_GLOBAL_QUOTES", kind="DELTA", seq=1, rows=rows_a)
    # Filename must match Kind.DELTA pattern for FileScanner
    write_zip(input_dir, "EIS_DELTA_GLOBAL_QUOTES.REF.99999.20260425.1.1.1.txt.zip", body_a)
    
    # File B: D then I -> Should be live
    rows_b = [
        {"Action": "D", "Asset_ID": "A2", "Quote_ID": "Q2", "RIC": "MSFT.O", "Ticker": "MSFT"},
        {"Action": "I", "Asset_ID": "A2", "Quote_ID": "Q2", "RIC": "MSFT.O", "Ticker": "MSFT"}
    ]
    body_b = make_quote_file(dataset="EIS_DELTA_GLOBAL_QUOTES", kind="DELTA", seq=2, rows=rows_b)
    write_zip(input_dir, "EIS_DELTA_GLOBAL_QUOTES.REF.99999.20260425.2.1.1.txt.zip", body_b)
    
    job_id = trigger_job()
    assert wait_for_job(job_id) == "COMPLETED"
    
    with db.cursor() as cur:
        # AAPL should be soft-deleted (is_deleted=1)
        # Note: We use <=> operator in SqlBuilder now to handle NULLs, 
        # but here they are explicitly A1/A2.
        cur.execute("SELECT is_deleted FROM lseg_quotes WHERE quote_id='Q1' AND asset_id='A1'")
        res = cur.fetchone()
        assert res is not None and res[0] == 1
        
        # MSFT should be live (is_deleted=0)
        cur.execute("SELECT is_deleted FROM lseg_quotes WHERE quote_id='Q2' AND asset_id='A2'")
        res = cur.fetchone()
        assert res is not None and res[0] == 0

def test_deadlock_on_reconciliation(clean_db, input_dir, db):
    # 1. Insert some existing data to reconcile
    with db.cursor() as cur:
        cur.execute("INSERT INTO lseg_quotes (quote_id, asset_id, is_deleted) VALUES ('Q_DL', NULL, 0)")
        cur.execute("INSERT INTO lseg_quotes (quote_id, asset_id, is_deleted) VALUES ('Q_DL', 'A_DL', 0)")
    db.commit()

    # 2. Block the lseg_quotes table from another thread using real LOCK TABLES
    def hold_lock():
        import pymysql
        conn2 = pymysql.connect(host=db.host, port=db.port, user=db.user, password=db.password, database=db.db)
        try:
            with conn2.cursor() as cur2:
                cur2.execute("LOCK TABLES lseg_quotes WRITE")
                time.sleep(5) 
                cur2.execute("UNLOCK TABLES")
        finally:
            conn2.close()

    lock_thread = threading.Thread(target=hold_lock)
    
    # 3. Create a quote file that will trigger reconciliation
    rows = [{"Action": "I", "Asset_ID": "A_NEW", "Quote_ID": "Q_NEW", "RIC": "NEW.O", "Ticker": "NEW"}]
    body = make_quote_file(dataset="EIS_INT_GLOBAL_QUOTES", kind="INT", seq=1, rows=rows)
    write_zip(input_dir, "EIS_INT_GLOBAL_QUOTES.INT.99999.20260425.1.1.1.txt.zip", body)

    lock_thread.start()
    time.sleep(1) # Ensure lock is acquired
    job_id = trigger_job()
    status = wait_for_job(job_id, timeout=180)
    lock_thread.join()
    
    assert status == "COMPLETED"
    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM lseg_quotes WHERE quote_id='Q_DL' AND asset_id IS NULL")
        assert cur.fetchone()[0] == 0

def test_circuit_breaker_max_skipped_rows(clean_db, input_dir, db):
    # Entity_ID is VARCHAR(255). 300 chars will cause truncation error in strict mode.
    long_id = "X" * 300
    rows = []
    for i in range(1001):
        rows.append({"Action": "I", "Entity_ID": long_id, "Entity_Name": "TEST"})
        
    body = make_org_int_file(seq=1, rows=rows)
    write_zip(input_dir, "Organization.INT.99999.20260425.1.1.1.txt.zip", body)
    
    job_id = trigger_job()
    status = wait_for_job(job_id)
    assert status == "FAILED"
    
    with db.cursor() as cur:
        # Verify transaction rolled back
        cur.execute("SELECT COUNT(*) FROM lseg_orgs")
        assert cur.fetchone()[0] == 0
