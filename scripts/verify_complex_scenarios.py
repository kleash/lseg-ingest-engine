import requests
import time
import mysql.connector
import sys
import os

sys.path.append(os.path.dirname(__file__))
import generate_synthetic_suite

def connect_owner():
    return mysql.connector.connect(
        host="localhost", port=3306, user="owner", password="ownerpw", database="lseg"
    )

def trigger_job(date, dir):
    resp = requests.post(f"http://localhost:8080/api/jobs/trigger?businessDate={date}&inputDir={dir}")
    return resp.json()['jobId']

def stop_job(job_id):
    requests.post(f"http://localhost:8080/api/jobs/stop?jobId={job_id}")

def get_status(job_id):
    resp = requests.get(f"http://localhost:8080/api/jobs/status?jobId={job_id}")
    return resp.json()['status']

def main():
    print("--- Starting Comprehensive Complex Scenario Test ---")
    
    db = connect_owner()
    cursor = db.cursor()
    
    # 1. Cleanup
    cursor.execute("DELETE FROM lseg_orgs")
    cursor.execute("DELETE FROM lseg_assets")
    cursor.execute("DELETE FROM lseg_quotes")
    cursor.execute("DELETE FROM lseg_pricing")
    cursor.execute("DELETE FROM lseg_dss_bonds")
    cursor.execute("DELETE FROM lseg_file_audit")
    cursor.execute("DELETE FROM lseg_jobs")
    db.commit()

    feed1 = "F1" + str(int(time.time()))
    generate_synthetic_suite.main(feed1)

    # --- SCENARIO 1: Non-blocking Pricing & Multi-job Interleaving ---
    print(f"\nScenario 1: Non-blocking Pricing (Feed {feed1})")
    job1 = trigger_job("20260502", "/test-data/synthetic")
    
    while get_status(job1) == 'QUEUED': time.sleep(0.5)
    while get_status(job1) == 'RUNNING': time.sleep(0.5)
    
    print(f"Job {job1} Main phase done (status={get_status(job1)})")
    
    job2 = trigger_job("20260502", "/test-data/job2")
    start = time.time()
    while get_status(job2) != 'COMPLETED' and time.time() - start < 20:
        time.sleep(1)
    
    if get_status(job2) == 'COMPLETED':
        print("✅ SUCCESS: Job 2 finished while Job 1 Pricing was backgrounded")
    else:
        print(f"❌ FAIL: Job 2 status is {get_status(job2)}")

    # --- SCENARIO 2: Idempotency (Audit Skip) ---
    print("\nScenario 2: Idempotency")
    cursor.execute("SELECT COUNT(*) FROM lseg_file_audit WHERE status='SUCCESS'")
    count_before = cursor.fetchone()[0]
    
    job3 = trigger_job("20260502", "/test-data/job2") 
    while get_status(job3) != 'COMPLETED': time.sleep(0.5)
    
    cursor.execute("SELECT COUNT(*) FROM lseg_file_audit WHERE status='SUCCESS'")
    count_after = cursor.fetchone()[0]
    
    if count_before == count_after:
        print(f"✅ SUCCESS: Idempotency held (Audit count stayed at {count_before})")
    else:
        print(f"❌ FAIL: Idempotency failed (Audit count {count_before} -> {count_after})")

    # --- SCENARIO 4: Bonds Deduplication ---
    print("\nScenario 4: Bonds Deduplication")
    cursor.execute("SELECT issuer_name FROM lseg_dss_bonds WHERE isin='ISIN_B1'")
    res = cursor.fetchone()
    print(f"Bond ISIN_B1 Issuer: {res}")
    if res and res[0] == 'Issuer HK 1 UPDATED':
        print("✅ SUCCESS: Intra-file deduplication worked (Last row wins)")
    else:
        print("❌ FAIL: Bond deduplication failed")

    # --- SCENARIO 5: Quotes Reconciliation ---
    print("\nScenario 5: Quotes Reconciliation")
    cursor.execute("SELECT COUNT(*) FROM lseg_quotes WHERE quote_id='QT_3'")
    count = cursor.fetchone()[0]
    cursor.execute("SELECT asset_id FROM lseg_quotes WHERE quote_id='QT_3'")
    res = cursor.fetchone()
    asset = res[0] if res else None
    print(f"QT_3 count: {count}, asset_id: {asset}")
    if count == 1 and asset == 'AST_1':
        print("✅ SUCCESS: NULL record consolidated and removed")
    else:
        print("❌ FAIL: Quotes reconciliation failed")

    # --- SCENARIO 3: Stop Signal mid-Pricing ---
    print("\nScenario 3: Stop Signal mid-Pricing")
    
    # New feed to avoid conflicts and archive issues
    feed3 = "F3" + str(int(time.time()))
    generate_synthetic_suite.main(feed3)
    
    job4 = trigger_job("20260502", "/test-data/synthetic")
    while get_status(job4) == 'QUEUED': time.sleep(0.1)
    while get_status(job4) == 'RUNNING': time.sleep(0.1)
    
    print(f"Job {job4} finished main phase. Checking if Pricing started...")
    
    start = time.time()
    while time.time() - start < 15:
        try:
            # Check for the HUGE pricing file
            cursor.execute(f"SELECT status FROM lseg_file_audit WHERE file_name LIKE '%PRC.H_{feed3}%'")
            res = cursor.fetchone()
            if res:
                print(f"Pricing audit status: {res[0]}")
                if res[0] == 'STARTED': break
        except: pass
        time.sleep(1)
    else:
        print("❌ FAIL: Pricing never started in audit")
        sys.exit(1)

    print(f"Issuing STOP to Job {job4}...")
    stop_job(job4)
    
    time.sleep(5)
    
    cursor.execute(f"SELECT status, error_message FROM lseg_file_audit WHERE file_name LIKE '%PRC.H_{feed3}%'")
    res = cursor.fetchone()
    print(f"Audit result for stopped pricing: {res}")
    if res and res[0] == 'FAILED' and ('InterruptedException' in str(res[1]) or 'Stop signaled' in str(res[1])):
        print("✅ SUCCESS: Pricing aborted and marked FAILED")
    else:
        print("❌ FAIL: Pricing did not abort as expected")

    cursor.close()
    db.close()

if __name__ == "__main__":
    main()
