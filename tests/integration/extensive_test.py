import requests
import os
import shutil
import time
import subprocess
import pymysql

SOURCE_DIR = "/Users/sa/Downloads/LSEG/20260425"
INST_DIRS = {
    1: "test-input/inst1",
    2: "test-input/inst2",
    3: "test-input/inst3"
}
PORTS = {1: 8081, 2: 8082, 3: 8083}

DB_OPTS = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "rootpw",
    "database": "lseg"
}

def get_db_conn():
    return pymysql.connect(**DB_OPTS)

def clean_inst_dirs():
    for d in INST_DIRS.values():
        if os.path.exists(d):
            for item in os.listdir(d):
                item_path = os.path.join(d, item)
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.unlink(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
        else:
            os.makedirs(d)

def copy_files(n, dest_dir):
    files = sorted([f for f in os.listdir(SOURCE_DIR) if os.path.isfile(os.path.join(SOURCE_DIR, f))])
    if n > 0:
        files = files[:n]
    for f in files:
        shutil.copy(os.path.join(SOURCE_DIR, f), os.path.join(dest_dir, f))
    print(f"Copied {len(files)} files to {dest_dir}", flush=True)

def wait_for_api(inst_id, timeout=60):
    url = f"http://localhost:{PORTS[inst_id]}/actuator/health"
    start = time.time()
    while time.time() - start < timeout:
        try:
            resp = requests.get(url)
            if resp.status_code == 200:
                print(f"Instance {inst_id} is UP.", flush=True)
                return True
        except:
            pass
        time.sleep(2)
    print(f"Timeout waiting for instance {inst_id}", flush=True)
    return False

def trigger_job(inst_id):
    if not wait_for_api(inst_id):
        return None
    url = f"http://localhost:{PORTS[inst_id]}/api/jobs/trigger"
    try:
        resp = requests.post(url)
        print(f"Triggered job on instance {inst_id}: {resp.status_code} - {resp.json()}", flush=True)
        return resp.json().get("jobId")
    except Exception as e:
        print(f"Failed to trigger job on instance {inst_id}: {e}", flush=True)
        return None

def monitor_jobs():
    conn = get_db_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, status, node_id, started_at, finished_at FROM lseg_jobs ORDER BY id DESC")
            return cursor.fetchall()
    finally:
        conn.close()

def monitor_audit_summary():
    conn = get_db_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT status, count(*) FROM lseg_file_audit GROUP BY status")
            return cursor.fetchall()
    finally:
        conn.close()

def main():
    print("--- Starting Multi-Instance Coordination Test ---", flush=True)
    clean_inst_dirs()
    print("Directories cleaned.", flush=True)
    
    # TC 1: copy 100 files to inst1 and trigger
    print("Copying 100 files to inst1...", flush=True)
    copy_files(100, INST_DIRS[1])
    job1 = trigger_job(1)
    
    # TC 2: copy all files to inst2 and trigger
    print("Copying all files to inst2...", flush=True)
    copy_files(0, INST_DIRS[2])
    job2 = trigger_job(2)
    
    # TC 3: copy all files to inst3 and trigger
    print("Copying all files to inst3...", flush=True)
    copy_files(0, INST_DIRS[3])
    job3 = trigger_job(3)
    
    print("\n--- Monitoring Progress (Polling every 10s) ---")
    while True:
        jobs = monitor_jobs()
        audit = monitor_audit_summary()
        
        print("\n--- Jobs Status ---")
        for j in jobs:
            print(f"Job {j[0]}: {j[1]} (Node: {j[2]}, Started: {j[3]}, Finished: {j[4]})")
        
        print("--- Audit Summary ---")
        for a in audit:
            print(f"Status {a[0]}: {a[1]}")
            
        running = any(j[1] == 'RUNNING' or j[1] == 'QUEUED' for j in jobs)
        if not running and len(jobs) >= 3:
            # Check if at least one reached terminal state
            terminal = all(j[1] in ['COMPLETED', 'FAILED', 'STOPPED'] for j in jobs)
            if terminal:
                print("\nAll jobs reached terminal state.")
                break
        
        time.sleep(10)

if __name__ == "__main__":
    main()
