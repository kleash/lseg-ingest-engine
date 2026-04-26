import os
import shutil
import time
import requests
import zipfile
import io

API_URL = "http://localhost:8081/api/jobs/trigger"
INST_DIR = "test-input/inst1"

def create_large_zip(name, filename, num_rows):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        content = "asset_id|entity_id|level\n" 
        for i in range(num_rows):
            content += f"SABO-{i}|E-{i}|L1\n"
        z.writestr(filename, content)
    with open(os.path.join(INST_DIR, name), "wb") as f:
        f.write(buf.getvalue())

def main():
    if not os.path.exists(INST_DIR): os.makedirs(INST_DIR)
    
    # Corrected pattern: EIS_INT_SABOTAGE_ASSETS.INT.999.20260425.1.1.1.txt.zip
    filename = "EIS_INT_SABOTAGE_ASSETS.INT.999.20260425.1.1.1.txt.zip"
    print(f"Creating large file {filename}...", flush=True)
    create_large_zip(filename, "data.txt", 200000)
    
    print("Triggering job...", flush=True)
    requests.post(API_URL)
    
    print("Waiting for ingestion to start (10s)...", flush=True)
    time.sleep(10)
    
    print(f"SABOTAGE: Deleting {filename} mid-ingestion!", flush=True)
    try:
        os.remove(os.path.join(INST_DIR, filename))
        print("File deleted.", flush=True)
    except Exception as e:
        print(f"Failed to delete: {e}", flush=True)

if __name__ == "__main__":
    main()
