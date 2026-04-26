import os
import shutil
import time
import requests
import zipfile
import io

API_URL = "http://localhost:8081/api/jobs/trigger"
INST_DIR = "test-input/inst1"

def create_zip(name, filename, content):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr(filename, content)
    with open(os.path.join(INST_DIR, name), "wb") as f:
        f.write(buf.getvalue())

def main():
    if not os.path.exists(INST_DIR): os.makedirs(INST_DIR)
    
    # 1. Missing Headers
    print("Test 1: Missing Headers", flush=True)
    create_zip("EIS_INT_GLOBAL_ASSETS.REF.999.20260425.1.1.1.txt.zip", 
               "data.txt", "123|Asset1|Type1\n456|Asset2|Type2") # No header line
    
    # 2. Too Big Data (VARCHAR truncation/error)
    print("Test 2: Too Big Data", flush=True)
    long_val = "A" * 5000
    create_zip("EIS_INT_GLOBAL_ASSETS.REF.999.20260425.2.1.1.txt.zip", 
               "data.txt", "asset_id|name|type\nID1|" + long_val + "|Type")

    # 3. Wrong Data (Alpha in Numeric)
    print("Test 3: Wrong Data", flush=True)
    create_zip("EIS_INT_GLOBAL_ASSETS.REF.999.20260425.3.1.1.txt.zip", 
               "data.txt", "asset_id|name|type\nWRONG_ID|Name|Type")

    # Trigger job
    resp = requests.post(API_URL)
    print(f"Triggered resilience job: {resp.json()}", flush=True)
    
    # Wait and check audit
    time.sleep(10)
    # sabotaged files should probably show some FAILED or SKIPPED in audit.
    
if __name__ == "__main__":
    main()
