import zipfile
import os
import re
import mysql.connector
import sys

# DB Connection
db_config = {
    'host': '127.0.0.1',
    'user': 'ingest',
    'password': 'ingestpw',
    'database': 'lseg'
}

input_dir = "./input"

def get_audit_data():
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT file_name, declared_rows, parsed_rows, inserted_rows, skipped_rows, status FROM lseg_file_audit")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return {r['file_name']: r for r in rows}

def count_file_rows(file_path):
    """Counts data rows (starting with I, U, or D) in an LSEG zip file."""
    data_count = 0
    with zipfile.ZipFile(file_path, 'r') as z:
        for name in z.namelist():
            with z.open(name) as f:
                for line in f:
                    decoded = line.decode('utf-8', errors='ignore').strip()
                    if not decoded: continue
                    if decoded.startswith(('I|', 'U|', 'D|')):
                        data_count += 1
    return data_count

def main():
    print(f"Starting reconciliation for {input_dir}...")
    audit_map = get_audit_data()
    files = [f for f in os.listdir(input_dir) if f.endswith('.txt.zip') and '.note.' not in f and 'Reference-INT-EQUI-' not in f]
    
    discrepancies = []
    total_files = len(files)
    
    for i, fname in enumerate(files):
        if fname not in audit_map:
            discrepancies.append(f"MISSING FROM AUDIT: {fname}")
            continue
            
        audit = audit_map[fname]
        if audit['status'] != 'SUCCESS':
            discrepancies.append(f"NOT SUCCESSFUL: {fname} (status={audit['status']})")
            continue
            
        # Verify internal consistency: parsed = inserted + skipped
        if audit['parsed_rows'] != (audit['inserted_rows'] + audit['skipped_rows']):
            discrepancies.append(f"COUNT MISMATCH (Audit): {fname} (parsed={audit['parsed_rows']} != inserted={audit['inserted_rows']} + skipped={audit['skipped_rows']})")

        # Spot check: verify actual file rows vs audit parsed_rows (every 20th file to save time)
        if i % 20 == 0:
            actual_rows = count_file_rows(os.path.join(input_dir, fname))
            if actual_rows != audit['parsed_rows']:
                discrepancies.append(f"COUNT MISMATCH (File): {fname} (file_rows={actual_rows} != audit_parsed={audit['parsed_rows']})")
            else:
                print(f"Verified {fname}: {actual_rows} rows match audit.")

    print("\n--- Reconciliation Summary ---")
    if not discrepancies:
        print("ALL VERIFIED SAMPLES AND AUDIT COUNTS MATCH!")
        print(f"Total files checked: {total_files}")
    else:
        print(f"Found {len(discrepancies)} discrepancies:")
        for d in discrepancies:
            print(f"  - {d}")

if __name__ == "__main__":
    main()
