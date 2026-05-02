import mysql.connector
import sys

def connect():
    try:
        return mysql.connector.connect(
            host="localhost",
            port=3306,
            user="ingest",
            password="ingestpw",
            database="lseg"
        )
    except Exception as e:
        print(f"Failed to connect to DB: {e}")
        sys.exit(1)

def check_counts(cursor):
    print("--- Row Counts ---")
    tables = ["lseg_orgs", "lseg_assets", "lseg_quotes", "lseg_pricing", "lseg_dss_bonds", "lseg_file_audit"]
    for t in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {t}")
        count = cursor.fetchone()[0]
        print(f"{t}: {count}")

def verify_scenarios(cursor):
    print("\n--- Scenario Verification ---")
    errors = []

    # 1. ORGS
    cursor.execute("SELECT issuer_name, is_deleted FROM lseg_orgs WHERE entity_id = 'ORG_1'")
    res = cursor.fetchone()
    if not res or res[0] != "Updated Org 1" or res[1] != 0:
        errors.append(f"ORG_1 mismatch: expected ('Updated Org 1', 0), got {res}")

    cursor.execute("SELECT issuer_name FROM lseg_orgs WHERE entity_id = 'ORG_CORNER'")
    res = cursor.fetchone()
    if not res or res[0] is not None:
        errors.append(f"ORG_CORNER mismatch: expected None, got {res}")

    # 2. ASSETS
    cursor.execute("SELECT is_deleted FROM lseg_assets WHERE asset_id = 'AST_3'")
    res = cursor.fetchone()
    if not res or res[0] != 1:
        errors.append(f"AST_3 (I then D) mismatch: expected is_deleted=1, got {res}")

    cursor.execute("SELECT is_deleted, security_long_description FROM lseg_assets WHERE asset_id = 'AST_4'")
    res = cursor.fetchone()
    if not res or res[0] != 0 or res[1] != "Asset 4 Live":
        errors.append(f"AST_4 (D then I) mismatch: expected (0, 'Asset 4 Live'), got {res}")

    # 3. QUOTES
    cursor.execute("SELECT COUNT(*) FROM lseg_quotes WHERE quote_id = 'QT_2'")
    if cursor.fetchone()[0] != 0:
        errors.append("QT_2 (INT caret filter) should have been skipped")

    cursor.execute("SELECT ric FROM lseg_quotes WHERE quote_id = 'QT_4'")
    res = cursor.fetchone()
    if not res or res[0] != "AMZN.O^B":
        errors.append("QT_4 (DELTA caret) should have been kept")

    cursor.execute("SELECT asset_id FROM lseg_quotes WHERE quote_id = 'QT_3'")
    res = cursor.fetchone()
    if not res or res[0] != "AST_3":
        errors.append(f"QT_3 consolidation mismatch: expected AST_3, got {res}")

    # 4. PRICING
    cursor.execute("SELECT close_price FROM lseg_pricing WHERE quote_id = 'QT_1'")
    res = cursor.fetchone()
    if not res or float(res[0]) != 155.00:
        errors.append(f"QT_1 pricing mismatch: expected 155.00, got {res}")

    # 5. AUDIT
    cursor.execute("SELECT COUNT(*) FROM lseg_file_audit WHERE status = 'SUCCESS'")
    if cursor.fetchone()[0] != 10:
        errors.append("Expected 10 successful file audit records")

    if not errors:
        print("✅ ALL SCENARIOS VERIFIED SUCCESSFULLY")
    else:
        print("❌ VERIFICATION FAILED:")
        for e in errors:
            print(f"  - {e}")
        sys.exit(1)

def main():
    db = connect()
    cursor = db.cursor()
    try:
        check_counts(cursor)
        verify_scenarios(cursor)
    finally:
        cursor.close()
        db.close()

if __name__ == "__main__":
    main()
