import pytest
import time
from conftest import (write_zip, make_quote_file, make_pricing_file,
                      trigger_job, wait_for_job, get_logs)

def test_pricing_newer_date_wins(clean_db, input_dir, db):
    # 1. Ingest base record
    write_zip(input_dir, "EIS_INT_US_PRICING.PRC.25DA1.20260425.1.1.1.1.txt.zip",
              make_pricing_file(rows=[{"Quote_ID": "Q1", "Trade_Date": "20260429", "Close_Price": "100.00"}]))
    
    job1 = trigger_job()
    assert wait_for_job(job1) == "COMPLETED"

    with db.cursor() as cur:
        cur.execute("SELECT trade_date, close_price FROM lseg_pricing WHERE quote_id='Q1'")
        res = cur.fetchone()
        assert res[0] == "20260429"
        assert res[1] == "100.00"

    # 2. Ingest OLDER record (should NOT overwrite)
    write_zip(input_dir, "EIS_INT_US_PRICING.PRC.25DA1.20260425.2.1.1.1.txt.zip",
              make_pricing_file(rows=[{"Quote_ID": "Q1", "Trade_Date": "20260428", "Close_Price": "90.00"}]))
    
    job2 = trigger_job()
    assert wait_for_job(job2) == "COMPLETED"

    with db.cursor() as cur:
        cur.execute("SELECT trade_date, close_price FROM lseg_pricing WHERE quote_id='Q1'")
        res = cur.fetchone()
        assert res[0] == "20260429"  # Still 29
        assert res[1] == "100.00"    # Still 100

    # 3. Ingest NEWER record (SHOULD overwrite)
    write_zip(input_dir, "EIS_INT_US_PRICING.PRC.25DA1.20260425.3.1.1.1.txt.zip",
              make_pricing_file(rows=[{"Quote_ID": "Q1", "Trade_Date": "20260430", "Close_Price": "110.00"}]))
    
    job3 = trigger_job()
    assert wait_for_job(job3) == "COMPLETED"

    with db.cursor() as cur:
        cur.execute("SELECT trade_date, close_price FROM lseg_pricing WHERE quote_id='Q1'")
        res = cur.fetchone()
        assert res[0] == "20260430"
        assert res[1] == "110.00"

def test_quotes_deduplication_null_asset(clean_db, input_dir, db):
    # 1. Ingest record with Asset_ID
    write_zip(input_dir, "EIS_INT_GLOBAL_QUOTES.INT.99999.20260425.1.1.1.txt.zip",
              make_quote_file(rows=[{"Action": "I", "Asset_ID": "A1", "Quote_ID": "Q1", "RIC": "RIC1"}]))
    
    job1 = trigger_job()
    assert wait_for_job(job1) == "COMPLETED"

    with db.cursor() as cur:
        cur.execute("SELECT asset_id, ric FROM lseg_quotes WHERE quote_id='Q1'")
        res = cur.fetchone()
        assert res[0] == "A1"
        assert res[1] == "RIC1"

    # 2. Ingest record with NULL Asset_ID (should collapse into same row)
    write_zip(input_dir, "EIS_INT_GLOBAL_QUOTES.INT.99999.20260425.2.1.1.txt.zip",
              make_quote_file(rows=[{"Action": "I", "Asset_ID": "", "Quote_ID": "Q1", "RIC": "RIC1_UPDATED"}]))
    
    job2 = trigger_job()
    assert wait_for_job(job2) == "COMPLETED"

    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM lseg_quotes WHERE quote_id='Q1'")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT asset_id, ric FROM lseg_quotes WHERE quote_id='Q1'")
        res = cur.fetchone()
        assert res[0] is None or res[0] == "" # Depending on how binder handles empty
        assert res[1] == "RIC1_UPDATED"

def test_pricing_async_processing(clean_db, input_dir, db):
    # Need at least one Phase 1 file and one PRICING file
    write_zip(input_dir, "Organization.INT.99999.20260425.1.1.1.txt.zip",
              make_org_int_file(rows=[{"Action": "I", "Entity_ID": "E1"}]))
    write_zip(input_dir, "EIS_INT_US_PRICING.PRC.25DA1.20260425.1.1.1.1.txt.zip",
              make_pricing_file(rows=[{"Quote_ID": "Q1", "Trade_Date": "20260429"}]))

    job = trigger_job()
    assert wait_for_job(job) == "COMPLETED"
    
    # Check logs for async behavior
    logs = get_logs(lines=200)
    
    assert "Starting target pipeline for ORGS" in logs
    assert "Target pipeline for ORGS finished" in logs
    assert "Submitting 1 PRICING files to background executor" in logs
    assert "Ingestion session finished for job" in logs
    # completion of PRICING might happen slightly after "session finished" in logs
    
    # Wait a bit more for background pricing to finish if needed
    time.sleep(5)
    logs = get_logs(lines=200)
    assert "Background PRICING complete: files=1 success=true" in logs

def make_org_int_file(seq=1, rows=None, business_date="20260425"):
    rows = rows if rows is not None else []
    declared = len(rows)
    header_metadata = (f"Organization|INT|99999|{business_date}|{seq}|{declared}|")
    header = "Action|Entity_ID|Entity_Perm_ID|Issuer_Name|Company_Short_Name|"
    lines = [header_metadata, header]
    for r in rows:
        lines.append("|".join([r.get("Action", "I"),
                              r.get("Entity_ID", ""),
                              r.get("Entity_Perm_ID", ""),
                              r.get("Issuer_Name", ""),
                              r.get("Company_Short_Name", ""),
                              ""]))
    return "\n".join(lines) + "\n"
