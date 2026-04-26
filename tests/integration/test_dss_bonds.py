import pytest
import os
from conftest import (trigger_job, wait_for_job)

def test_dss_bonds_ingestion(clean_db, input_dir, db):
    # Create a synthetic CSV file
    csv_content = (
        "ISIN,Instrument ID,Instrument ID Type,RIC,Ticker,Security Description,"
        "Instrument Full Name - ESMA,Security Source,Asset ID,Asset Type,"
        "Asset Type Description,Currency Code,Issuer Name,Issuer LEI,Issuer Short Name\n"
        "SG123,ID1,CHR,RIC1,T1,Desc1,Full1,SRC1,AID1,TYPE1,DESC1,SGD,Issuer1,LEI1,Short1\n"
        "SG456,ID2,CHR,RIC2,T2,Desc2,Full2,SRC2,AID2,TYPE2,DESC2,SGD,\"Issuer 2, Inc.\",LEI2,Short2\n"
    )
    file_name = "SG_HK_Bonds_20260420 070012.csv"
    with open(os.path.join(input_dir, file_name), "w") as f:
        f.write(csv_content)

    # Note: we use business_date="20260420" to match the filename.
    # The app sanity check verifies the business date.
    job = trigger_job(business_date="20260420")
    assert wait_for_job(job) == "COMPLETED"

    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM lseg_dss_bonds")
        assert cur.fetchone()[0] == 2
        cur.execute("SELECT issuer_name FROM lseg_dss_bonds WHERE isin='SG456'")
        assert cur.fetchone()[0] == "Issuer 2, Inc."
        cur.execute("SELECT COUNT(*) FROM lseg_file_audit WHERE status='SUCCESS'")
        assert cur.fetchone()[0] == 1

def test_dss_bonds_upsert(clean_db, input_dir, db):
    # 1. Ingest initial row
    csv1 = (
        "ISIN,Instrument ID,Instrument ID Type,RIC,Ticker,Security Description,"
        "Instrument Full Name - ESMA,Security Source,Asset ID,Asset Type,"
        "Asset Type Description,Currency Code,Issuer Name,Issuer LEI,Issuer Short Name\n"
        "SG123,ID1,CHR,RIC1,T1,v1,Full1,SRC1,AID1,TYPE1,DESC1,SGD,Issuer1,LEI1,Short1\n"
    )
    with open(os.path.join(input_dir, "SG_HK_Bonds_20260420 070012.csv"), "w") as f:
        f.write(csv1)
    
    j1 = trigger_job(business_date="20260420")
    assert wait_for_job(j1) == "COMPLETED"

    # 2. Ingest update row with same unique key (ISIN, ID, Type, RIC)
    csv2 = (
        "ISIN,Instrument ID,Instrument ID Type,RIC,Ticker,Security Description,"
        "Instrument Full Name - ESMA,Security Source,Asset ID,Asset Type,"
        "Asset Type Description,Currency Code,Issuer Name,Issuer LEI,Issuer Short Name\n"
        "SG123,ID1,CHR,RIC1,T1,v2,Full1,SRC1,AID1,TYPE1,DESC1,SGD,Issuer1,LEI1,Short1\n"
    )
    # Use different filename so it is not skipped by audit logic
    with open(os.path.join(input_dir, "SG_HK_Bonds_20260420 070013.csv"), "w") as f:
        f.write(csv2)

    j2 = trigger_job(business_date="20260420")
    assert wait_for_job(j2) == "COMPLETED"

    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM lseg_dss_bonds")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT security_description FROM lseg_dss_bonds WHERE isin='SG123'")
        assert cur.fetchone()[0] == "v2"

def test_dss_bonds_idempotent_rerun(clean_db, input_dir, db):
    csv = (
        "ISIN,Instrument ID,Instrument ID Type,RIC,Ticker,Security Description,"
        "Instrument Full Name - ESMA,Security Source,Asset ID,Asset Type,"
        "Asset Type Description,Currency Code,Issuer Name,Issuer LEI,Issuer Short Name\n"
        "SG123,ID1,CHR,RIC1,T1,v1,Full1,SRC1,AID1,TYPE1,DESC1,SGD,Issuer1,LEI1,Short1\n"
    )
    file_name = "SG_HK_Bonds_20260420 070012.csv"
    with open(os.path.join(input_dir, file_name), "w") as f:
        f.write(csv)
    
    # First run
    j1 = trigger_job(business_date="20260420")
    assert wait_for_job(j1) == "COMPLETED"

    # Second run (should be skipped by audit)
    j2 = trigger_job(business_date="20260420")
    assert wait_for_job(j2) == "COMPLETED"

    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM lseg_file_audit WHERE file_name=%s", (file_name,))
        assert cur.fetchone()[0] == 1  # Only one record in audit means second was skipped early

def test_dss_bonds_skips_ric_and_notes(clean_db, input_dir, db):
    # Valid file
    with open(os.path.join(input_dir, "SG_HK_Bonds_20260420 070012.csv"), "w") as f:
        f.write("ISIN,Instrument ID,Instrument ID Type,RIC,Ticker,Security Description\nSG1,ID1,CHR,R1,T1,D1\n")
    
    # These should be ignored by FileScanner
    with open(os.path.join(input_dir, "SG_HK_Bonds_20260420 070012.ric.csv"), "w") as f:
        f.write("Some RIC data\n")
    with open(os.path.join(input_dir, "SG_HK_Bonds_20260420 070012.csv.notes.txt"), "w") as f:
        f.write("Some notes\n")

    job = trigger_job(business_date="20260420")
    assert wait_for_job(job) == "COMPLETED"

    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM lseg_file_audit")
        assert cur.fetchone()[0] == 1  # Only the valid .csv should have an audit record
        cur.execute("SELECT file_name FROM lseg_file_audit")
        assert cur.fetchone()[0] == "SG_HK_Bonds_20260420 070012.csv"

