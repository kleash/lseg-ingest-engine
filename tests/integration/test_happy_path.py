"""End-to-end happy-path tests against the running stack with synthetic small files."""
import pytest
from conftest import (write_zip, make_org_int_file, make_org_delta_file,
                      trigger_job, wait_for_job)


def test_int_only_one_file(clean_db, input_dir, db):
    rows = [{"Action": "I", "Entity_ID": f"E{i}", "Entity_Perm_ID": f"P{i}",
             "Issuer_Name": f"Org {i}"} for i in range(1, 11)]
    body = make_org_int_file(seq=1, rows=rows)
    write_zip(input_dir, "Organization.INT.99999.20260425.1.1.1.txt.zip", body)

    job = trigger_job()
    assert wait_for_job(job) == "COMPLETED"

    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM lseg_orgs")
        assert cur.fetchone()[0] == 10
        cur.execute("SELECT COUNT(*) FROM lseg_file_audit WHERE status='SUCCESS'")
        assert cur.fetchone()[0] == 1


def test_delta_after_int_uses_seq_order(clean_db, input_dir, db):
    int_body = make_org_int_file(seq=1, rows=[
        {"Action": "I", "Entity_ID": "E1", "Entity_Perm_ID": "P1", "Issuer_Name": "v1"},
        {"Action": "I", "Entity_ID": "E2", "Entity_Perm_ID": "P2", "Issuer_Name": "v1"},
    ])
    write_zip(input_dir, "Organization.INT.99999.20260425.1.1.1.txt.zip", int_body)

    # DELTA seq 1 updates E1 to v2; seq 2 updates E1 to v3 — final must be v3.
    write_zip(input_dir, "EIS_DELTA_GLOABL_ORGN.REF.99999.20260425.1.1.1.txt.zip",
              make_org_delta_file(seq=1, rows=[{"Action": "U", "Entity_ID": "E1",
                                                 "Entity_Perm_ID": "P1", "Issuer_Name": "v2"}]))
    write_zip(input_dir, "EIS_DELTA_GLOABL_ORGN.REF.99999.20260425.2.1.1.txt.zip",
              make_org_delta_file(seq=2, rows=[{"Action": "U", "Entity_ID": "E1",
                                                 "Entity_Perm_ID": "P1", "Issuer_Name": "v3"}]))

    job = trigger_job()
    assert wait_for_job(job) == "COMPLETED"

    with db.cursor() as cur:
        cur.execute("SELECT issuer_name FROM lseg_orgs WHERE entity_id='E1'")
        assert cur.fetchone()[0] == "v3"


def test_idempotent_rerun(clean_db, input_dir, db):
    body = make_org_int_file(seq=1, rows=[
        {"Action": "I", "Entity_ID": "E1", "Entity_Perm_ID": "P1", "Issuer_Name": "x"},
    ])
    write_zip(input_dir, "Organization.INT.99999.20260425.1.1.1.txt.zip", body)

    j1 = trigger_job()
    assert wait_for_job(j1) == "COMPLETED"
    j2 = trigger_job()
    assert wait_for_job(j2) == "COMPLETED"

    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM lseg_orgs")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT COUNT(*) FROM lseg_file_audit")
        assert cur.fetchone()[0] == 1
