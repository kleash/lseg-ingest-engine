"""Corner cases: malformed files, action ordering, RIC filter, sanity failures."""
import os
import zipfile
import pytest
from conftest import (write_zip, make_org_int_file, make_org_delta_file,
                      trigger_job, wait_for_job, INPUT_DIR_HOST)


def _quote_int_file(seq, rows, business_date="20260425"):
    declared = len(rows)
    meta = f"EIS_INT_US_EQU_QUOTE|INT|99999|{business_date}|{seq}|{declared}|"
    header = "Action|Asset_ID|Entity_ID|Entity_Perm_ID|Issue_Perm_ID|Quote_ID|Quote_Perm_ID|RIC|Ticker|"
    lines = [meta, header]
    for r in rows:
        lines.append("|".join([
            r.get("Action", "I"),
            r.get("Asset_ID", ""),
            r.get("Entity_ID", ""),
            r.get("Entity_Perm_ID", ""),
            r.get("Issue_Perm_ID", ""),
            r.get("Quote_ID", ""),
            r.get("Quote_Perm_ID", ""),
            r.get("RIC", ""),
            r.get("Ticker", ""),
            ""]))
    return "\n".join(lines) + "\n"


def _quote_delta_file(seq, rows, business_date="20260425"):
    declared = len(rows)
    meta = f"EIS_DELTA_ASIA_US_QUOTE|REF|99999|{business_date}|{seq}|{declared}|"
    header = "Action|Asset_ID|Entity_ID|Entity_Perm_ID|Issue_Perm_ID|Quote_ID|Quote_Perm_ID|RIC|Ticker|"
    lines = [meta, header]
    for r in rows:
        lines.append("|".join([
            r.get("Action", "I"),
            r.get("Asset_ID", ""),
            r.get("Entity_ID", ""),
            r.get("Entity_Perm_ID", ""),
            r.get("Issue_Perm_ID", ""),
            r.get("Quote_ID", ""),
            r.get("Quote_Perm_ID", ""),
            r.get("RIC", ""),
            r.get("Ticker", ""),
            ""]))
    return "\n".join(lines) + "\n"


def test_d_then_i_same_key_keeps_row_live(clean_db, input_dir, db):
    """C1 fix: in-file D then I for same key should leave row live (is_deleted=0)."""
    # First an INT to create the row
    write_zip(input_dir, "Organization.INT.99999.20260425.1.1.1.txt.zip",
              make_org_int_file(seq=1, rows=[
                  {"Action": "I", "Entity_ID": "E1", "Entity_Perm_ID": "P1", "Issuer_Name": "v1"},
              ]))
    # Then a DELTA that does D then I in the same file
    write_zip(input_dir, "EIS_DELTA_GLOABL_ORGN.REF.99999.20260425.1.1.1.txt.zip",
              make_org_delta_file(seq=1, rows=[
                  {"Action": "D", "Entity_ID": "E1", "Entity_Perm_ID": "P1"},
                  {"Action": "I", "Entity_ID": "E1", "Entity_Perm_ID": "P1", "Issuer_Name": "v2"},
              ]))
    job = trigger_job()
    assert wait_for_job(job) == "COMPLETED"

    with db.cursor() as cur:
        cur.execute("SELECT issuer_name, is_deleted FROM lseg_orgs WHERE entity_id='E1'")
        name, deleted = cur.fetchone()
        assert deleted == 0, "D-then-I in same file must leave row live"
        assert name == "v2"


def test_i_then_d_same_key_leaves_row_deleted(clean_db, input_dir, db):
    """In-file I then D should leave row soft-deleted."""
    write_zip(input_dir, "EIS_DELTA_GLOABL_ORGN.REF.99999.20260425.1.1.1.txt.zip",
              make_org_delta_file(seq=1, rows=[
                  {"Action": "I", "Entity_ID": "E2", "Entity_Perm_ID": "P2", "Issuer_Name": "v"},
                  {"Action": "D", "Entity_ID": "E2", "Entity_Perm_ID": "P2"},
              ]))
    job = trigger_job()
    assert wait_for_job(job) == "COMPLETED"

    with db.cursor() as cur:
        cur.execute("SELECT is_deleted FROM lseg_orgs WHERE entity_id='E2'")
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 1


def test_ric_caret_filter_int_only(clean_db, input_dir, db):
    """RIC containing '^' is dropped only on INT-quote rows; DELTA quotes keep them."""
    int_body = _quote_int_file(seq=1, rows=[
        {"Asset_ID": "A1", "Quote_ID": "Q1", "RIC": "AAPL.O"},
        {"Asset_ID": "A2", "Quote_ID": "Q2", "RIC": "BAD^X"},
    ])
    write_zip(input_dir, "EIS_INT_US_EQU_QUOTE.INT.99999.20260425.1.1.1.txt.zip", int_body)
    delta_body = _quote_delta_file(seq=1, rows=[
        {"Asset_ID": "A3", "Quote_ID": "Q3", "RIC": "DELTA^Y"},
    ])
    write_zip(input_dir, "EIS_DELTA_ASIA_US_QUOTE.REF.99999.20260425.1.1.1.txt.zip", delta_body)

    job = trigger_job()
    assert wait_for_job(job) == "COMPLETED"

    with db.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM lseg_quotes WHERE ric LIKE '%^%'")
        # Only DELTA rows with ^ should remain
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT COUNT(*) FROM lseg_quotes")
        assert cur.fetchone()[0] == 2  # AAPL.O + DELTA^Y; BAD^X was filtered


def test_sanity_fail_wrong_business_date(clean_db, input_dir, db):
    body = make_org_int_file(seq=1, rows=[
        {"Action": "I", "Entity_ID": "E1", "Entity_Perm_ID": "P1", "Issuer_Name": "x"},
    ], business_date="20990101")
    write_zip(input_dir, "Organization.INT.99999.20260425.1.1.1.txt.zip", body)

    job = trigger_job()
    assert wait_for_job(job) == "COMPLETED"

    with db.cursor() as cur:
        cur.execute("SELECT status, error_message FROM lseg_file_audit")
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "SKIPPED_SANITY"
        assert "business date" in (rows[0][1] or "")


def test_corrupt_zip_does_not_kill_job(clean_db, input_dir, db):
    """A non-zip file with the right name fails sanity but doesn't abort the job."""
    bad = os.path.join(input_dir, "Organization.INT.99999.20260425.1.1.1.txt.zip")
    with open(bad, "wb") as f:
        f.write(b"not a zip")

    # Add one good file alongside
    write_zip(input_dir, "Organization.INT.99999.20260425.2.1.1.txt.zip",
              make_org_int_file(seq=2, rows=[{"Action": "I", "Entity_ID": "E_OK",
                                              "Entity_Perm_ID": "P_OK", "Issuer_Name": "ok"}]))

    job = trigger_job()
    assert wait_for_job(job) == "COMPLETED"

    with db.cursor() as cur:
        cur.execute("SELECT file_name, status FROM lseg_file_audit ORDER BY file_name")
        rows = dict(cur.fetchall())
        assert rows["Organization.INT.99999.20260425.1.1.1.txt.zip"] == "SKIPPED_SANITY"
        assert rows["Organization.INT.99999.20260425.2.1.1.txt.zip"] == "SUCCESS"
        cur.execute("SELECT COUNT(*) FROM lseg_orgs")
        assert cur.fetchone()[0] == 1


def test_extra_and_missing_columns_tolerated(clean_db, input_dir, db):
    """Extras ignored; missing columns left as NULL on insert."""
    meta = "Organization|INT|99999|20260425|1|1|"
    # Header with an extra column FOO and missing Issuer_Name
    header = "Action|Entity_ID|Entity_Perm_ID|Company_Short_Name|FOO|"
    body = "\n".join([meta, header, "I|E1|P1|ShortCo|extra|"]) + "\n"
    write_zip(input_dir, "Organization.INT.99999.20260425.1.1.1.txt.zip", body)

    job = trigger_job()
    assert wait_for_job(job) == "COMPLETED"

    with db.cursor() as cur:
        cur.execute("SELECT entity_id, company_short_name, issuer_name FROM lseg_orgs")
        eid, short, name = cur.fetchone()
        assert eid == "E1"
        assert short == "ShortCo"
        assert name is None


def test_empty_zip_marked_sanity_skipped(clean_db, input_dir, db):
    fp = os.path.join(input_dir, "Organization.INT.99999.20260425.1.1.1.txt.zip")
    with zipfile.ZipFile(fp, "w") as zf:
        pass  # empty
    job = trigger_job()
    assert wait_for_job(job) == "COMPLETED"
    with db.cursor() as cur:
        cur.execute("SELECT status FROM lseg_file_audit")
        rows = [r[0] for r in cur.fetchall()]
        assert rows == ["SKIPPED_SANITY"]
