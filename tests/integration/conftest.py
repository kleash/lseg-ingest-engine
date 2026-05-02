"""
Shared fixtures for the lseg-ingest integration tests.

Assumptions:
  - docker compose stack is already running (mariadb on localhost:3306, ingest on localhost:8080).
  - Tests connect as root for DB cleanup; ingestion still uses the limited 'ingest' user via the app.

To run:
    docker compose -f docker-compose.yml up -d --build
    pip install -r tests/integration/requirements.txt
    pytest -v tests/integration/

The default fixture set wipes DB state and the input directory between tests so each test
starts on a known-empty cluster. Tests targeting the real production drop are marked
@pytest.mark.realdata and are skipped unless --realdata is passed.
"""
import os
import time
import zipfile
import io
import shutil
import contextlib
import pytest
import pymysql
import requests


APP_URL = os.environ.get("INGEST_APP_URL", "http://localhost:8080")
DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT = int(os.environ.get("DB_PORT", "3306"))
DB_NAME = os.environ.get("DB_NAME", "lseg")
DB_ROOT_USER = os.environ.get("DB_ROOT_USER", "root")
DB_ROOT_PASS = os.environ.get("DB_ROOT_PASS", "rootpw")
INPUT_DIR_HOST = os.environ.get("INPUT_DIR_HOST", "/Users/sa/Downloads/LSEG/lseg-ingest/test-input")
INPUT_DIR_CONTAINER = os.environ.get("INPUT_DIR_CONTAINER", "/test-data")
BUSINESS_DATE = "20260425"


def pytest_addoption(parser):
    parser.addoption("--realdata", action="store_true", default=False,
                     help="run @realdata tests against the production drop")


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--realdata"):
        skip_real = pytest.mark.skip(reason="needs --realdata to run")
        for item in items:
            if "realdata" in item.keywords:
                item.add_marker(skip_real)


@pytest.fixture(scope="session")
def db():
    """Root DB connection used by tests for setup/teardown and assertions."""
    conn = pymysql.connect(host=DB_HOST, port=DB_PORT, user=DB_ROOT_USER,
                           password=DB_ROOT_PASS, db=DB_NAME, autocommit=True)
    yield conn
    conn.close()


@pytest.fixture
def clean_db(db):
    """Clean all data tables and audit/jobs before each test."""
    with db.cursor() as cur:
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        for t in ("lseg_orgs", "lseg_assets", "lseg_quotes", "lseg_dss_bonds"):
            cur.execute(f"TRUNCATE TABLE {t}")
        # Use DELETE for audit and jobs to preserve auto-increment across tests
        for t in ("lseg_file_audit", "lseg_jobs"):
            cur.execute(f"DELETE FROM {t}")
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
    yield db


@pytest.fixture
def input_dir():
    """Empty input dir bind-mounted into the ingest container as /test-data.

    NOTE: do NOT rmtree the directory itself — that breaks the bind mount inode
    on Docker for Mac, leaving the container with a stale empty view. Clear contents instead.
    """
    os.makedirs(INPUT_DIR_HOST, exist_ok=True)
    for entry in os.listdir(INPUT_DIR_HOST):
        path = os.path.join(INPUT_DIR_HOST, entry)
        if os.path.isfile(path) or os.path.islink(path):
            os.remove(path)
        else:
            shutil.rmtree(path)
    yield INPUT_DIR_HOST


def write_zip(input_dir_path, file_name, body_text):
    """Write `body_text` into a zip at <input_dir>/<file_name>."""
    full = os.path.join(input_dir_path, file_name)
    inner_name = file_name.replace(".zip", "")
    with zipfile.ZipFile(full, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(inner_name, body_text)
    return full


def make_org_int_file(seq=1, rows=None, business_date=BUSINESS_DATE):
    """Build an INT Organization file body."""
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


def make_org_delta_file(seq=1, rows=None, business_date=BUSINESS_DATE):
    rows = rows if rows is not None else []
    declared = len(rows)
    header_metadata = (f"EIS_DELTA_GLOABL_ORGN|REF|99999|{business_date}|{seq}|{declared}|")
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


def make_quote_file(dataset="EIS_INT_GLOBAL_QUOTES", kind="INT", seq=1, rows=None, business_date=BUSINESS_DATE):
    rows = rows if rows is not None else []
    declared = len(rows)
    header_metadata = f"{dataset}|{kind}|99999|{business_date}|{seq}|{declared}|"
    header = "Action|Asset_ID|Quote_ID|RIC|Ticker|Currency_Code|Entity_ID|Entity_Perm_ID|Issue_Perm_ID|Level|Quote_Perm_ID|"
    lines = [header_metadata, header]
    for r in rows:
        lines.append("|".join([
            r.get("Action", "I"),
            r.get("Asset_ID", ""),
            r.get("Quote_ID", ""),
            r.get("RIC", ""),
            r.get("Ticker", ""),
            r.get("Currency_Code", ""),
            r.get("Entity_ID", ""),
            r.get("Entity_Perm_ID", ""),
            r.get("Issue_Perm_ID", ""),
            r.get("Level", ""),
            r.get("Quote_Perm_ID", ""),
            ""
        ]))
    return "\n".join(lines) + "\n"


def make_pricing_file(dataset="EIS_INT_US_PRICING", seq=1, chunk=1, rows=None, business_date=BUSINESS_DATE):
    rows = rows if rows is not None else []
    declared = len(rows)
    # PRC format: dataset|PRC|feed|date|batch|chunk|rows|
    header_metadata = f"{dataset}|PRC|99999|{business_date}|{seq}|{chunk}|{declared}|"
    header = "Quote_ID|Trade_Date|Close_Price|Ask_Price|Bid_Price|"
    lines = [header_metadata, header]
    for r in rows:
        lines.append("|".join([
            r.get("Quote_ID", ""),
            r.get("Trade_Date", ""),
            r.get("Close_Price", ""),
            r.get("Ask_Price", ""),
            r.get("Bid_Price", ""),
            ""
        ]))
    return "\n".join(lines) + "\n"


def trigger_job(business_date=BUSINESS_DATE, input_dir_container=INPUT_DIR_CONTAINER):
    r = requests.post(f"{APP_URL}/api/jobs/trigger",
                      params={"businessDate": business_date, "inputDir": input_dir_container})
    r.raise_for_status()
    return r.json()["jobId"]


def wait_for_job(job_id, timeout=120, poll=2):
    """Poll until the job leaves QUEUED/RUNNING. Returns final status."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            r = requests.get(f"{APP_URL}/api/jobs/status", params={"jobId": job_id}, timeout=5)
            last = r.json()["status"]
        except Exception:
            last = None
        if last and last not in ("QUEUED", "RUNNING"):
            return last
        time.sleep(poll)
    raise TimeoutError(f"Job {job_id} did not finish in {timeout}s; last={last}")


def stop_job(job_id):
    requests.post(f"{APP_URL}/api/jobs/stop", params={"jobId": job_id}).raise_for_status()


def get_logs(lines=100):
    """Retrieve the last N lines of logs from the ingest container."""
    import subprocess
    # Find the container name for the 'ingest' service
    try:
        cmd = ["docker", "compose", "ps", "-q", "ingest"]
        container_id = subprocess.check_output(cmd).decode().strip()
        if not container_id:
            return ""
        cmd = ["docker", "logs", "--tail", str(lines), container_id]
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode()
    except Exception as e:
        return f"Error getting logs: {e}"
