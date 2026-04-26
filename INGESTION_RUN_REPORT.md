# LSEG Ingest — Production Run Report

**Date:** 2026-04-26  
**Environment:** 2-instance cluster (`docker-compose.production.yml`), MariaDB 11, Docker Desktop on macOS (256 GB virtual disk)  
**Source data:** `/Users/sa/Downloads/LSEG/20260425` and `/Users/sa/Downloads/LSEG/20260426`

---

## Run summary

| Job | Business date | Triggered on | Executed by | Duration | Files | Status |
|-----|---------------|-------------|-------------|----------|-------|--------|
| Job 1 | 20260425 | ingest1 (:8081) | ingest2 (cluster-lock winner) | **479 s (7 m 59 s)** | 464 | COMPLETED |
| Job 2 | 20260426 | ingest2 (:8082) | ingest1 (next lock winner) | **64 s (1 m 4 s)** | 93 | COMPLETED |
| **Total** | both | — | — | **543 s (9 m 3 s)** | **557** | — |

> Job 2 was queued for the entire duration of Job 1. ingest1 repeatedly logged  
> `Cluster lock unavailable; another node is running. Leaving job 2 QUEUED.`  
> until Job 1 released the lock — exactly the intended cluster-singleton behaviour.

---

## Cluster-lock & multi-instance behaviour

- Both instances mount both date directories (`/data/20260425`, `/data/20260426`); jobs carry an explicit `inputDir` so whichever node wins the lock reads the correct data.
- At no point did `SELECT COUNT(*) FROM lseg_jobs WHERE status='RUNNING'` exceed 1.
- `node_id` in `lseg_jobs` confirmed which container held the lock for each job.

---

## File-level reconciliation

| Source | Ingestible files | Audit rows (SUCCESS) | Audit rows (other status) |
|--------|-----------------|----------------------|---------------------------|
| 20260425 (1 102 total, 631 `.note`, 7 `Reference-INT-EQUI`) | **464** | **464** | 0 |
| 20260426 (225 total, 132 `.note`) | **93** | **93** | 0 |
| **Combined** | **557** | **557** | **0** |

- Files in source **not** in audit: **0**
- Files in audit **not** in source: **0**
- Duplicate filenames in source: **0**
- Files where `declared_rows ≠ parsed_rows`: **0**
- Files where `parsed_rows ≠ inserted_rows + skipped_rows`: **0**
- Files where `inserted_rows ≠ ins_count + upd_count + del_count`: **0**

---

## Row-level metrics — INT phase

| Target | Files | Parsed rows | Inserted rows | Skipped (caret filter) | Insert % | Throughput |
|--------|-------|-------------|---------------|------------------------|----------|-----------|
| `lseg_orgs` | 2 | 392,524 | 392,524 | 0 | 100% | 4,313 rows/s |
| `lseg_assets` | 144 | 9,582,720 | 9,582,720 | 0 | 100% | 17,713 rows/s |
| `lseg_quotes` | 138 | 49,126,473 | 5,237,888 | 43,888,585 | **10.7%** | 92,343 rows/s |

> The quotes INT insertion rate of 10.7% is expected and by design. The RIC caret filter (`ingest.ricCaretFilter=true`) drops rows where the `RIC` column contains `^`. This filter applies to **all INT files whose target is `lseg_quotes`** (not just EU warrants), activated per-file only when the `RIC` column is present in that file's header. EU warrants quote files (`EIS_INT_EU_WRNTS_QUOTE`) see near-100% filtering because warrants index RICs are almost universally `^`-prefixed; US and Asia equity quote files see much lower filter rates.

---

## Row-level metrics — DELTA/REF phase

| Target | Files | Parsed rows | Inserts (I) | Updates (U) | Soft-deletes (D) |
|--------|-------|-------------|-------------|-------------|-----------------|
| `lseg_orgs` | 11 | 96 | 87 | 9 | 0 |
| `lseg_assets` | 100 | 497,815 | 15,374 | 15,592 | **466,849** |
| `lseg_quotes` | 162 | 71,054 | 2,171 | 55,986 | 12,897 |

---

## Processing timeline

| Phase | Target | Start | End | Wall time |
|-------|--------|-------|-----|-----------|
| INT | orgs | 06:34:12 | 06:35:43 | 91 s |
| DELTA | orgs | 06:35:43 | 06:35:43 | < 1 s |
| INT | assets | 06:34:12 | 06:43:13 | 541 s |
| DELTA | assets | 06:41:20 | 06:43:14 | 114 s |
| INT | quotes | 06:34:12 | 06:43:05 | 532 s |
| DELTA | quotes | 06:41:07 | 06:43:07 | 119 s |

INT phases for all three targets ran in parallel from the same start time. DELTA phases began as soon as all INT files for each respective target completed (orgs DELTA started within milliseconds of orgs INT finishing at 06:35:43).

---

## Final database state

| Table | Total rows | Soft-deleted (`is_deleted=1`) | Active rows |
|-------|-----------|-------------------------------|-------------|
| `lseg_orgs` | 392,610 | 0 | **392,610** |
| `lseg_assets` | 9,581,871 | 461,371 | **9,120,500** |
| `lseg_quotes` | 5,286,169 | 0 | **5,286,169** |

---

## DELTA sequence coverage

| Target | min seq | max seq | Distinct seq values | Files |
|--------|---------|---------|---------------------|-------|
| orgs | 1 | 42 | 11 | 11 |
| assets | 1 | 96 | 80 | 100 |
| quotes | 1 | 96 | 90 | 162 |

DELTA files were applied in ascending `seq` order as enforced by `IngestPlan`. Files at the same sequence number from different feed IDs ran as separate file records.

---

## Totals across both jobs

| Metric | Value |
|--------|-------|
| Total files processed | 557 |
| Total rows parsed | 59,670,682 |
| Total rows inserted | 15,782,097 |
| Total rows skipped (RIC caret filter) | 43,888,585 |
| Total row failures | **0** |
| Total sanity failures | **0** |
| Cluster lock violations (>1 RUNNING) | **0** |

---

## Observations & notes

1. **Docker disk space** — Docker Desktop's VM virtual disk was at 100% (59 GB) midway through an earlier run attempt. The disk was expanded to 256 GB before the successful production run. For datasets of this size (~10 GB final DB footprint) ensure Docker VM disk ≥ 128 GB.
2. **Job routing** — In the multi-instance model, any node can claim any queued job regardless of which node the trigger API call was received on. Explicit `inputDir` in the trigger call ensures the correct data directory is used regardless of which instance wins the cluster lock.
3. **RIC caret filter scope** — The filter is applied to every `lseg_quotes` INT file that contains a `RIC` column. It is deliberately not applied to DELTA files (DELTA caret rows represent index-component changes that must be tracked).
4. **Job 2 throughput** — Job 2 (64 s for 93 files / 10.3 M rows) was faster than Job 1 proportionally because (a) the DB was already warm, (b) 20260426 INT files upserted over existing rows (hot InnoDB buffer pool), and (c) fewer files.
