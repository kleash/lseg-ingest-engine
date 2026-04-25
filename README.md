# 🚀 LSEG Ingest Engine

A high-performance, enterprise-grade, idempotent data ingestion engine for LSEG Reference Data.

This service is a **reference architecture** for building high-throughput database ingestion pipelines using Spring Boot and plain JDBC. It handles millions of rows from compressed source files while maintaining extreme resilience and strict operational audit trails.

---

## 🛠 Technical Architecture

The engine is designed as a phased pipeline that prioritizes consistency and throughput.

### 1. Phased Orchestration Flow
The service follows a strict execution order to ensure data integrity, especially when mixing full snapshots and incremental updates.

```text
┌────────────────────────────────────────────────────────────────┐
│ Phase A: Planning & Sanity                                     │
│ 1. Scan directory for *.txt.zip files                          │
│ 2. Classify by filename (Target & Kind)                        │
│ 3. Check Audit table to skip processed files                   │
└───────────────────────────────┬────────────────────────────────┘
                                │
                                ▼
┌────────────────────────────────────────────────────────────────┐
│ Phase B: INT Phase (Full Snapshots)                            │
│ - Processes snapshots in parallel across all target tables.    │
│ - Parallelism: configurable via 'ingest.threads.intPerTable'   │
└───────────────────────────────┬────────────────────────────────┘
                                │ (Gating: INT must complete before DELTA)
                                ▼
┌────────────────────────────────────────────────────────────────┐
│ Phase C: DELTA Phase (Incremental)                             │
│ - Processes daily changes.                                     │
│ - Strictly sequential per target to maintain change order.     │
└────────────────────────────────────────────────────────────────┘
```

### 2. File Classification & Routing
Files are automatically routed to their respective target tables based on their prefix.

| Target Table | Dataset Patterns | Kind |
| :--- | :--- | :--- |
| **`lseg_orgs`** | `Organization.*`, `EIS_DELTA_GLOABL_ORGN.*` | Org Reference |
| **`lseg_assets`** | `EIS_INT_GLOBAL_*_ASSETS.*`, `EIS_DELTA_GLOBAL_ASSETS.*` | Instrument Data |
| **`lseg_quotes`** | `EIS_INT_*_QUOTE.*`, `EIS_DELTA_*_QUOTE.*` | Quote/Venue Data |

### 3. Enterprise-Grade Resiliency (`ResilientBatchExecutor`)
Our "quarantine and continue" pattern ensures that one bad row doesn't kill a million-row ingestion.

```text
[ Batch 1: 5000 rows ] ──→ [ DB Execute ] ── OK!
[ Batch 2: 5000 rows ] ──→ [ DB Execute ] ── ERROR!
                               │
                               ▼
        ┌──────────────────────────────────────────────┐
        │        AUTO-FALLBACK TO ROW-BY-ROW           │
        ├──────────────────────────────────────────────┤
        │ Row 1: OK   │ Row 2: FAIL (Logged) │ Row 3: OK ... │
        └──────────────────────┬───────────────────────┘
                               │
                               ▼
                    [ Commit 4999 valid rows ]
```

### 4. Performance Benchmarks
| Feature | Optimization | Impact |
| :--- | :--- | :--- |
| **Streaming** | Zero-unzip `ZipInputStream` | 0% Temporary Disk Usage |
| **Batching** | `rewriteBatchedStatements=true` | ~4x Insert Throughput |
| **Idempotency** | `ON DUPLICATE KEY UPDATE` | Safe for Concurrent/Re-runs |
| **I/O** | Name-driven Header Binding | Resilient to Column Reordering |

---

## 🚦 Getting Started

### 1. Database Provisioning
We use a two-account security model: `owner` (DDL) and `ingest` (DML).
```bash
docker compose up -d mariadb
docker compose run --rm init-accounts
```

### 2. Execution
Place your LSEG `.txt.zip` files in the `./input` directory and trigger the job.
```bash
# Run the service
INGEST_DIR=./input INGEST_DATE=20260425 java -jar target/lseg-ingest-1.0.0.jar

# Trigger via API
curl -X POST http://localhost:8080/api/jobs/trigger
```

---

## 📊 Observability

Track ingestion health via the `lseg_file_audit` table:

| Column | Description |
| :--- | :--- |
| `status` | `STARTED`, `SUCCESS`, `FAILED`, or `SKIPPED_SANITY` |
| `parsed_rows` | Total rows read from the source file |
| `inserted_rows` | Rows successfully upserted into the target |
| `skipped_rows` | Rows quarantined due to errors or RIC filtering |
| `error_message` | Root cause if status is `FAILED` |

---
*Developed for high-scale financial data workloads.*
