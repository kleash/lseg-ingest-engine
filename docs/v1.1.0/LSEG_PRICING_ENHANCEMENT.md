# LSEG Pricing Enhancement — Technical Replication Guide

> **Purpose:** A self-contained reference for engineers replicating `EIS_INT_*_PRICING` ingestion
> support in any repository that already has the same `lseg-ingest` code structure. Follow
> §8 (Replication Checklist) top-to-bottom; every referenced snippet is in this document.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Pricing File Analysis](#2-pricing-file-analysis)
3. [Root-Cause Analysis — Why Existing Code Breaks](#3-root-cause-analysis--why-existing-code-breaks)
4. [Database Schema](#4-database-schema)
5. [Code Changes — Exact Before/After](#5-code-changes--exact-beforeafter)
6. [Spring Boot Event Contract](#6-spring-boot-event-contract)
7. [Priority Implementation Detail](#7-priority-implementation-detail)
8. [Step-by-Step Replication Checklist](#8-step-by-step-replication-checklist)
9. [Verification Checklist](#9-verification-checklist)

---

## 1. Architecture Overview

### Pipeline Flow

```
REST trigger → lseg_jobs (QUEUED)
    ↓
JobWorker (@Scheduled 1s poll) — claims job atomically
    ↓
IngestOrchestrator.run(jobId)
    ├── ClusterLock (MariaDB GET_LOCK — only one cluster node runs at a time)
    ├── Heartbeat thread (updates lseg_jobs.last_heartbeat_at every 30s)
    ├── FileScanner.scan()    → List<IngestFile> (classify files by Target)
    ├── Audit dedup           → skip files already in lseg_file_audit with status=SUCCESS
    ├── FileSanityCheck       → verify metadata kind + business date + key columns
    ├── IngestPlan            → group by Target and Kind (INT vs DELTA), sort DELTA by seq
    │
    ├── Phase 1 [parallel, up to deltaTargetsParallel=3 threads]
    │     ORGS / ASSETS / QUOTES / DSS_BONDS run concurrently
    │     Each: runIntPhase() [parallel files] → runDeltaPhase() [sequential by seq]
    │           → FileIngestor.ingest() → PipeFileParser/CsvFileParser → SQL upsert/delete
    │           → TargetIngestCompletedEvent published in finally block (even on failure)
    │
    ├── submitPricingAsync(): submit ~1 080 PRICING files to pricingExecutor (3 threads)
    │     returns immediately — does NOT block run()
    │
    └── run() returns true → ClusterLock released → job marked COMPLETED
          ↓ next ORGS/ASSETS/QUOTES job can start within 1 second

pricingExecutor (application-scoped, 3 threads, lives for Spring context lifetime):
    └── safeIngest(f) per file → FileIngestor → SQL upsert → archive
    └── CompletableFuture.allOf().whenComplete() fires when all files done:
          └── TargetIngestCompletedEvent(PRICING, ...) published asynchronously
              (fires AFTER the job row shows COMPLETED in lseg_jobs)
```

### Key Design Invariants

| Invariant | Where enforced |
|-----------|---------------|
| **Idempotency** | `lseg_file_audit` — re-triggers skip SUCCESS files | `IngestOrchestrator` |
| **Soft delete** | All tables have `is_deleted TINYINT(1)` — rows are never hard-deleted | `SqlBuilder.delete()` |
| **Upsert** | `INSERT … ON DUPLICATE KEY UPDATE` keyed on each table's `UNIQUE` constraint | `SqlBuilder.upsert()` |
| **Action column** | Files carry `I`/`U`/`D`. If absent (`actionIdx == -1`), all rows default to `I` (upsert) | `FileIngestor` line 196 |
| **Action ordering** | When stream flips between upsert and delete, the active buffer is flushed first | `FileIngestor` activeSide logic |
| **Cooperative stop** | `JobDao.isStopped()` polled every `cancel.checkRows=5000` rows and at file boundaries | `FileIngestor`, `IngestOrchestrator` |
| **Cluster lock** | MariaDB `GET_LOCK` — only one node processes at a time | `ClusterLock` |

### Relevant Configuration (`application.yml`)

```yaml
ingest:
  skipPatterns:
    - "*.note.txt.zip"          # ← pricing note files excluded here — no code needed
    - "Reference-INT-EQUI-*"
  threads:
    intPerTable: 10             # ← parallel file threads within a target's INT phase
    deltaTargetsParallel: 3     # ← parallel target pipelines in Phase 1
    pricing-threads: 3          # ← background executor threads for Phase 2 PRICING (NEW)
  batch:
    upsertSize: 5000
    deleteSize: 5000
  cancel:
    checkRows: 5000
```

---

## 2. Pricing File Analysis

### 2.1 File Naming Grammar

```
{DATASET}.PRC.{FEED}.{DATE}.{BATCH}.{CHUNK}.{A}.{B}.txt.zip
```

| Segment | Example | Notes |
|---------|---------|-------|
| `DATASET` | `EIS_INT_US_PRICING` | Always ends with `_PRICING` — the regex anchor used in `FileScanner` |
| kind token | `PRC` | Literal, always `PRC`. Distinguishes pricing from reference (`INT`/`REF`) files |
| `FEED` | `25DA1` (US), `25DA0` (EU), `25D9F` (ASIA) | Per-region feed identifier |
| `DATE` | `20260430` | Business date, 8 digits (yyyymmdd) |
| `BATCH` | `1`–`226` (US) | Batch number within the day; used as `seq` in `IngestFile` |
| `CHUNK` | `1`–`N` | Chunk within the batch |
| `A.B` | `1.1` | Always `1.1` in observed production data |

Full example: `EIS_INT_US_PRICING.PRC.25DA1.20260430.1.1.1.1.txt.zip`

**Note files** (`*.note.txt.zip`) follow a different pattern:
```
EIS_INT_US_PRICING.PRC.25DA1.20260430.1.1.note.txt.zip
```
They have `note` in place of `A.B` — this causes the `PRICING_PATTERN` regex to **not match** (it requires `\d+\.\d+` at that position). Additionally, `application.yml`'s `skipPatterns: ["*.note.txt.zip"]` excludes them before `classify()` is even called. Both guards are present by design.

### 2.2 File Counts (business date 20260430)

| Dataset | Data files | Note files | Feed code |
|---------|-----------|------------|-----------|
| `EIS_INT_US_PRICING` | 226 | 434 | `25DA1` |
| `EIS_INT_EU_PRICING` | ~450 | ~800 | `25DA0` |
| `EIS_INT_ASIA_PRICING` | ~404 | ~432 | `25D9F` |
| **Total data** | **~1 080** | **~1 666** | |

All three datasets share **identical** column headers and internal format. They all land in the same `lseg_pricing` table.

### 2.3 Internal File Format

Each file is a UTF-8 pipe-delimited text file compressed with zip.

**Row 1 — metadata line:**
```
EIS_INT_US_PRICING|PRC|25DA1|20260430|1|1|82010|
```

| Index | Value | Meaning |
|-------|-------|---------|
| `[0]` | `EIS_INT_US_PRICING` | Dataset name |
| `[1]` | `PRC` | Kind token — **differs from `INT`/`REF` used in all other feeds** |
| `[2]` | `25DA1` | Feed code |
| `[3]` | `20260430` | Business date |
| `[4]` | `1` | Batch number (used as `seq`) |
| `[5]` | `1` | **Chunk number** — extra field not present in INT/REF files |
| `[6]` | `82010` | **Declared row count** — shifted one position right vs INT/REF |

**Row 2 — column headers:**
```
Quote_ID|Quote_Perm_ID|Trade_Date|Alternate_Close_Price|Ask_Price|Bid_Price|Close_Auction_Price|Close_Auction_Price_Grade|Close_Price|Close_Price_Timestamp|Close_Price_Timestamp_Grade|High_Price|Low_Price|Mid_Price|Offer_Price|Open_Price|Settlement_Price|
```

**No `Action` column.** This is a deliberate difference from ORGS/ASSETS/QUOTES/DSS_BONDS which all start with `Action|`. The absence is handled transparently by `FileIngestor`: when `actionIdx == -1`, every row defaults to action `I` (upsert).

**Row 3+ — data rows:**
```
0x00100b004b250008|22166832635|20260429||0.2|0.12|||0.19||||||||0.19|
```

Observed data characteristics:
- `Quote_ID` is a hex string prefixed with `0x` (e.g., `0x00100b004b250008`)
- `Trade_Date` is `yyyymmdd` format (e.g., `20260429`) — one day prior to business date is common
- Price fields are decimals as strings (e.g., `0.19`, `1.05`). Empty string maps to `NULL` via the `STRING` binder.
- Many price fields are empty for a given row (grade columns especially)
- No trailer or footer row

### 2.4 Metadata Format Comparison

This table shows exactly why `PipeFileParser` needs changes:

| Field | INT/REF format | PRC format |
|-------|---------------|------------|
| `toks[0]` | dataset name | dataset name |
| `toks[1]` | `INT` or `REF` | **`PRC`** ← parser rejected this |
| `toks[2]` | feed code | feed code |
| `toks[3]` | date (8 digits) | date (8 digits) |
| `toks[4]` | sequence/batch | batch (used as seq) |
| `toks[5]` | **row count** | **chunk** ← parser read wrong index |
| `toks[6]` | — | **row count** ← shifted right by one |
| `len` | 6+ fields | **7+ fields** ← extra guard needed |

### 2.5 IngestPlan Routing for Pricing

`FileScanner.classify()` returns `IngestFile` with `kind = Kind.INT` for all pricing files (PRC kind maps to `Kind.INT` by convention — there are no DELTA pricing files).

`IngestPlan` therefore places all pricing files in `intByTarget[PRICING]` and `deltaByTarget[PRICING]` is empty.

Execution consequence:
- PRICING is **not** processed by `runTarget()` / `runIntPhase()`. Instead, `submitPricingAsync()` retrieves files via `plan.intFor(Target.PRICING)` and submits each one directly to the application-scoped `pricingExecutor` (3 threads).
- This keeps pricing out of `runTarget()`'s Phase 1 concurrency pool and avoids blocking `JobWorker` for the ~25-minute pricing window.
- `runDeltaPhase(PRICING, ...)` is never called — PRICING has no DELTA files.

### 2.6 Sanity Check Behaviour

`FileSanityCheck.checkWithParser()` verifies three things:
1. **Kind match** — `md.kind()` equals `INT` or `REF` per the `IngestFile.kind()`. For PRICING this fails unless exempted: `md.kind()` is `PRC` but `file.kind()` is `Kind.INT` → expected `"INT"`.
2. **Business date match** — `md.businessDate()` equals the job's configured date.
3. **Required headers present** — `Quote_ID` and `Trade_Date` must appear in the file header.

Fix: wrap the kind check in `if (file.target() != Target.PRICING)`. The business-date and header checks still apply to pricing files.

---

## 3. Root-Cause Analysis — Why Existing Code Breaks

Four independent issues in three files. Each is a targeted one-to-three line fix. No other existing code needs changes.

### Issue A — `PipeFileParser.looksLikeMetadata()`: rejects PRC kind

**File:** `src/main/java/com/lseg/ingest/io/PipeFileParser.java`, method `looksLikeMetadata()`

**Symptom:** `initialize()` never recognises the metadata row → falls through to `looksLikeHeader()` which also fails (no `Action|`) → throws `IOException("Header row not found within first 50 lines")` → `FileSanityCheck` returns `Result(false, "exception: ...")` → file is `SKIPPED_SANITY`.

**Root cause:**
```java
// BEFORE
if (!toks[1].equals(KIND_INT) && !toks[1].equals(KIND_REF)) return false;
```
`toks[1] = "PRC"` matches neither `"INT"` nor `"REF"` → method returns `false`.

---

### Issue B — `PipeFileParser.parseMetadata()`: row count at wrong index

**File:** `src/main/java/com/lseg/ingest/io/PipeFileParser.java`, method `parseMetadata()`

**Symptom:** `Metadata.declaredRows()` returns the chunk number (e.g., `1`) instead of the actual row count (e.g., `82010`). This produces wrong audit log entries (`declared=1`) but does not block ingestion.

**Root cause:**
```java
// BEFORE
int rows = safeInt(t[5]);
```
For PRC, `t[5]` is the chunk number. The row count is at `t[6]`.

---

### Issue C — `PipeFileParser.initialize()`: header detection requires `Action|` prefix

**File:** `src/main/java/com/lseg/ingest/io/PipeFileParser.java`, method `initialize()`

**Symptom:** Even after fixing Issue A (metadata recognised), `initialize()` never finds the header row because `looksLikeHeader()` requires `Action|` as the first token, and pricing headers start with `Quote_ID|`.

**Root cause:**
```java
// BEFORE
if (looksLikeHeader(trimmed)) {
    // looksLikeHeader() returns false unless line.startsWith("Action|")
```

---

### Issue D — `FileSanityCheck.checkWithParser()`: kind mismatch fails PRICING

**File:** `src/main/java/com/lseg/ingest/sanity/FileSanityCheck.java`, method `checkWithParser()`

**Symptom:** Even if the parser works, sanity check fails: `md.kind()` is `"PRC"`, `expectedKind` is `"INT"` → `Result(false, "metadata kind=PRC expected=INT")` → file is `SKIPPED_SANITY`.

**Root cause:**
```java
// BEFORE
String expectedKind = (file.kind() == Kind.INT) ? KIND_INT : KIND_REF;
if (!expectedKind.equals(md.kind()))
    return new Result(false, "metadata kind=" + md.kind() + " expected=" + expectedKind, ...);
```
No exception for the PRICING target.

---

## 4. Database Schema

**File:** `src/main/resources/db/changelog/sql/007-lseg_pricing.sql`

```sql
-- liquibase formatted sql

-- changeset lseg-ingest:007-create-lseg_pricing
CREATE TABLE IF NOT EXISTS lseg_pricing (
    id                          BIGINT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    quote_id                    VARCHAR(255),
    quote_perm_id               VARCHAR(255),
    trade_date                  VARCHAR(20),
    alternate_close_price       VARCHAR(255),
    ask_price                   VARCHAR(255),
    bid_price                   VARCHAR(255),
    close_auction_price         VARCHAR(255),
    close_auction_price_grade   VARCHAR(255),
    close_price                 VARCHAR(255),
    close_price_timestamp       VARCHAR(255),
    close_price_timestamp_grade VARCHAR(255),
    high_price                  VARCHAR(255),
    low_price                   VARCHAR(255),
    mid_price                   VARCHAR(255),
    offer_price                 VARCHAR(255),
    open_price                  VARCHAR(255),
    settlement_price            VARCHAR(255),
    is_deleted                  TINYINT(1) NOT NULL DEFAULT 0,
    UNIQUE KEY uniq_pricing_quote_id (quote_id),
    KEY idx_pricing_trade_date    (trade_date),
    KEY idx_pricing_quote_perm_id (quote_perm_id),
    KEY idx_pricing_is_deleted    (is_deleted)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- rollback DROP TABLE lseg_pricing;
```

**Column type rationale:**
- All price values are `VARCHAR(255)` — consistent with the entire codebase (ORGS, ASSETS, QUOTES, DSS_BONDS all use `VARCHAR`). Consumers do `CAST(close_price AS DECIMAL(18,6))` for arithmetic.
- `trade_date` is `VARCHAR(20)` — slightly narrower because dates are fixed at 8 chars (`yyyymmdd`), but VARCHAR keeps the convention consistent.
- Empty strings in source data become `NULL` via the `STRING` `ValueBinder` in `TargetSchema`.

**Unique key rationale:** `quote_id` — the quoted instrument is the unique entity for pricing. To handle multiple records for the same `quote_id` (either in the same file or across days), the `SqlBuilder` uses conditional `IF(VALUES(trade_date) >= trade_date, ...)` logic to ensure only the record with the most recent `trade_date` is retained in the database.

**Indexes:**
- `idx_pricing_trade_date` — range queries by date
- `idx_pricing_quote_perm_id` — joins to ORGS/ASSETS/QUOTES on `quote_perm_id`
- `idx_pricing_is_deleted` — fast filtered counts

**Register in changelog master** (`src/main/resources/db/changelog/db.changelog-master.xml`):
```xml
<!-- Add after the 006 include line -->
<include file="db/changelog/sql/007-lseg_pricing.sql"/>
```

---

## 5. Code Changes — Exact Before/After

Changes in implementation order. Make each change, then run `mvn test` before proceeding.

### 5.1 `Constants.java` — add `KIND_PRC`

**File:** `src/main/java/com/lseg/ingest/Constants.java`

```java
// BEFORE (line 27):
    public static final String KIND_REF = "REF";

// AFTER:
    public static final String KIND_REF = "REF";
    public static final String KIND_PRC = "PRC";
```

Used in: `PipeFileParser.looksLikeMetadata()`, `PipeFileParser.parseMetadata()`, `FileSanityCheck` (via import).

---

### 5.2 `Target.java` — add PRICING enum entry

**File:** `src/main/java/com/lseg/ingest/plan/Target.java`

The last existing entry is `DSS_BONDS`. Change its trailing `;` to `,` and append `PRICING`:

```java
// BEFORE:
    DSS_BONDS("lseg_dss_bonds",
            List.of("isin", "instrument_id", "instrument_id_type", "ric"),
            List.of("ISIN", "Instrument ID", "Instrument ID Type", "RIC"));

// AFTER:
    DSS_BONDS("lseg_dss_bonds",
            List.of("isin", "instrument_id", "instrument_id_type", "ric"),
            List.of("ISIN", "Instrument ID", "Instrument ID Type", "RIC")),
    PRICING("lseg_pricing",
            List.of("quote_id"),
            List.of("Quote_ID"));
```

`uniqueKeyColumns` = DB column names (`quote_id`)
`uniqueKeySourceHeaders` = file header names (`Quote_ID`) — used by `FileSanityCheck` to verify required columns are present. `Trade_Date` is no longer part of the unique key but is used in conditional upsert logic in `SqlBuilder`.

---

### 5.3 `TargetSchema.java` — add 17-column PRICING schema

**File:** `src/main/java/com/lseg/ingest/load/TargetSchema.java`

Add inside the `static {}` block, before the `SCHEMAS.put(Target.DSS_BONDS, …)` entry:

```java
        SCHEMAS.put(Target.PRICING, List.of(
                col("Quote_ID",                    "quote_id",                    Types.VARCHAR, STRING),
                col("Quote_Perm_ID",               "quote_perm_id",               Types.VARCHAR, STRING),
                col("Trade_Date",                  "trade_date",                  Types.VARCHAR, STRING),
                col("Alternate_Close_Price",       "alternate_close_price",       Types.VARCHAR, STRING),
                col("Ask_Price",                   "ask_price",                   Types.VARCHAR, STRING),
                col("Bid_Price",                   "bid_price",                   Types.VARCHAR, STRING),
                col("Close_Auction_Price",         "close_auction_price",         Types.VARCHAR, STRING),
                col("Close_Auction_Price_Grade",   "close_auction_price_grade",   Types.VARCHAR, STRING),
                col("Close_Price",                 "close_price",                 Types.VARCHAR, STRING),
                col("Close_Price_Timestamp",       "close_price_timestamp",       Types.VARCHAR, STRING),
                col("Close_Price_Timestamp_Grade", "close_price_timestamp_grade", Types.VARCHAR, STRING),
                col("High_Price",                  "high_price",                  Types.VARCHAR, STRING),
                col("Low_Price",                   "low_price",                   Types.VARCHAR, STRING),
                col("Mid_Price",                   "mid_price",                   Types.VARCHAR, STRING),
                col("Offer_Price",                 "offer_price",                 Types.VARCHAR, STRING),
                col("Open_Price",                  "open_price",                  Types.VARCHAR, STRING),
                col("Settlement_Price",            "settlement_price",            Types.VARCHAR, STRING)));
```

Column order matches the exact file header order. `TargetSchema.intersect()` is position-independent (name-driven), so order only affects `schemaSummary()` logging.

---

### 5.4 `PipeFileParser.java` — three fixes (Issues A, B, C)

**File:** `src/main/java/com/lseg/ingest/io/PipeFileParser.java`

#### Fix A — `looksLikeMetadata()`: accept PRC kind and enforce minimum 7 fields

```java
// BEFORE:
    private static boolean looksLikeMetadata(String line) {
        // Rough check: 6 pipe-separated fields, second is INT or REF, fourth looks like date.
        String[] toks = line.split("\\|", -1);
        if (toks.length < 6) return false;
        if (!toks[1].equals(KIND_INT) && !toks[1].equals(KIND_REF)) return false;
        return toks[3].matches("\\d{8}");
    }

// AFTER:
    private static boolean looksLikeMetadata(String line) {
        // Rough check: second field is INT/REF/PRC, fourth looks like a date.
        String[] toks = line.split("\\|", -1);
        if (toks.length < 6) return false;
        if (!toks[1].equals(KIND_INT) && !toks[1].equals(KIND_REF) && !toks[1].equals(KIND_PRC)) return false;
        // PRC format has an extra chunk field: needs at least 7 fields to hold row count at index 6
        if (KIND_PRC.equals(toks[1]) && toks.length < 7) return false;
        return toks[3].matches("\\d{8}");
    }
```

The `toks.length < 7` guard prevents accepting a malformed PRC line (only 6 fields) as valid metadata. Such a line falls through to `looksLikeHeader()` — if it starts with `Action|`, it's treated as a header-only file (no metadata parsed). This is the correct fallback behaviour, verified by `malformedPrcMetadataMissingRowCountField` test.

#### Fix B — `parseMetadata()`: read row count from index 6 for PRC, with bounds guard

```java
// BEFORE:
    private static Metadata parseMetadata(String line) {
        String[] t = line.split("\\|", -1);
        int seq = safeInt(t[4]);
        int rows = safeInt(t[5]);
        return new Metadata(t[0], t[1], t[2], t[3], seq, rows);
    }

// AFTER:
    private static Metadata parseMetadata(String line) {
        String[] t = line.split("\\|", -1);
        int seq = safeInt(t[4]);
        // PRC format: dataset|PRC|feed|date|batch|chunk|rows| — rows at index 6
        // INT/REF format: dataset|INT|feed|date|seq|rows|    — rows at index 5
        int rows = KIND_PRC.equals(t[1]) ? (t.length > 6 ? safeInt(t[6]) : -1) : safeInt(t[5]);
        return new Metadata(t[0], t[1], t[2], t[3], seq, rows);
    }
```

The `t.length > 6` guard is defensive: `looksLikeMetadata()` already enforces `toks.length >= 7` for PRC, so this guard will never trigger for valid input. It protects against any caller who invokes `parseMetadata()` directly on a short line.

#### Fix C — `initialize()`: accept pipe-containing lines as header after metadata is found

```java
// BEFORE (the header-detection condition):
            if (looksLikeHeader(trimmed)) {

// AFTER:
            // Standard files: header starts with "Action|".
            // Pricing files (PRC kind): no Action column — accept any pipe-containing line once
            // metadata is known. Junk lines between metadata and header have no pipes, so they
            // are naturally skipped without the old Action-prefix guard.
            if (looksLikeHeader(trimmed) || (metadata != null && trimmed.contains("|"))) {
```

**Why this is backward-safe:** Existing INT/REF files have their metadata on row 1. Row 2 is the `Action|…` header. After the metadata is parsed, `metadata != null` becomes true. The next non-empty line is the `Action|…` header which contains `|`, so `metadata != null && trimmed.contains("|")` fires. The result is identical to the old `looksLikeHeader()` path.

**Why junk lines between metadata and header are safe:** The `toleratesExtraLeadingLines` test has `some other junk` (no pipes) between the metadata row and the `Action|` header. The condition `trimmed.contains("|")` is `false` for that line, so it is skipped. The correct header is found on the next non-empty pipe-containing line.

The full `initialize()` loop (showing context):

```java
    @Override
    public void initialize(int maxLookahead) throws IOException {
        for (int i = 0; i < maxLookahead; i++) {
            String line = reader.readLine();
            if (line == null) break;
            lineNumber++;
            String trimmed = line.trim();
            if (trimmed.isEmpty()) continue;

            if (metadata == null && looksLikeMetadata(trimmed)) {
                metadata = parseMetadata(trimmed);
                continue;
            }
            if (looksLikeHeader(trimmed) || (metadata != null && trimmed.contains("|"))) {
                headerColumns = splitTokens(trimmed);
                headerIndex = new HashMap<>(headerColumns.size() * 2);
                java.util.List<String> dups = new java.util.ArrayList<>();
                for (int j = 0; j < headerColumns.size(); j++) {
                    String h = headerColumns.get(j);
                    if (h == null) continue;
                    if (headerIndex.putIfAbsent(h, j) != null) dups.add(h);
                }
                if (!dups.isEmpty()) {
                    log.warn("Duplicate header column(s) ignored (kept first occurrence): {}", dups);
                }
                ready = true;
                return;
            }
        }
        throw new IOException("Header row not found within first " + maxLookahead + " lines");
    }
```

---

### 5.5 `FileScanner.java` — PRICING_PATTERN + classify() branch + mapTarget() rule

**File:** `src/main/java/com/lseg/ingest/plan/FileScanner.java`

#### 5.5a Add `PRICING_PATTERN` constant (after `BONDS_CSV_PATTERN`)

```java
// ADD after BONDS_CSV_PATTERN (after line 31):
    // Covers all three regions: EIS_INT_US_PRICING, EIS_INT_EU_PRICING, EIS_INT_ASIA_PRICING
    // Format: {dataset}.PRC.{feed}.{date}.{batch}.{chunk}.{a}.{b}.txt.zip
    // Note files (*.note.txt.zip) do NOT match: "note" != \d+
    private static final Pattern PRICING_PATTERN = Pattern.compile(
            "^(?<dataset>[A-Za-z0-9_\\-]+_PRICING)\\.PRC\\.(?<feed>[A-Za-z0-9]+)\\.(?<date>\\d{8})\\.(?<seq>\\d+)\\.\\d+\\.\\d+\\.\\d+\\.txt\\.zip$");
```

The dataset group `[A-Za-z0-9_\-]+_PRICING` matches all three regions because every pricing dataset ends with `_PRICING`. The regex requires **four** `\d+` segments after the date (`batch.chunk.a.b`), so note files (`batch.chunk.note.txt.zip`) naturally fail the match.

#### 5.5b Add pricing branch in `classify()` (after bonds block, before `return null`)

```java
// ADD after the bonds block (after the BONDS_CSV_PATTERN matcher):
        Matcher mp = PRICING_PATTERN.matcher(name);
        if (mp.matches()) {
            String dataset = mp.group("dataset");
            int seq = Integer.parseInt(mp.group("seq"));
            return new IngestFile(path, name, dataset, Target.PRICING, Kind.INT, seq);
        }
```

`Kind.INT` is always used for pricing files — there are no DELTA pricing files. `seq` maps to the batch number, which determines parallel processing order within `runIntPhase()`.

#### 5.5c Add PRICING rule in `mapTarget()` (after QUOTES block)

```java
// ADD after the QUOTES block:
        // PRICING — covers EIS_INT_US_PRICING, EIS_INT_EU_PRICING, EIS_INT_ASIA_PRICING.
        // NOTE: pricing files are routed via PRICING_PATTERN in classify() before mapTarget() is
        // called, so this rule acts as a secondary fallback for direct mapTarget() callers only.
        if (dataset.endsWith("_PRICING")) return Target.PRICING;
```

---

### 5.6 `FileSanityCheck.java` — skip kind check for PRICING (Issue D)

**File:** `src/main/java/com/lseg/ingest/sanity/FileSanityCheck.java`

First, add the `Target` import:
```java
import com.lseg.ingest.plan.Target;
```

Then wrap the kind check:

```java
// BEFORE:
            String expectedKind = (file.kind() == Kind.INT) ? KIND_INT : KIND_REF;
            if (!expectedKind.equals(md.kind()))
                return new Result(false, "metadata kind=" + md.kind() + " expected=" + expectedKind, md, p.headerColumns());

// AFTER:
            // PRICING files carry "PRC" in their metadata but are treated as Kind.INT in the pipeline
            if (file.target() != Target.PRICING) {
                String expectedKind = (file.kind() == Kind.INT) ? KIND_INT : KIND_REF;
                if (!expectedKind.equals(md.kind()))
                    return new Result(false, "metadata kind=" + md.kind() + " expected=" + expectedKind, md, p.headerColumns());
            }
```

Only PRICING is exempted. All other targets still have their kind validated. The business-date check and required-headers check both still run for PRICING.

---

### 5.7 `TargetIngestCompletedEvent.java` — new file

**File (NEW):** `src/main/java/com/lseg/ingest/event/TargetIngestCompletedEvent.java`

Create the `event` package directory if it does not already exist.

```java
package com.lseg.ingest.event;

import com.lseg.ingest.plan.Target;

/**
 * Published by IngestOrchestrator after all files for a Target finish processing
 * (INT phase + DELTA phase). Fired in the finally block so it always fires,
 * even on failure. Listeners use {@code @EventListener}.
 *
 * <pre>
 * {@code
 * @EventListener
 * public void onTargetComplete(TargetIngestCompletedEvent e) {
 *     if (e.target() == Target.PRICING && e.success()) { ... }
 * }
 * }
 * </pre>
 *
 * fileCount is 0 when no files were present for the target on this business date.
 */
public record TargetIngestCompletedEvent(
        Target target,
        String businessDate,
        long jobId,
        boolean success,
        int fileCount
) {}
```

**Important:** This record has **no `source` field**. Spring's `ApplicationEventPublisher` does not require the `ApplicationEvent` base class or a `source` parameter for plain records/POJOs. Adding `Object source` would be unnecessary noise.

---

### 5.8 `IngestOrchestrator.java` — event publisher + Phase 1/2 split

**File:** `src/main/java/com/lseg/ingest/orchestrator/IngestOrchestrator.java`

#### 5.8a Add import

```java
import com.lseg.ingest.event.TargetIngestCompletedEvent;
import org.springframework.context.ApplicationEventPublisher;
```

#### 5.8b Add field and constructor parameter

```java
// ADD field (with other private final fields):
    private final ApplicationEventPublisher eventPublisher;

// ADD to constructor signature (last parameter):
                              ApplicationEventPublisher eventPublisher) {

// ADD to constructor body (last line):
        this.eventPublisher = eventPublisher;
```

Spring autowires `ApplicationEventPublisher` automatically — no `@Bean` declaration needed.

#### 5.8c Replace single target loop with Phase 1 + async Phase 2

Find the section in `run()` that looks like:

```java
// BEFORE:
            ExecutorService perTargetPool = Executors.newFixedThreadPool(...);

            List<Future<?>> targetFutures = new ArrayList<>();
            for (Target t : Target.values()) {
                targetFutures.add(perTargetPool.submit(() -> runTarget(t, plan, jobId, businessDate)));
            }

            boolean anyTargetError = false;
            for (Future<?> f : targetFutures) {
                try {
                    waitWithHeartbeat(f, jobId, "target pipeline");
                } catch (ExecutionException e) {
                    anyTargetError = true;
                    log.error("Target pipeline failed unexpectedly", e.getCause());
                    registry.counter(METRIC_TARGET_ERRORS).increment();
                }
            }

            perTargetPool.shutdown();
            if (!perTargetPool.awaitTermination(2, TimeUnit.MINUTES)) {
                perTargetPool.shutdownNow();
            }
```

Replace with:

```java
// AFTER:
            ExecutorService perTargetPool = Executors.newFixedThreadPool(
                    Math.max(1, props.getThreads().getDeltaTargetsParallel()),
                    daemonThreads("target"));

            // Phase 1: ORGS, ASSETS, QUOTES, DSS_BONDS — run in parallel (unchanged behaviour)
            List<Future<?>> phase1Futures = new ArrayList<>();
            for (Target t : Target.values()) {
                if (t == Target.PRICING) continue;
                phase1Futures.add(perTargetPool.submit(() -> runTarget(t, plan, jobId, businessDate)));
            }

            boolean anyTargetError = false;
            for (Future<?> f : phase1Futures) {
                try {
                    waitWithHeartbeat(f, jobId, "target pipeline");
                } catch (ExecutionException e) {
                    anyTargetError = true;
                    log.error("Target pipeline failed unexpectedly", e.getCause());
                    registry.counter(METRIC_TARGET_ERRORS).increment();
                }
            }

            // Phase 2: PRICING submitted to dedicated background executor.
            // run() returns immediately after this call — the cluster lock is released before
            // any pricing file starts processing, so the next ORGS/ASSETS/QUOTES job can start
            // within 1 second.
            if (!jobDao.isStopped(jobId)) {
                submitPricingAsync(plan, jobId, businessDate);
            } else {
                eventPublisher.publishEvent(
                        new TargetIngestCompletedEvent(Target.PRICING, businessDate, jobId, false, 0));
            }

            perTargetPool.shutdown();
            if (!perTargetPool.awaitTermination(2, TimeUnit.MINUTES)) {
                perTargetPool.shutdownNow();
            }
```

#### 5.8e Add `pricingExecutor` field, constructor init, `@PreDestroy`, and `submitPricingAsync()`

**Add imports:**
```java
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import jakarta.annotation.PreDestroy;
```

**Add field** (alongside other `private final` fields):
```java
    private final ExecutorService pricingExecutor;
```

**Add to constructor** (last line of body, after `this.eventPublisher = eventPublisher`):
```java
        this.pricingExecutor = Executors.newFixedThreadPool(
                Math.max(1, props.getThreads().getPricingThreads()),
                daemonThreads("pricing"));
```

**Add `@PreDestroy` method** (after the constructor):
```java
    @PreDestroy
    public void shutdownPricingExecutor() {
        pricingExecutor.shutdown();
        try {
            if (!pricingExecutor.awaitTermination(5, TimeUnit.MINUTES)) {
                pricingExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            pricingExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
```

**Add `submitPricingAsync()` method** (alongside `runTarget()`, `runIntPhase()` etc.):
```java
    private void submitPricingAsync(IngestPlan plan, long jobId, String businessDate) {
        List<IngestFile> pricingFiles = plan.intFor(Target.PRICING);
        int totalFiles = pricingFiles.size();

        if (pricingFiles.isEmpty()) {
            log.info("No PRICING files to process; firing event immediately");
            eventPublisher.publishEvent(
                    new TargetIngestCompletedEvent(Target.PRICING, businessDate, jobId, true, 0));
            return;
        }

        log.info("Submitting {} PRICING files to background executor (threads={})",
                totalFiles, props.getThreads().getPricingThreads());

        AtomicBoolean anyFailed = new AtomicBoolean(false);
        List<CompletableFuture<Void>> futures = new ArrayList<>(totalFiles);
        for (IngestFile f : pricingFiles) {
            futures.add(CompletableFuture.runAsync(() -> {
                MDC.put(MDC_JOB_ID, String.valueOf(jobId));
                try {
                    safeIngest(f, jobId, businessDate);
                } catch (Exception e) {
                    anyFailed.set(true);
                    log.error("Background PRICING ingest failed for {}", f.fileName(), e);
                } finally {
                    MDC.remove(MDC_JOB_ID);
                }
            }, pricingExecutor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        log.error("Background PRICING executor error", ex);
                    }
                    boolean success = !anyFailed.get() && ex == null;
                    log.info("Background PRICING complete: files={} success={}", totalFiles, success);
                    eventPublisher.publishEvent(
                            new TargetIngestCompletedEvent(Target.PRICING, businessDate, jobId, success, totalFiles));
                });
    }
```

`CompletableFuture.allOf().whenComplete()` fires from whichever `pricingExecutor` thread completes the last future. All PRICING `@EventListener` handlers therefore run on a `pricingExecutor` thread — see §6 listener-thread warning.

---

### 5.9 `SqlBuilder.java` — conditional PRICING upsert

**File:** `src/main/java/com/lseg/ingest/load/SqlBuilder.java`

To ensure we only keep the record with the most recent `trade_date`, the `upsert` method is modified to wrap assignments in `IF(VALUES(trade_date) >= trade_date, ...)` for the `PRICING` target.

```java
    public static String upsert(Target target, List<TargetSchema.Column> cols) {
        Set<String> keyCols = new HashSet<>(target.uniqueKeyColumns);
        boolean isPricing = target == Target.PRICING && cols.stream().anyMatch(c -> "trade_date".equals(c.dbColumn()));

        String colList = cols.stream().map(TargetSchema.Column::dbColumn).collect(Collectors.joining(", "));
        String params = cols.stream().map(c -> "?").collect(Collectors.joining(", "));

        String onDup;
        if (isPricing) {
            List<String> updates = cols.stream()
                    .filter(c -> !keyCols.contains(c.dbColumn()) && !"trade_date".equals(c.dbColumn()))
                    .map(c -> c.dbColumn() + " = IF(VALUES(trade_date) >= trade_date, VALUES(" + c.dbColumn() + "), " + c.dbColumn() + ")")
                    .collect(Collectors.toCollection(ArrayList::new));

            updates.add("is_deleted = IF(VALUES(trade_date) >= trade_date, 0, is_deleted)");
            updates.add("trade_date = IF(VALUES(trade_date) >= trade_date, VALUES(trade_date), trade_date)");
            onDup = String.join(", ", updates);
        } else {
            // ... standard logic ...
        }
```

This logic guarantees that even if pricing files for different dates are processed in any order, the database always contains the "newest" data for each `quote_id`.

---

#### 5.8d Modify `runTarget()` — track success/fileCount and publish event

```java
// BEFORE:
    private void runTarget(Target t, IngestPlan plan, long jobId, String businessDate) {
        log.info("Starting target pipeline for {}", t);
        try {
            checkStop(jobId);
            runIntPhase(t, plan.intFor(t), jobId, businessDate);
            checkStop(jobId);
            runDeltaPhase(t, plan.deltaFor(t), jobId, businessDate);
        } catch (Exception e) {
            log.error("Pipeline for {} aborted with error", t, e);
            throw new RuntimeException(e);
        } finally {
            log.info("Target pipeline for {} finished", t);
        }
    }

// AFTER:
    private void runTarget(Target t, IngestPlan plan, long jobId, String businessDate) {
        log.info("Starting target pipeline for {}", t);
        boolean success = false;
        int fileCount = plan.intFor(t).size() + plan.deltaFor(t).size();
        try {
            checkStop(jobId);
            runIntPhase(t, plan.intFor(t), jobId, businessDate);
            checkStop(jobId);
            runDeltaPhase(t, plan.deltaFor(t), jobId, businessDate);
            success = true;
        } catch (Exception e) {
            log.error("Pipeline for {} aborted with error", t, e);
            throw new RuntimeException(e);
        } finally {
            log.info("Target pipeline for {} finished (success={}, files={})", t, success, fileCount);
            eventPublisher.publishEvent(
                    new TargetIngestCompletedEvent(t, businessDate, jobId, success, fileCount));
        }
    }
```

The `finally` block guarantees the event fires even on failure, even on stop signal. `success=false` when any file in the target failed.

---

## 6. Spring Boot Event Contract

### Event record

```java
public record TargetIngestCompletedEvent(
        Target target,       // which target: ORGS, ASSETS, QUOTES, DSS_BONDS, or PRICING
        String businessDate, // e.g. "20260430"
        long jobId,          // lseg_jobs.id
        boolean success,     // false if any file in this target failed or stop was signalled
        int fileCount        // number of files processed; 0 if no files existed for this date
) {}
```

### Example listener

```java
@Component
public class PricingReadyListener {

    @EventListener
    public void onTargetComplete(TargetIngestCompletedEvent event) {
        if (event.target() == Target.PRICING && event.success() && event.fileCount() > 0) {
            log.info("Pricing ingestion complete: {} files for {}",
                    event.fileCount(), event.businessDate());
            // kick off downstream analytics, notifications, etc.
        }
    }
}
```

### Firing order per job

Events 1–4 fire synchronously in the `runTarget()` finally block (Phase 1 threads). Their relative order is non-deterministic (parallel). Event 5 fires asynchronously from a `pricingExecutor` thread — **after the job row shows COMPLETED** in `lseg_jobs`.

1. `TargetIngestCompletedEvent(ORGS, …)`
2. `TargetIngestCompletedEvent(ASSETS, …)`
3. `TargetIngestCompletedEvent(QUOTES, …)`
4. `TargetIngestCompletedEvent(DSS_BONDS, …)`
5. `TargetIngestCompletedEvent(PRICING, …)` — **always last, fires asynchronously ~25 min after job COMPLETED**

Events 1–4 are published **synchronously** in `runTarget()` finally blocks (Spring's default `SimpleApplicationEventMulticaster`). Event 5 is published from a `pricingExecutor` background thread.

**Important for PRICING event listeners:** Because event 5 is dispatched synchronously from a `pricingExecutor` thread, any `@EventListener` that handles `Target.PRICING` events will execute on that thread — blocking one of the 3 pricing threads until the listener returns. Keep PRICING listeners fast (logging, flag-setting) or add `@Async` to the listener method and `@EnableAsync` to a configuration class to dispatch off the executor thread.

### `success` flag semantics

| Scenario | `success` |
|----------|-----------|
| All files ingested without error | `true` |
| Any file threw an exception | `false` |
| Stop signal received mid-target | `false` (exception thrown from `checkStop`) |
| No files for this target today | `true` (empty phases complete without error; `fileCount=0`) |

---

## 7. Priority and Async Execution Detail

### Why PRICING starts after Phase 1

Pricing data depends on reference data (ORGS, ASSETS, QUOTES) being up to date first. `submitPricingAsync()` is only called after all Phase 1 futures resolve, guaranteeing the reference tables are fully populated before any pricing row is written.

### Why PRICING runs in a background executor (not blocking)

`JobWorker.pollAndExecute()` is `@Scheduled(fixedDelay=1000)` — a single thread that blocks until `orchestrator.run()` returns. If PRICING ran synchronously (~25 min), the next ORGS/ASSETS/QUOTES job would wait in QUEUED for that entire window. The background executor releases the scheduling thread immediately after Phase 1, so the next job starts within seconds.

### How Phase 1 / Phase 2 execution works

```
JobWorker thread (blocks until run() returns):
  Phase 1 pool: up to deltaTargetsParallel=3 threads
    ├── Thread A: runTarget(ORGS, ...)
    ├── Thread B: runTarget(ASSETS, ...)
    └── Thread C: runTarget(QUOTES, ...) + DSS_BONDS queued

  All Phase 1 futures resolved → cluster lock released on run() return
  submitPricingAsync() called → futures submitted to pricingExecutor, returns immediately

  run() returns true → JobWorker marks COMPLETED → can claim next job within 1 second

pricingExecutor (3 threads, application-scoped):
  Up to 3 pricing files processed in parallel
  CompletableFuture.allOf().whenComplete() → fires TargetIngestCompletedEvent(PRICING)
```

### Event timing for PRICING

`TargetIngestCompletedEvent(PRICING)` fires **after** the job row shows `COMPLETED` in `lseg_jobs`. This is intentional and expected. Listeners must not assume the event fires before job completion.

### Error handling

| Scenario | Behaviour |
|----------|-----------|
| Phase 1 target fails | PRICING still submitted (pricing is independent of reference errors) |
| Stop signal during Phase 1 | PRICING skipped; event fires with `success=false, fileCount=0` |
| Stop signal after pricing submitted | Pricing continues — job is already COMPLETED, stop signal no longer applies |
| A pricing file fails in background | `anyFailed` flag set; event fires with `success=false` once all files complete |
| App crash mid-PRICING | Files with `STARTED` status in `lseg_file_audit` are re-attempted on next trigger |

### `pricingExecutor` lifecycle

- Created in `IngestOrchestrator` constructor with `Executors.newFixedThreadPool(pricingThreads)`.
- Lives for the Spring context lifetime (shared across all job runs).
- `@PreDestroy shutdownPricingExecutor()` awaits up to 5 minutes for in-flight tasks on app shutdown.
- Configured via `ingest.threads.pricing-threads` (default: 3).

---

## 8. Step-by-Step Replication Checklist

Work through these steps in order. Each step is independently testable.

- [ ] **Step 1** — Add `KIND_PRC = "PRC"` to `Constants.java` (§5.1)
- [ ] **Step 2** — Add `PRICING` enum entry to `Target.java` with `uniqueKeyColumns=(quote_id)` (§5.2)
- [ ] **Step 3** — Add 17-column `SCHEMAS.put(Target.PRICING, ...)` block to `TargetSchema.java` `static {}` initializer (§5.3)
- [ ] **Step 4** — Fix `PipeFileParser.looksLikeMetadata()`: add `KIND_PRC` check + `toks.length < 7` PRC guard (§5.4 Fix A)
- [ ] **Step 5** — Fix `PipeFileParser.parseMetadata()`: read row count from `t[6]` for PRC with bounds guard (§5.4 Fix B)
- [ ] **Step 6** — Fix `PipeFileParser.initialize()`: change `if (looksLikeHeader(trimmed))` to `if (looksLikeHeader(trimmed) || (metadata != null && trimmed.contains("|")))` (§5.4 Fix C)
- [ ] **Step 7** — Fix `FileSanityCheck.checkWithParser()`: add `Target` import and wrap kind check in `if (file.target() != Target.PRICING)` (§5.6)
- [ ] **Step 8** — Add `PRICING_PATTERN` constant to `FileScanner.java` (§5.5a)
- [ ] **Step 9** — Add `PRICING_PATTERN` matcher branch in `FileScanner.classify()` after the bonds block (§5.5b)
- [ ] **Step 10** — Add `dataset.endsWith("_PRICING") → Target.PRICING` to `FileScanner.mapTarget()` (§5.5c)
- [ ] **Step 11** — Create `src/main/java/com/lseg/ingest/event/TargetIngestCompletedEvent.java` (§5.7)
- [ ] **Step 12** — Add `ApplicationEventPublisher` field and constructor injection to `IngestOrchestrator` (§5.8b)
- [ ] **Step 13** — Import `TargetIngestCompletedEvent` and `ApplicationEventPublisher` in `IngestOrchestrator` (§5.8a)
- [ ] **Step 14** — Split the single target loop in `IngestOrchestrator.run()` into Phase 1 + Phase 2 (§5.8c)
- [ ] **Step 15** — Modify `runTarget()` to track `success`/`fileCount` and publish event in `finally` block (§5.8d)
- [ ] **Step 16** — Create `src/main/resources/db/changelog/sql/007-lseg_pricing.sql` (§4)
- [ ] **Step 17** — Add `<include file="db/changelog/sql/007-lseg_pricing.sql"/>` to `db.changelog-master.xml` (§4)
- [ ] **Step 18** — Verify `application.yml` has `skipPatterns: ["*.note.txt.zip"]` — this must be present in the target repo or note files will be ingested
- [ ] **Step 19** — Add `pricingThreads = 3` + getter/setter to `IngestProperties.Threads` inner class (§5.8e)
- [ ] **Step 20** — Add `pricing-threads: 3` under `ingest.threads` in `application.yml` (§1 config block)
- [ ] **Step 21** — Add `pricingExecutor` field to `IngestOrchestrator`, initialize in constructor with `Executors.newFixedThreadPool(Math.max(1, props.getThreads().getPricingThreads()), daemonThreads("pricing"))` (§5.8e)
- [ ] **Step 22** — Add `@PreDestroy shutdownPricingExecutor()` to `IngestOrchestrator` (§5.8e)
- [ ] **Step 23** — Replace the single target loop in `run()` with Phase 1 loop + async Phase 2 `submitPricingAsync()` call (§5.8c)
- [ ] **Step 24** — Add `submitPricingAsync()` method with `CompletableFuture.allOf().whenComplete()` — full code in §5.8e
- [ ] **Step 25** — Modify `SqlBuilder.upsert()` to add conditional `IF` logic for `Target.PRICING` (§5.9)
- [ ] **Step 26** — Run `mvn test` — all 36 tests must pass, including 9 pricing-specific tests
- [ ] **Step 27** — Start app, run Liquibase migration, trigger a job, verify §9 checklist

### New tests added (for reference)

`FileScannerTest`:
- `classifiesUsPricingFile` — `EIS_INT_US_PRICING.PRC.25DA1.20260430.1.1.1.1.txt.zip` → `Target.PRICING, Kind.INT, seq=1`
- `classifiesEuPricingFile` — `EIS_INT_EU_PRICING.PRC.25DA0.20260430.10.16.1.1.txt.zip` → `seq=10`
- `classifiesAsiaPricingFile` — `EIS_INT_ASIA_PRICING.PRC.25D9F.20260430.96.120.1.1.txt.zip` → `seq=96`
- `rejectsPricingNoteFile` — `EIS_INT_US_PRICING.PRC.25DA1.20260430.1.1.note.txt.zip` → `null`

`PipeFileParserTest`:
- `parsesPrcMetadataAndHeaderWithNoActionColumn` — metadata parsed, `Quote_ID` at index 0, data rows readable
- `prcMetadataReadsRowCountFromIndex6` — confirms `declaredRows=12345` from `t[6]`, not `t[5]`
- `prcJunkBetweenMetadataAndHeaderIsSkipped` — junk line (no pipes) between metadata and header is skipped
- `malformedPrcMetadataMissingRowCountField` — PRC with only 6 fields → metadata not parsed, header found via `Action|` fallback

---

## 9. Verification Checklist

### Build

```bash
mvn clean package -DskipTests   # must compile without errors
mvn test                         # 35 tests must pass
```

### Start application

```bash
mvn spring-boot:run
```

On first start Liquibase runs `007-create-lseg_pricing`. Verify:
```sql
SHOW TABLES LIKE 'lseg_pricing';
SHOW CREATE TABLE lseg_pricing;    -- verify UNIQUE KEY on (quote_id)
```

### Trigger ingestion

```bash
curl -X POST "http://localhost:8080/api/jobs/trigger?businessDate=20260430&inputDir=/path/to/20260430"
```

### Verify priority ordering in logs

The following sequence must appear. `Submitting … PRICING files` must appear only after all four Phase 1 "finished" lines. `Ingestion session finished` (job COMPLETED, lock released) must appear before `Background PRICING complete`.

```
Starting target pipeline for ORGS
Starting target pipeline for ASSETS
Starting target pipeline for QUOTES
Starting target pipeline for DSS_BONDS
...
Target pipeline for ORGS finished (success=true, files=N)
Target pipeline for ASSETS finished (success=true, files=N)
Target pipeline for QUOTES finished (success=true, files=N)
Target pipeline for DSS_BONDS finished (success=true, files=N)
Submitting 1080 PRICING files to background executor (threads=3)   ← after all Phase 1 done
Ingestion session finished for job X.                              ← job COMPLETED, lock released
                                                                   ← next job can start here
...                                                                ← ~25 min later
Background PRICING complete: files=1080 success=true              ← async event fires
```

### Verify data in database

```sql
-- Confirm all three regions ingested
SELECT
    SUBSTRING_INDEX(file_name, '.', 1) AS dataset,
    COUNT(*)                            AS files_audited,
    SUM(status = 'SUCCESS')             AS succeeded,
    SUM(status = 'SKIPPED_SANITY')      AS sanity_failed
FROM lseg_file_audit
WHERE file_name LIKE 'EIS_INT_%_PRICING%'
GROUP BY 1
ORDER BY 1;
-- Expected: 3 rows — EIS_INT_ASIA_PRICING, EIS_INT_EU_PRICING, EIS_INT_US_PRICING

-- Row counts
SELECT COUNT(*) AS total_rows, MIN(trade_date), MAX(trade_date)
FROM lseg_pricing
WHERE is_deleted = 0;

-- Note files must NOT appear in audit
SELECT COUNT(*) FROM lseg_file_audit WHERE file_name LIKE '%.note.txt.zip';
-- Expected: 0
```

### Corner cases

| Scenario | How to trigger | Expected behaviour |
|----------|---------------|-------------------|
| Note files skipped | Check `lseg_file_audit` | Zero rows with `*.note.txt.zip` names |
| Idempotency | Trigger same `businessDate` again | All pricing files logged as `already-ingested`; row counts unchanged |
| Stop during Phase 1 | `POST /api/jobs/stop?jobId=X` mid-run | Phase 2 PRICING skipped; all 5 events fire with `success=false` (or `true` if target finished before stop) |
| Stop during PRICING | Same, but during Phase 2 | Already-submitted futures continue; the stop signal is not checked inside `pricingExecutor` tasks (job is COMPLETED). Files not yet submitted are never started. PRICING event fires with `success=false` if any file failed. |
| Empty input directory | Point `inputDir` at empty folder | All 5 events fire with `fileCount=0`, `success=true` |
| Wrong business date | Trigger with `businessDate` one day off | Pricing files fail sanity: `SKIPPED_SANITY` in audit, `reason=business date=20260430 expected=20260429` |
| Partial day (US only) | Run with only US files present | Only `EIS_INT_US_PRICING` rows in audit; EU/ASIA events fire with `fileCount=0` |

### Micrometer metrics emitted for PRICING

| Metric | Tags |
|--------|------|
| `ingest.files.total` | `target=PRICING, kind=INT, status=success\|failed` |
| `ingest.rows.parsed` | `target=PRICING` |
| `ingest.rows.inserted` | `target=PRICING` |
| `ingest.rows.ops` | `target=PRICING, op=I` (no U or D — no Action column) |
| `ingest.file.duration` | `target=PRICING, kind=INT, status=success\|failed` |
| `ingest.overall.duration` | (no tags) |
| `ingest.target.errors` | (no tags — incremented on PRICING pipeline failure) |
