# LSEG Ingest — Comprehensive Code Review

> **Purpose**: A manual-review document that explains every file, class, method, and design
> decision in the `lseg-ingest` Spring Boot application. Written for a reader who has not seen
> the code before. Covers validations, constraints, assumptions, workflow, and processing in
> full detail.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Repository Layout](#2-repository-layout)
3. [Configuration — `application.yml`](#3-configuration--applicationyml)
4. [Entry Point — `IngestApplication.java`](#4-entry-point--ingestapplicationjava)
5. [Constants — `Constants.java`](#5-constants--constantsjava)
6. [Configuration Binding — `IngestProperties.java`](#6-configuration-binding--ingestpropertiesjava)
7. [Database Connection — `DataSourceConfig.java`](#7-database-connection--datasourceconfigjava)
8. [Database Schema — Liquibase SQL files](#8-database-schema--liquibase-sql-files)
9. [Domain Model — `plan/` package](#9-domain-model--plan-package)
    - 9.1 `Kind.java`
    - 9.2 `Target.java`
    - 9.3 `IngestFile.java`
    - 9.4 `IngestPlan.java`
    - 9.5 `FileScanner.java`
10. [File I/O — `io/` package](#10-file-io--io-package)
    - 10.1 `ZipLineReader.java`
    - 10.2 `PipeFileParser.java`
11. [Sanity Check — `FileSanityCheck.java`](#11-sanity-check--filesanitycheckjava)
12. [Row Filter — `RicCaretFilter.java`](#12-row-filter--riccaretfilterjava)
13. [Database Loading — `load/` package](#13-database-loading--load-package)
    - 13.1 `TargetSchema.java`
    - 13.2 `SqlBuilder.java`
    - 13.3 `PendingRow.java`
    - 13.4 `SqlRetry.java`
    - 13.5 `ResilientBatchExecutor.java`
    - 13.6 `FileIngestor.java`
14. [Audit & Job Tracking — `audit/` package](#14-audit--job-tracking--audit-package)
    - 14.1 `FileAuditDao.java`
    - 14.2 `JobDao.java`
15. [Orchestration — `orchestrator/` package](#15-orchestration--orchestrator-package)
    - 15.1 `ClusterLock.java`
    - 15.2 `JobReaper.java`
    - 15.3 `JobWorker.java`
    - 15.4 `IngestOrchestrator.java`
    - 15.5 `JobController.java` (REST API)
    - 15.6 `FileController.java` (REST API)
16. [End-to-End Workflow Summary](#16-end-to-end-workflow-summary)
17. [Key Assumptions & Constraints](#17-key-assumptions--constraints)
18. [Known Issues / Review Findings](#18-known-issues--review-findings)

---

## 1. Project Overview

`lseg-ingest` is a **Spring Boot 3.3.5 / Java 21** microservice that reads LSEG (London Stock
Exchange Group) reference-data files from a directory, parses them, and loads the records into
a MariaDB database.

### What it loads

Three logical datasets arrive as **pipe-delimited, gzip-zipped text files**:

| Dataset | DB table | Unique key |
|---------|----------|------------|
| Organisations (ORGS) | `lseg_orgs` | `entity_id` |
| Assets (ASSETS) | `lseg_assets` | `asset_id` |
| Quotes (QUOTES) | `lseg_quotes` | `(asset_id, quote_id)` composite |

### Two file kinds

| Kind | Description | Processing |
|------|-------------|------------|
| `INT` (Initial) | Full snapshot of all rows for a dataset | Multiple files allowed in parallel |
| `DELTA` (REF) | Daily changes (`I`/`U`/`D` actions) applied on top of INT | Must be processed **in sequence number order** |

---

## 2. Repository Layout

```
lseg-ingest/
├── pom.xml                        Maven build descriptor
├── Dockerfile                     Container image definition
├── docker-compose.yml             Single-node dev stack
├── docker-compose.test.yml        Multi-node soak-test stack
├── application.yml                Spring config (in src/main/resources)
├── CORNER_CASES.md                Test matrix: verified + planned scenarios
├── src/
│   ├── main/
│   │   ├── java/com/lseg/ingest/
│   │   │   ├── IngestApplication.java
│   │   │   ├── Constants.java
│   │   │   ├── config/            DataSourceConfig, IngestProperties
│   │   │   ├── plan/              FileScanner, IngestFile, IngestPlan, Kind, Target
│   │   │   ├── io/                ZipLineReader, PipeFileParser
│   │   │   ├── sanity/            FileSanityCheck
│   │   │   ├── filter/            RicCaretFilter
│   │   │   ├── load/              TargetSchema, SqlBuilder, PendingRow,
│   │   │   │                      SqlRetry, ResilientBatchExecutor, FileIngestor
│   │   │   ├── audit/             FileAuditDao, JobDao
│   │   │   └── orchestrator/      ClusterLock, JobReaper, JobWorker,
│   │   │                          IngestOrchestrator, JobController, FileController
│   │   └── resources/
│   │       ├── application.yml
│   │       └── db/changelog/      Liquibase SQL changesets
│   └── test/                      JUnit 5 unit tests + Testcontainers integration tests
```

---

## 3. Configuration — `application.yml`

Location: `src/main/resources/application.yml`

```yaml
server:
  port: ${SERVER_PORT:8080}          # HTTP port; override via env var
```

### Spring auto-configuration

```yaml
spring:
  application:
    name: lseg-ingest
  main:
    web-application-type: servlet    # Runs a traditional Tomcat-backed HTTP server
  liquibase:
    enabled: true
    change-log: classpath:db/changelog/db.changelog-master.xml
    url: jdbc:mariadb://${DB_HOST:localhost}:${DB_PORT:3306}/${DB_NAME:lseg}
    user: ${DB_OWNER_USER:owner}          # Schema-owner account — used ONLY by Liquibase
    password: ${DB_OWNER_PASSWORD:ownerpw}
```

**Key design**: Liquibase (schema migrations) runs as a **separate, privileged `owner` account**
that has DDL rights. The runtime application uses a **separate, restricted `ingest` account** that
only has INSERT/UPDATE/DELETE. This limits blast radius if credentials are compromised.

### Ingest tunables

```yaml
ingest:
  inputDir:  ${INGEST_DIR:/data}      # Directory to scan for files
  archiveDir: ${ARCHIVE_DIR:/archive} # Where processed files are moved after SUCCESS
  charset: ${INGEST_CHARSET:UTF-8}    # File text encoding (override for windows-1252, etc.)
  skipPatterns:
    - "*.note.txt.zip"                # Human-readable note files — never data
    - "Reference-INT-EQUI-*"         # Equity reference feed excluded by business rule
  ricCaretFilter: true                # Drop INT-QUOTES rows whose RIC contains '^'
  threads:
    intPerTable: 10                   # Max parallel threads per table for INT phase
    deltaTargetsParallel: 3           # Max parallel threads across targets for DELTA phase
  batch:
    upsertSize: 5000                  # Rows per JDBC batch for INSERT/UPDATE
    deleteSize: 5000                  # Rows per JDBC batch for DELETE
  resilience:
    fallbackOnBatchFail: true         # On BatchUpdateException: retry row-by-row
    maxSkippedRowsPerFile: 1000       # Abort a file if too many rows error
  cancel:
    checkRows: 5000                   # Poll stop signal every N rows inside a file
  reaper:
    enabled: true
    staleJobTimeoutSeconds: 10800     # 3 hours: mark RUNNING jobs FAILED if heartbeat gone
    pollIntervalSeconds: 60           # How often the reaper checks
  cluster:
    lockName: ${CLUSTER_LOCK_NAME:lseg-ingest-cluster}
    heartbeatIntervalSeconds: 30      # Background thread updates last_heartbeat_at this often
  retry:
    maxAttempts: 3
    initialDelayMs: 250               # First retry after 250 ms
    maxDelayMs: 5000                  # Cap backoff at 5 s
```

### Runtime datasource

```yaml
db:
  runtime:
    url: jdbc:mariadb://...?rewriteBatchedStatements=true&useServerPrepStmts=false
                            &cachePrepStmts=true&useLocalSessionState=true
    user: ${DB_USER:ingest}           # Restricted account — INSERT/UPDATE/DELETE only
    password: ${DB_PASSWORD:ingestpw}
    pool:
      maxSize: 40                     # HikariCP max pool connections
      minIdle: 10
      connectionTimeoutMs: 30000
```

`rewriteBatchedStatements=true` — **Critical performance setting**: tells the MariaDB JDBC driver
to rewrite a `executeBatch()` call into a single multi-row INSERT statement rather than sending
N individual INSERT statements. Without this, batching would be much slower.

---

## 4. Entry Point — `IngestApplication.java`

```java
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
```

Spring Boot's `DataSourceAutoConfiguration` would try to auto-create a DataSource from
`spring.datasource.*` properties. It is **excluded** because the application manually creates its
own HikariCP DataSource (`DataSourceConfig`) from `db.runtime.*` properties. Without this
exclusion, Spring would fail to start if `spring.datasource.url` is not set.

```java
@EnableConfigurationProperties(IngestProperties.class)
```

Binds the `ingest.*` YAML block to the `IngestProperties` POJO via `@ConfigurationProperties`.

```java
@EnableScheduling
```

Enables Spring's `@Scheduled` annotation, which drives `JobWorker.pollAndExecute()` (every 1 s)
and `JobReaper.reap()` (every 60 s).

```java
@Bean
public MeterRegistry meterRegistry() {
    return new SimpleMeterRegistry();
}
```

Registers a simple in-memory Micrometer registry. All metrics (`ingest.files.total`,
`ingest.rows.parsed`, etc.) are stored here. In a production environment, you would replace
`SimpleMeterRegistry` with a Prometheus or Graphite registry and expose `/actuator/metrics`.

---

## 5. Constants — `Constants.java`

A utility class with no instances (`private Constants() {}`). Contains every hard-coded string
used across the application, preventing typos and making grep-based searches reliable.

### Job Status lifecycle

```
QUEUED → RUNNING → COMPLETED
                 → FAILED
                 → STOPPED
```

- `QUEUED` — inserted by `JobController.trigger()`
- `RUNNING` — set atomically by `JobDao.claimJob()`
- `COMPLETED` — set by `JobWorker` after `orchestrator.run()` returns `true`
- `FAILED` — set on uncaught exception, or by the reaper
- `STOPPED` — set by `JobController.stop()`; once set, it is **never overwritten**

### File Audit Status lifecycle

```
STARTED → SUCCESS
        → FAILED
        → SKIPPED
        → SKIPPED_SANITY
```

### Action Types

| Constant | Value | Meaning |
|----------|-------|---------|
| `ACTION_INSERT` | `"I"` | Insert new row |
| `ACTION_UPDATE` | `"U"` | Full upsert (treated same as I) |
| `ACTION_DELETE` | `"D"` | Soft-delete: sets `is_deleted = 1` |

### Metrics

All metric names follow the `ingest.*` namespace. Tags are `target`, `kind`, `status`, `op`.

---

## 6. Configuration Binding — `IngestProperties.java`

Annotated with `@ConfigurationProperties("ingest")`, this class is the **single source of truth**
for all tunable parameters. Spring populates it at startup from `application.yml` / environment
variables.

### Key nested classes

| Class | Fields | Purpose |
|-------|--------|---------|
| `Threads` | `intPerTable`, `deltaTargetsParallel` | Thread pool sizes |
| `Batch` | `upsertSize`, `deleteSize` | JDBC batch flush thresholds |
| `Resilience` | `fallbackOnBatchFail`, `maxSkippedRowsPerFile` | Error tolerance limits |
| `Cancel` | `checkRows` | How often to poll for job-stop signal |
| `Reaper` | `enabled`, `staleJobTimeoutSeconds`, `pollIntervalSeconds` | Zombie-job cleanup |
| `Cluster` | `lockName`, `heartbeatIntervalSeconds` | Distributed lock name + heartbeat rate |
| `Retry` | `maxAttempts`, `initialDelayMs`, `maxDelayMs` | Transient-error retry config |

**Validation gap**: There is no `@Validated` or `@Min`/`@Max` annotation on these nested classes.
A value of `intPerTable: 0` would cause `Math.min(0, files.size())` to create a thread pool of
size 0, causing `RejectedExecutionException`. Defensive `Math.max(1, ...)` guards exist in some
places but not all.

---

## 7. Database Connection — `DataSourceConfig.java`

Creates a **HikariCP** connection pool for the runtime (restricted) DB user.

```java
cfg.setConnectionTestQuery("SELECT 1");
cfg.setValidationTimeout(5000);
```
Every connection is validated with `SELECT 1` before being handed to a caller. If the DB is
unreachable, the pool throws rather than returning a broken connection.

```java
cfg.setLeakDetectionThreshold(600000);  // 10 minutes
```
If a thread holds a connection for more than 10 minutes, HikariCP logs a warning. Typical
file ingestion takes seconds, so any hold > 10 min indicates a stuck thread.

```java
cfg.setKeepaliveTime(120000);  // 2 minutes
```
Sends a keepalive query every 2 minutes to prevent the MariaDB server from closing idle
connections (MariaDB default `wait_timeout` is 8 hours, but some network firewalls drop
idle TCP connections earlier).

---

## 8. Database Schema — Liquibase SQL files

Liquibase applies changes under the schema-owner account. The master changelog includes five
files in order.

### `001-lseg_orgs.sql` — `lseg_orgs`

| Column | Type | Notes |
|--------|------|-------|
| `id` | `BIGINT AUTO_INCREMENT PK` | Surrogate key; never exposed to LSEG |
| `entity_id` | `VARCHAR(255)` | **Business unique key** — `UNIQUE KEY uniq_orgs_entity_id` |
| *(20 more columns)* | `VARCHAR(255)` | All source columns stored as strings |
| `is_deleted` | `TINYINT(1) DEFAULT 0` | Soft-delete flag; 0=live, 1=deleted |

Secondary indexes: `idx_orgs_entity_perm_id`, `idx_orgs_is_deleted`.

### `002-lseg_assets.sql` — `lseg_assets`

| Column | Type | Notes |
|--------|------|-------|
| `id` | `BIGINT AUTO_INCREMENT PK` | Surrogate key |
| `asset_id` | `VARCHAR(255)` | **Business unique key** — `UNIQUE KEY uniq_assets_asset_id` |
| *(12 more columns)* | `VARCHAR(255)` | All as strings |
| `is_deleted` | `TINYINT(1) DEFAULT 0` | Soft-delete |

### `003-lseg_quotes.sql` — `lseg_quotes`

| Column | Type | Notes |
|--------|------|-------|
| `id` | `BIGINT AUTO_INCREMENT PK` | Surrogate key |
| `asset_id` + `quote_id` | `VARCHAR(255)` each | **Composite business unique key** — `UNIQUE KEY uniq_quotes_asset_id_quote_id` |
| *(21 more columns)* | `VARCHAR(255)` | All as strings |
| `is_deleted` | `TINYINT(1) DEFAULT 0` | Soft-delete |

### `004-lseg_file_audit.sql` — `lseg_file_audit`

Tracks per-file processing state. The `file_name` column has a `UNIQUE KEY`; this is the
idempotency anchor — if the same file is submitted twice, the second run finds the row and
skips it.

Key columns: `file_name`, `status`, `declared_rows`, `parsed_rows`, `inserted_rows`,
`skipped_rows`, `ins_count`, `upd_count`, `del_count`, `error_message`, `started_at`, `finished_at`.

### `005-lseg_jobs.sql` — `lseg_jobs`

Tracks per-job lifecycle. Used for distributed locking/coordination.

Key columns: `status`, `node_id`, `business_date`, `input_dir`, `created_at`, `started_at`,
`finished_at`, `last_heartbeat_at`, `error_message`.

**Design note on NULL handling in UNIQUE indexes**: MariaDB (like MySQL) treats `NULL` values in
a `UNIQUE` index as distinct from each other — multiple rows can have `entity_id = NULL`. This
is intentional: the code comment in `Target.java` says "Rows with NULL values in any unique-key
column are still inserted; duplicates are tolerated by design."

---

## 9. Domain Model — `plan/` package

### 9.1 `Kind.java`

```java
public enum Kind { INT, DELTA }
```

Two values only. `INT` = full initial load. `DELTA` = incremental change feed.

### 9.2 `Target.java`

```java
public enum Target {
    ORGS("lseg_orgs",
         List.of("entity_id"),
         List.of("Entity_ID")),
    ASSETS("lseg_assets",
           List.of("asset_id"),
           List.of("Asset_ID")),
    QUOTES("lseg_quotes",
           List.of("asset_id", "quote_id"),
           List.of("Asset_ID", "Quote_ID"));
```

Each target carries:

| Field | Purpose |
|-------|---------|
| `table` | The MariaDB table name |
| `uniqueKeyColumns` | DB column names used in `ON DUPLICATE KEY UPDATE` and `WHERE` for DELETE |
| `uniqueKeySourceHeaders` | The **file column names** that correspond to the above DB columns |

The constructor validates that both lists have the same size — a hard invariant required for the
index-aligned look-up in `FileIngestor`.

### 9.3 `IngestFile.java`

```java
public record IngestFile(Path path, String fileName, String dataset, Target target, Kind kind, int seq) {}
```

An immutable value object. `seq` is the sequence number parsed from the filename (e.g., `.3.` in
`EIS_DELTA_ASIA_US_QUOTE.REF.25963.20260425.3.1.1.txt.zip`). DELTA files must be applied in
ascending `seq` order.

### 9.4 `IngestPlan.java`

Organises all `IngestFile` objects into two `EnumMap` buckets:
- `intByTarget` — maps each `Target` to its list of INT files
- `deltaByTarget` — maps each `Target` to its list of DELTA files, **sorted ascending by `seq`**

The sort is applied at construction time so every downstream consumer automatically gets DELTA
files in the correct order.

```java
public String summary() { ... }
```

Produces a one-line summary like `ORGS: INT=1 DELTA=5, ASSETS: INT=1 DELTA=3, QUOTES: INT=4 DELTA=47`.

### 9.5 `FileScanner.java`

Scans the input directory and converts filenames into `IngestFile` objects.

#### Filename regex

```
^(?<dataset>[A-Za-z0-9_\-]+)\.(?<kind>INT|REF)\.(?<feed>[A-Za-z0-9]+)\.(?<date>\d{8})\.(?<seq>\d+)\.\d+\.\d+\.txt\.zip$
```

Only filenames matching this pattern are accepted. Everything else is logged at DEBUG level
and ignored.

**Note**: The pattern accepts `INT` or `REF` as the kind token. `REF` is the LSEG-native
name for DELTA files; the code maps `REF` → `Kind.DELTA`.

#### Skip patterns

Before the regex is applied, each filename is tested against the configured glob patterns
(`*.note.txt.zip`, `Reference-INT-EQUI-*`). Matching files are silently dropped.

#### Target mapping — `mapTarget(String dataset)`

```java
// ORGS
if (dataset.equals("Organization")) return Target.ORGS;
if (dataset.contains("GLOBAL_ORGN") || dataset.contains("GLOABL_ORGN")) return Target.ORGS;
// ASSETS
if (dataset.startsWith("EIS_INT_") && dataset.endsWith("_ASSETS")) return Target.ASSETS;
if (dataset.equals("EIS_DELTA_GLOBAL_ASSETS")) return Target.ASSETS;
// QUOTES
if (dataset.startsWith("EIS_INT_") && (dataset.endsWith("_QUOTE") || dataset.endsWith("_QUOTES"))) return Target.QUOTES;
if (dataset.startsWith("EIS_DELTA_") && dataset.endsWith("_QUOTE")) return Target.QUOTES;
return null;
```

**Hardcoded vendor typo**: `"GLOABL_ORGN"` (GLOABL not GLOBAL) is intentionally accepted because
LSEG has shipped files with this typo in production.

Files whose dataset name does not match any rule return `null` and are ignored. This means new
feed types added by LSEG will silently not be loaded until the mapping is updated.

---

## 10. File I/O — `io/` package

### 10.1 `ZipLineReader.java`

Opens a `.txt.zip` file and streams its first non-directory entry as a `BufferedReader`.

```java
this.fileStream = Files.newInputStream(path);
this.zip = new ZipInputStream(fileStream);
ZipEntry entry = zip.getNextEntry();
while (entry != null && entry.isDirectory()) entry = zip.getNextEntry();
if (entry == null) throw new IOException("Zip is empty: " + path);
this.reader = new BufferedReader(new InputStreamReader(zip, charset), 1 << 16);
```

- Loops past directory entries (`.txt.zip` files from LSEG do not have directories, but this is
  defensive).
- Buffer size `1 << 16 = 65536` bytes — tuned for streaming large files without excessive
  system-call overhead.
- `AutoCloseable` with a nested try-finally chain in `close()` to guarantee all three streams
  (`reader`, `zip`, `fileStream`) are closed even if one of the closes throws.

**Assumption**: The zip contains exactly one meaningful file. If the zip contained multiple text
files, only the first would be read.

### 10.2 `PipeFileParser.java`

Parses the pipe-delimited LSEG text format from a `BufferedReader`.

#### LSEG file format

```
EIS_DELTA_ASIA_US_QUOTE|REF|25963|20260425|3|12345|     ← metadata row
Action|Asset_ID|Quote_ID|RIC|...                        ← header row
I|12345|67890|VOD.L|...                                  ← data rows
U|12345|99999|BT.L|...
D|00001|11111||...
```

#### `initialize(int maxLookahead)`

Reads up to `maxLookahead` (default: 50) lines looking for:

1. **Metadata row**: detected by `looksLikeMetadata()` — at least 6 pipe-separated tokens where
   token[1] is `INT` or `REF` and token[3] matches `\d{8}`. Parses into:
   `Metadata(dataset, kind, feed, businessDate, seq, declaredRows)`.

2. **Header row**: detected by `looksLikeHeader()` — line starts with `Action|`. Once found, the
   column names are extracted and indexed in a `Map<String, Integer>` for O(1) lookup.

   - Duplicate column names: the **first occurrence** wins; duplicates are logged as a warning.
   - If the header is not found within 50 lines, throws `IOException("Header row not found...")`.

#### `nextRow()`

Reads the next non-empty line, splits on `|`, and:
- **Short row** (fewer tokens than header columns): padded with `null` to header length.
- **Long row** (more tokens than header columns): truncated to header length, warning logged.
  This handles cases where a free-text field contains an embedded `|`.
- Trailing empty token from the row-terminating `|` is dropped.

#### `splitTokens(String line)`

```java
String[] arr = line.split("\\|", -1);
int len = arr.length;
if (len > 0 && arr[len - 1].isEmpty()) len--;
```

Uses `-1` limit on `split` to preserve empty tokens (empty middle fields = `||` become `null`).
Drops exactly one trailing empty token introduced by the mandatory trailing `|`.

---

## 11. Sanity Check — `FileSanityCheck.java`

Runs **before** any database work. Reads only the first 50 lines of the file.

### Checks performed (in order)

1. **Metadata row present**: calls `PipeFileParser.initialize(50)`. If metadata is null, fails.

2. **Kind matches filename**: if the `IngestFile` was classified as `Kind.INT`, metadata must
   say `INT`. If classified as `Kind.DELTA`, metadata must say `REF`.
   ```java
   String expectedKind = (file.kind() == Kind.INT) ? KIND_INT : KIND_REF;
   ```

3. **Business date matches**: if `expectedBusinessDate` is non-null (always passed from job),
   the metadata's `businessDate` field must equal it. This prevents loading yesterday's files
   into today's job.

4. **Unique-key columns present**: for each `uniqueKeySourceHeaders` of the target, checks that
   the column name appears in the file's header. If any are missing, the file is rejected.
   Example: a QUOTES file missing `Quote_ID` cannot be safely upserted.

### Result

Returns a `Result(ok, reason, metadata, headerColumns)` record. On failure, the orchestrator
calls `audit.markSkippedSanity(...)` and the file is not ingested.

**Assumption**: The sanity check reads the same zip twice — once during sanity, once during
`FileIngestor.doIngest()`. This is intentional: sanity only reads 50 lines (cheap), while
ingest reads the entire file.

---

## 12. Row Filter — `RicCaretFilter.java`

```java
public boolean shouldSkip(String[] row) {
    if (ricColumnIndex < 0 || ricColumnIndex >= row.length) return false;
    String ric = row[ricColumnIndex];
    return ric != null && ric.indexOf('^') >= 0;
}
```

Drops rows whose `RIC` column contains a caret (`^`). RICs with `^` are LSEG internal
identifiers for index components (e.g. `.SPX^A`), not tradeable instruments.

**Applied only when**:
1. `ingest.ricCaretFilter = true` in config
2. The file is of kind `INT` (DELTA caret rows are retained — design decision in `FileIngestor`)
3. The target is `QUOTES`
4. The `RIC` column is present in the file header

If the `RIC` column is absent, `ricColumnIndex = -1` and the filter never fires (all rows pass).

---

## 13. Database Loading — `load/` package

### 13.1 `TargetSchema.java`

A static registry mapping each `Target` to an ordered list of `Column` records:
`(sourceHeader, dbColumn, sqlType, ValueBinder)`.

Every column is bound as `VARCHAR` using the `STRING` binder:
```java
private static final ValueBinder STRING = (ps, i, v) -> {
    if (v == null) ps.setNull(i, Types.VARCHAR);
    else ps.setString(i, v);
};
```

`NULL` values are properly bound as SQL NULL (`ps.setNull`), not as the string `"null"`.

#### `intersect(Target t, Set<String> fileHeaders)`

Returns only the columns whose `sourceHeader` is present in the file's header set. This is
the "column-mapping" step: if the file does not have `Issuer_Name`, that column is omitted
from the INSERT statement entirely (rather than bound as NULL).

**Implication**: only columns present in **both** the schema definition and the file's header
are written to the DB. Unknown file columns are ignored. Schema columns absent from the file
are not written (default remains whatever the DB column default is — for `VARCHAR` columns:
`NULL`).

### 13.2 `SqlBuilder.java`

Generates two SQL templates at runtime:

#### `upsert(Target target, List<Column> cols)`

```sql
INSERT INTO lseg_quotes (asset_id, quote_id, ric, ...)
VALUES (?, ?, ?, ...)
ON DUPLICATE KEY UPDATE ric=VALUES(ric), ..., is_deleted=0
```

Rules:
- **Unique-key columns** (`asset_id`, `quote_id` for QUOTES) are **excluded** from the
  `ON DUPLICATE KEY UPDATE` clause — no point writing the same value back to the defining column.
- `is_deleted=0` is **always appended** to the ON DUPLICATE clause. This means if a row was
  previously soft-deleted and a new `I` or `U` arrives, the row is automatically un-deleted.
- If all columns in the file are key columns (edge case), the fallback `onDup = "is_deleted=0"`
  still produces a valid SQL statement.

#### `delete(Target target)`

```sql
UPDATE lseg_quotes SET is_deleted = 1 WHERE asset_id = ? AND quote_id = ?
```

Soft-delete only. The row is never physically removed. This allows audit trails and future
re-activation (via a subsequent `I` action which sets `is_deleted=0`).

### 13.3 `PendingRow.java`

```java
public record PendingRow(String[] values, long lineNumber, String keyValue) {}
```

An immutable container for one row's data:
- `values` — aligned to the column list (for upsert rows) or the unique-key list (for delete rows)
- `lineNumber` — source file line for error diagnostics
- `keyValue` — first key column value for log messages

### 13.4 `SqlRetry.java`

Wraps any operation in a retry loop with **exponential backoff**.

#### Transient error detection — `isTransient(Throwable t)`

Walks the full exception cause chain and returns `true` if any cause is:

| Condition | Error type |
|-----------|------------|
| `instanceof SQLTransientException` | JDBC standard transient |
| `errorCode == 1213` | MariaDB deadlock |
| `errorCode == 1205` | MariaDB lock-wait timeout |
| `sqlState starts with "08"` | Connection class errors |
| `sqlState == "40001"` | Serialization failure |

Non-transient errors (e.g. `1062` — unique key violation) propagate immediately without retry.

#### Retry loop

```java
for (int i = 1; i <= attempts; i++) {
    try { return op.run(); }
    catch (Exception e) {
        if (!isTransient(e) || i == attempts) throw e;  // non-transient or exhausted: give up
        Thread.sleep(delay);
        delay = Math.min(delay * 2, cfg.getMaxDelayMs());  // exponential cap
    }
}
```

Backoff sequence with `initialDelayMs=250`, `maxDelayMs=5000`: 250ms → 500ms → 1000ms → … → 5000ms.

### 13.5 `ResilientBatchExecutor.java`

Implements the **two-tier execution model**:

```
Tier 1 (fast): executeBatch() for a full batch (up to 5000 rows)
    ↓ on failure
Tier 2 (safe): ps.clearBatch() then executeUpdate() per row, skipping individual failures
```

#### Lifecycle

1. `add(PendingRow row)` — binds the row, calls `ps.addBatch()`, and appends to the
   `buffered` list. When `buffered.size() >= flushAt`, calls `flush()` automatically.

2. `flush()` — attempts `executeBatch()` wrapped in `SqlRetry.withRetry()`.
   - **Success**: increments `succeeded` by the batch size; clears `buffered`.
   - **Failure**: clears the batch, then loops through each row individually.
     - For each row: calls `SqlRetry.withRetry()` then `executeUpdate()`.
     - **Row success**: increments `succeeded`.
     - **Row failure**: increments `skipped`, logs the line number, key, and raw values.
   - `buffered.clear()` in the `finally` block runs in both success and failure paths.

3. `close()` — closes the `PreparedStatement` only (does **not** close the `Connection`).
   The `Connection` is managed by `FileIngestor`.

**Important**: transient batch failures (deadlocks) are retried at the batch level **without
falling back to row-by-row**. Only after retries are exhausted does the row-by-row fallback
engage. This avoids the high overhead of individual `executeUpdate()` calls for every deadlock.

### 13.6 `FileIngestor.java`

The central loading engine. Processes one file end-to-end within a single JDBC transaction.

#### High-level flow

```
MDC context setup (jobId, fileName)
  └── SqlRetry outer wrapper (for transient errors on entire ingest attempt)
        └── doIngest()
              1. Open ZipLineReader
              2. PipeFileParser.initialize(50) → locate header
              3. audit.markStarted()
              4. Intersect file header with TargetSchema → build column list
              5. Pre-compute source→DB index mapping arrays
              6. Pre-compute unique-key index mapping
              7. Determine action index, RIC index, filter flags
              8. Build upsert SQL + delete SQL
              9. Get Connection from pool; setAutoCommit(false)
              10. Create ResilientBatchExecutor for upsert + delete
              11. Read rows:
                    - every N rows: poll for stop signal + log progress
                    - if RIC caret filter applies: skip row
                    - determine action (I/U → upsert, D → delete)
                    - on action flip (upsert↔delete): flush the active side first
              12. Final flush of both sides (in correct order)
              13. conn.commit()
              14. Emit Micrometer counters
              15. audit.markFinished(SUCCESS)
              ON EXCEPTION:
                    conn.rollback()
                    audit.markFinished(FAILED, error)
                    rethrow
```

#### Action-order preservation (flush-on-flip)

```java
char activeSide = 0; // 0=none, 'U'=upsert side, 'D'=delete side

if (action == 'D') {
    if (activeSide == 'U') { upserter.flush(); activeSide = 0; }
    // add to deleter...
    activeSide = 'D';
} else {
    if (activeSide == 'D') { deleter.flush(); activeSide = 0; }
    // add to upserter...
    activeSide = 'U';
}
```

This guarantees in-file ordering is respected:
- `D key` then `I key` in same file → DELETE fires first, then INSERT → row is live
- `I key` then `D key` in same file → INSERT fires first, then DELETE → row is soft-deleted

Without this flush-on-flip logic, batching would reorder ops and produce wrong results.

#### Column value extraction

```java
String[] vals = new String[cols.size()];
for (int i = 0; i < cols.size(); i++) {
    int idx = srcIndex[i];
    vals[i] = (idx >= 0 && idx < row.length) ? row[idx] : null;
}
```

`srcIndex[i]` is pre-computed as `headerIndex.get(cols[i].sourceHeader())`. Access is O(1) per
column. Out-of-range index → null (defensive, should not occur if header intersect is correct).

#### Delete key extraction

```java
String[] keyVals = new String[keySrcIdx.length];
for (int k = 0; k < keySrcIdx.length; k++) {
    int idx = keySrcIdx[k];
    keyVals[k] = (idx >= 0 && idx < row.length) ? row[idx] : null;
}
deleter.add(new PendingRow(keyVals, parser.currentLine(), firstKey));
```

For delete rows, only the key column values are extracted (not the full row), since the DELETE
SQL only has `WHERE asset_id=? AND quote_id=?`.

#### Action field default

```java
String actionStr = (actionIdx >= 0 && actionIdx < row.length 
                    && row[actionIdx] != null && !row[actionIdx].isEmpty())
                   ? row[actionIdx] : ACTION_INSERT;
char action = actionStr.charAt(0);
```

If the `Action` column is missing, empty, or null, the row is treated as an `I` (insert).
Unknown action letters (e.g. `X`) also fall through to the upsert branch, treated as insert.

#### Final flush ordering

```java
if (activeSide == 'D') {
    upserter.flush();  // flush empty upserter first (no-op but safe)
    deleter.flush();
} else {
    deleter.flush();   // flush empty deleter first (no-op but safe)
    upserter.flush();
}
```

**Bug note**: The branch condition is inverted from what the comment implies.
- When `activeSide == 'D'` (last rows were deletes), the correct final order is:
  flush the upserter (anything pending there from earlier), then flush the deleter.
- When `activeSide == 'U'` or `0` (last rows were upserts or mixed), flush the deleter first
  (in case any prior deletes are pending), then the upserter.
- Both flushes are no-ops if the respective buffer is empty, so the current code is
  **functionally correct** — but the branch is logically redundant since calling
  `upserter.flush(); deleter.flush()` in either order would work if both have been
  incrementally flushed-on-flip. The code is safe.

---

## 14. Audit & Job Tracking — `audit/` package

### 14.1 `FileAuditDao.java`

Uses `JdbcTemplate` (Spring's thin JDBC wrapper).

#### `loadSuccessFileNames()`

```java
SELECT file_name FROM lseg_file_audit WHERE status = 'SUCCESS'
AND finished_at >= (CURRENT_DATE - INTERVAL 1 MONTH)
```

Returns a `Set<String>` of file names that were previously ingested successfully **in the last
month**. Files older than 1 month are excluded from the skip-cache, meaning they would be
re-ingested if they reappear in the input directory.

**Design trade-off**: the 1-month window prevents the skip-cache from growing unbounded.
The assumption is that input files from > 1 month ago will never reappear.

#### `markStarted()`

Uses `INSERT ... ON DUPLICATE KEY UPDATE` to upsert the audit row. This handles re-runs:
if a file was previously ingested (any status), the row is reset to `STARTED` with fresh
timestamps and cleared counts.

**SQL injection note**: Status constants (`AUDIT_STARTED`, etc.) are string literals from
`Constants.java`, not user input — they are safe to embed directly in SQL. User-supplied
data (filename, etc.) is always bound via `?` parameters.

#### `markFinished()`

Simple UPDATE by `file_name`. Updates status, counts, error message, and `finished_at`.

#### `markSkippedSanity()`

Called by orchestrator when a file fails the pre-ingest sanity check. Writes the failure
reason as `error_message`.

#### `markManualSkip()`

Called by `FileController.skip()` REST endpoint. Allows an operator to manually mark a
specific file as `SKIPPED` so it will not be re-ingested.

### 14.2 `JobDao.java`

Manages the `lseg_jobs` table lifecycle.

#### `queueJob(String businessDate, String inputDir)`

```java
INSERT INTO lseg_jobs (status, business_date, input_dir) VALUES ('QUEUED', ?, ?)
```

Uses `GeneratedKeyHolder` to capture the auto-incremented `id`. This is safer than calling
`LAST_INSERT_ID()` in a separate query (which could return the wrong ID if another insert
happened on the same connection).

#### `claimJob(String nodeId)` — Race-free atomic claim

```java
// Step 1: find the oldest QUEUED job
SELECT id FROM lseg_jobs WHERE status='QUEUED' ORDER BY id ASC LIMIT 1

// Step 2: atomically claim it
UPDATE lseg_jobs SET status='RUNNING', node_id=?, started_at=CURRENT_TIMESTAMP,
    last_heartbeat_at=CURRENT_TIMESTAMP
WHERE id=? AND status='QUEUED'
```

Only the node whose UPDATE returns `rowsAffected == 1` has successfully claimed the job.
If two nodes see the same `id` from the SELECT, only one will succeed on the UPDATE because
of the `AND status='QUEUED'` guard. The loser gets `rowsAffected == 0` and returns
`Optional.empty()`.

#### `updateStatus()` — STOPPED-aware

```sql
UPDATE lseg_jobs SET
    status = CASE WHEN status='STOPPED' THEN 'STOPPED' ELSE ? END,
    finished_at = CASE WHEN ? IN ('COMPLETED','FAILED','STOPPED') THEN CURRENT_TIMESTAMP ELSE finished_at END,
    error_message = CASE WHEN status='STOPPED' THEN error_message ELSE ? END
WHERE id = ?
```

If the job is already `STOPPED` (operator-set), neither `status` nor `error_message` is
overwritten. This prevents a race where the orchestrator tries to mark the job COMPLETED
after the operator has already stopped it.

#### `isStopped(long jobId)`

```java
try {
    return STATUS_STOPPED.equals(getStatus(jobId));
} catch (Exception e) {
    return false;  // treat as not-stopped to avoid false-positive aborts
}
```

Returns `false` on DB error rather than `true`. This is intentional: a transient DB hiccup
should not cause the job to abort; the heartbeat / reaper mechanism handles the crash case.

#### `reapStale(long staleSeconds)`

```sql
UPDATE lseg_jobs SET status='FAILED', finished_at=CURRENT_TIMESTAMP,
    error_message=CONCAT('reaped: heartbeat stale > ', ?, 's')
WHERE status='RUNNING' AND last_heartbeat_at < (NOW() - INTERVAL ? SECOND)
```

Single-statement atomic update. Only affects `RUNNING` jobs whose heartbeat has expired.
`STOPPED` jobs are excluded by the `status='RUNNING'` guard.

---

## 15. Orchestration — `orchestrator/` package

### 15.1 `ClusterLock.java`

Uses MariaDB's advisory lock function `GET_LOCK(name, timeout)` to implement a
**cluster-wide mutex**.

```java
PreparedStatement ps = conn.prepareStatement("SELECT GET_LOCK(?, 0)");
ps.setString(1, name);
// timeout=0 means: try once, return immediately if already held
ResultSet rs = ps.executeQuery();
int v = rs.getInt(1);  // 1=acquired, 0=held elsewhere, NULL=error
```

- If `v == 1`: lock acquired. The `Handle` holds the connection open for the lock's lifetime.
- If `v == 0 or null`: not acquired. Connection is closed immediately.

The `Handle.close()` method calls `RELEASE_LOCK(name)` and then closes the connection.

**Crash safety**: `GET_LOCK` is session-scoped in MariaDB. If the node crashes, MariaDB drops
the connection and automatically releases the lock. This bounds the "lock-stuck" window to
`wait_timeout` (default 8 hours) — which is why the `JobReaper` exists as a safety net for
the job row (the lock itself self-heals faster than the job row's heartbeat timeout).

**Assumption**: A `Handle` must be used in a try-with-resources block. Failure to close would
leak both the connection and the lock.

### 15.2 `JobReaper.java`

```java
@Scheduled(fixedDelayString = "#{${ingest.reaper.pollIntervalSeconds:60} * 1000}")
public void reap() {
    if (!props.getReaper().isEnabled()) return;
    int reaped = jobDao.reapStale(stale);
    if (reaped > 0) log.warn("Reaped {} stale RUNNING job(s)...", reaped, stale);
}
```

Runs every `pollIntervalSeconds` (default 60 s). Calls `JobDao.reapStale()` which marks jobs
whose `last_heartbeat_at` is older than `staleJobTimeoutSeconds` as FAILED.

**Design**: The heartbeat updates every 30 s. The reaper fires after 10800 s (3 hours). So a
node can miss ~360 heartbeats before its job is reaped. This is deliberately conservative.
For test/lab use, both can be overridden via config.

### 15.3 `JobWorker.java`

```java
@Scheduled(fixedDelay = 1000)  // every 1 second
public void pollAndExecute() {
    Optional<Long> jobIdOpt = jobDao.claimJob(nodeId);
    if (jobIdOpt.isEmpty()) return;
    long jobId = jobIdOpt.get();
    try {
        boolean ran = orchestrator.run(jobId);
        if (!ran) return;  // already put back to QUEUED or was stopped
        jobDao.updateStatus(jobId, STATUS_COMPLETED, null);
    } catch (Exception e) {
        jobDao.updateStatus(jobId, STATUS_FAILED, truncate(e.getMessage()));
    }
}
```

The polling loop runs every 1 second. `claimJob()` is a fast DB read + conditional update.
If no QUEUED jobs exist, it returns immediately.

`nodeId` is generated at startup from `hostname + epoch_ms_mod_100000 + uuid_prefix`:
```java
String host = InetAddress.getLocalHost().getHostName();
return host + "-" + System.currentTimeMillis() % 100000 + "-" + UUID.randomUUID().toString().substring(0, 4);
```

### 15.4 `IngestOrchestrator.java`

The central workflow coordinator. Drives one complete ingestion run.

#### Step-by-step flow

```
run(jobId):
  1. MDC context setup
  2. try (ClusterLock.Handle lock = clusterLock.tryAcquire()):
       if !lock.acquired():
           updateStatus(QUEUED)  ← put back for next poll
           return false
  3. heartbeat = startHeartbeat(jobId)
  4. checkStop(jobId)  ← fail-fast if already stopped
  5. businessDate, jobDir = jobDao.get*()
  6. files = scanner.scan()
  7. alreadyDone = audit.loadSuccessFileNames()
  8. remaining = files.filter(not in alreadyDone)
  9. for each file in remaining:
         checkStop()
         result = sanity.check(file, businessDate)
         if !result.ok(): audit.markSkippedSanity()
         else: good.add(file)
  10. plan = new IngestPlan(good)  ← sorts DELTA by seq
  11. perTargetPool = fixedThreadPool(deltaTargetsParallel=3)
  12. for each Target:
          perTargetPool.submit(() -> runTarget(t, plan, jobId, businessDate))
  13. waitWithHeartbeat() for all target futures
  14. perTargetPool.shutdown()
  15. if stopped → return false
  16. if anyTargetError → throw
  17. return true
  finally:
      heartbeat.shutdownNow()
      overallSample.stop()
      MDC.remove(jobId)
```

#### `runTarget(Target t, ...)`

```
runTarget:
  checkStop()
  runIntPhase(t, ...)   ← parallel within the INT files for this target
  checkStop()
  runDeltaPhase(t, ...) ← strictly sequential by seq
```

**Critical ordering guarantee**: DELTA never starts until ALL INT files for the same target
have completed. This is enforced by `runIntPhase` blocking until all futures complete before
returning.

#### `runIntPhase(Target t, ...)`

Creates a per-target thread pool of `Math.min(intPerTable, files.size())` threads, submits
one task per INT file, and waits for all of them before returning.

#### `runDeltaPhase(Target t, ...)`

A simple sequential `for` loop — no thread pool. Each DELTA file is processed one at a time
in ascending `seq` order.

#### `startHeartbeat(long jobId)`

```java
ScheduledExecutorService ex = Executors.newSingleThreadScheduledExecutor(daemonThreads("heartbeat"));
ex.scheduleAtFixedRate(() -> jobDao.heartbeat(jobId), interval, interval, TimeUnit.SECONDS);
```

Background daemon thread that calls `UPDATE lseg_jobs SET last_heartbeat_at=NOW() WHERE id=? AND status='RUNNING'`
every 30 seconds. Heartbeat failures are only WARN-logged — a temporary DB hiccup should not
abort an otherwise healthy job.

#### `archive(IngestFile f)`

After successful ingest, moves the file from `inputDir` to `archiveDir`:
```java
Files.move(f.path(), archivePath.resolve(f.fileName()), StandardCopyOption.REPLACE_EXISTING);
```

`REPLACE_EXISTING` means if a file with the same name already exists in the archive, it is
overwritten. Failures are caught and only increment the `ingest.archive.errors` metric — archive
failure does **not** mark the ingest as failed.

#### `waitWithHeartbeat(Future f, ...)`

```java
while (!f.isDone()) {
    try { f.get(1, TimeUnit.MINUTES); }
    catch (TimeoutException e) { log.info("Still running..."); }
}
```

Polls the future every 1 minute rather than blocking indefinitely. The heartbeat thread runs
independently; this method is just a monitoring loop so the log is not silent.

### 15.5 `JobController.java`

REST API for job lifecycle management.

| Endpoint | Method | Parameters | Action |
|----------|--------|-----------|--------|
| `/api/jobs/trigger` | POST | `businessDate` (required), `inputDir` (optional) | Inserts a QUEUED job row; returns `{jobId, status, businessDate, inputDir}` |
| `/api/jobs/stop` | POST | `jobId` (optional, if absent stops all) | Calls `forceStop(jobId)` or `forceStopAll()` |
| `/api/jobs/restart` | POST | `jobId` (required) | Resets a STOPPED/FAILED job to QUEUED |
| `/api/jobs/status` | GET | `jobId` (required) | Returns `{jobId, status}` |

**Security note** (from the code comment):
> "not yet authenticated — security is a later phase"

These endpoints are **unauthenticated**. In production, the `/api/jobs/stop` endpoint could
be used to halt running ingestion by any HTTP client with network access.

### 15.6 `FileController.java`

| Endpoint | Method | Parameters | Action |
|----------|--------|-----------|--------|
| `/api/files/skip` | POST | `fileName`, `reason` | Marks a file as `SKIPPED` in the audit table |

Useful when an operator knows a specific file is corrupt or irrelevant and wants to prevent it
from being ingested on subsequent runs.

---

## 16. End-to-End Workflow Summary

```
Operator / Scheduler
  │
  ├─► POST /api/jobs/trigger?businessDate=20260425
  │
  └─► lseg_jobs row: status=QUEUED
        │
        ▼ (within 1 second)
  JobWorker.pollAndExecute() claims job
        │ (status → RUNNING)
        ▼
  IngestOrchestrator.run(jobId)
    │
    ├─ Acquire ClusterLock (MariaDB GET_LOCK)
    ├─ Start heartbeat thread (every 30s)
    ├─ Read businessDate + inputDir from lseg_jobs
    │
    ├─ FileScanner.scan()
    │   └─ List files in inputDir
    │   └─ Apply skip patterns
    │   └─ Regex-classify → IngestFile objects
    │
    ├─ Load already-SUCCESS file names from lseg_file_audit (last 1 month)
    ├─ Filter out already-done files
    │
    ├─ FileSanityCheck for each remaining file
    │   └─ Open zip, read first 50 lines
    │   └─ Verify: metadata present, kind matches, date matches, key columns present
    │   └─ Failures: audit.markSkippedSanity()
    │
    ├─ Build IngestPlan (sorts DELTA by seq)
    │
    └─ For each Target (ORGS, ASSETS, QUOTES) in parallel (up to 3 threads):
         │
         ├─ INT Phase (parallel, up to 10 threads per target):
         │   For each INT file:
         │   └─ FileIngestor.ingest()
         │       ├─ Open ZipLineReader + PipeFileParser
         │       ├─ audit.markStarted()
         │       ├─ Intersect file columns with TargetSchema
         │       ├─ Pre-compute index arrays
         │       ├─ Get DB connection, disable autoCommit
         │       ├─ Create ResilientBatchExecutor (upsert + delete)
         │       ├─ Row loop:
         │       │   ├─ Poll stop signal every 5000 rows
         │       │   ├─ Apply RIC caret filter (INT QUOTES only)
         │       │   ├─ Extract action (I/U → upsert, D → delete)
         │       │   └─ Flush-on-flip when action side changes
         │       ├─ Final flush + conn.commit()
         │       ├─ Emit Micrometer metrics
         │       ├─ audit.markFinished(SUCCESS)
         │       └─ archive file to archiveDir
         │
         └─ DELTA Phase (sequential, seq order):
             For each DELTA file in seq order:
             └─ (same FileIngestor.ingest() flow as above)
        │
  Job marked COMPLETED
  ClusterLock released
  Heartbeat thread stopped
```

---

## 17. Key Assumptions & Constraints

### File naming

- Files **must** match the regex pattern exactly. New feed types or naming changes will be
  silently ignored until `FileScanner.mapTarget()` is updated.
- The `.note.txt.zip` files are always skipped; they contain human-readable metadata, not data.

### Ordering

- DELTA files are applied in ascending `seq` number order. A missing seq (e.g. seq 5 is absent)
  is NOT detected — the remaining files are applied in the gaps. If the feed sends seq 1, 2, 4
  and seq 3 is missing, the application will not flag it.

### Business date

- `businessDate` is stored per job (in `lseg_jobs.business_date`) and validated against the
  file's embedded metadata date. Files with a different date are sanity-failed.
- The date format is `yyyyMMdd` (e.g. `20260425`). No other format is supported.

### Transaction scope

- Each **file** is processed within its own JDBC transaction. If the file fails mid-way,
  only that file's changes are rolled back — not the entire job.
- This means partial file failures are possible if the DB accepts some batches before an error.
  However, because `conn.setAutoCommit(false)` and `conn.rollback()` are used, the entire file
  is either fully committed or fully rolled back.

### Soft-delete semantics

- `D` action sets `is_deleted = 1`, not a physical DELETE. Rows are never removed.
- A subsequent `I` or `U` for the same key resets `is_deleted = 0` via the
  `ON DUPLICATE KEY UPDATE ... is_deleted=0` clause.

### Null key values

- If a unique-key column (e.g. `entity_id`) is null in a row, MariaDB treats it as distinct —
  multiple null-keyed rows can coexist. The `D` action for a null-keyed row would be a no-op
  (`WHERE entity_id = NULL` never matches).

### Column-schema intersection

- The loaded column set is the intersection of file columns and `TargetSchema`. If the file
  provides a column not in `TargetSchema`, it is ignored. If `TargetSchema` defines a column
  absent from the file, that column is not in the INSERT statement (DB defaults apply).
- All `VARCHAR(255)` columns have no length validation before the INSERT. A value longer than
  255 chars will cause a JDBC exception, which triggers the row-by-row fallback.

### Cluster safety

- Only **one** node can process files at a time (enforced by `GET_LOCK`). This is a deliberate
  single-writer design to avoid concurrent upsert conflicts.
- The `JobWorker` polls every 1 second, which means there can be up to a 1-second delay between
  a job being queued and it being picked up.

---

## 18. Known Issues / Review Findings

| # | Severity | Location | Issue |
|---|----------|----------|-------|
| 1 | **SECURITY** | `JobController.java` | All job control endpoints (`/trigger`, `/stop`, `/restart`) are **unauthenticated**. Any HTTP client can queue, stop, or restart jobs. Must add authentication before production deployment. |
| 2 | **BUG (minor)** | `FileAuditDao.loadSuccessFileNames()` line 31 | SQL string concatenates `AUDIT_SUCCESS` constant directly. Although the constant is not user-supplied, using a parameterized query (or `?`) would be more consistent and future-proof. |
| 3 | **LOGIC** | `IngestOrchestrator.run()` line 94–97 | `props.setInputDir(jobDir)` mutates the shared `IngestProperties` bean. If two jobs run concurrently on the same JVM instance (not expected with the cluster lock, but possible in tests), this would cause a data race. The `inputDir` should be passed as a method parameter rather than mutating shared state. |
| 4 | **MISSING CHECK** | `FileScanner.mapTarget()` | A completely unknown dataset name returns `null` silently. There is no metric or warning counter for unrecognized datasets. Operators would only notice via the `Scanned N files` log message showing fewer files than expected. |
| 5 | **ASSUMPTION** | `IngestPlan` / DELTA ordering | Missing sequence numbers in the DELTA feed are not detected. If seq 3 of 5 is missing, the job proceeds silently. |
| 6 | **MISSING ENFORCEMENT** | `IngestProperties` | No `@Validated` annotations. Zero or negative values for thread counts, batch sizes, etc. can be set via config without causing startup errors — they will cause runtime failures instead. |
| 7 | **DATA RISK** | `TargetSchema` — all columns `VARCHAR(255)` | Fields like `ipo_listing_date`, `expiration_date` are stored as strings. No date/format validation occurs. Applications consuming the DB must handle any string that arrived from LSEG. |
| 8 | **PERFORMANCE** | `FileAuditDao.loadSuccessFileNames()` | Called once per job. For directories with thousands of files from multiple months, the `INTERVAL 1 MONTH` window could return a large result set loaded entirely into a `HashSet` in memory. |
| 9 | **RACE (theoretical)** | `JobDao.claimJob()` | The SELECT then UPDATE is not a single atomic statement. Under extreme concurrency, two nodes could SELECT the same row simultaneously. The UPDATE's `AND status='QUEUED'` guard prevents double-claiming, but the SELECT becomes stale — the claiming node just misses that job in that poll cycle. Functionally safe but generates extra no-op polls under high concurrency. |
| 10 | **FILE HANDLING** | `IngestOrchestrator.archive()` | Uses `REPLACE_EXISTING` — silently overwrites an existing archive file. If the archive already has a file with the same name (from a different run), that copy is lost without warning. |
| 11 | **STOP SIGNAL** | `FileIngestor.doIngest()` | The stop signal is only polled every `checkRows` (default 5000) rows. For files with very wide rows, this could mean up to 5000 × 10KB = 50 MB of work between stop checks. Reducing `checkRows` improves responsiveness. |
| 12 | **CHARSET** | `ZipLineReader` default constructor | Uses `StandardCharsets.UTF_8` hardcoded. If the charset config changes at runtime, the default constructor (used in `FileSanityCheck`) may not respect the change. `FileSanityCheck` should use the configured charset. |
