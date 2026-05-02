# Changelog

All notable changes to the LSEG Ingestion project will be documented in this file.

## [1.1.0] - 2026-05-02

### Resilience & Correctness Fixes
- **Business date age guard**: Jobs whose `business_date` is older than `ingest.maxBusinessDateAgeDays` (default 30) now fail fast before any file scanning, preventing accidental processing of stale feeds.
- **Bounded audit lookback**: `loadSuccessFileNames` now accepts a `lookbackDays` parameter and filters by `business_date >= CURDATE() - INTERVAL N DAY` (configured via `ingest.auditLookbackDays`, default 60). Eliminates unbounded full-table scans as audit history grows.
- **Reconciliation deadlock fix**: The NULL-asset deduplication `DELETE` in `FileIngestor` is now wrapped in `SqlRetry`, preventing entire-file rollbacks when parallel quote ingestion triggers a MariaDB deadlock.
- **Final flush order corrected**: The end-of-file batch flush in `FileIngestor` now flushes the active side first (`deleter` when last action was DELETE, `upserter` otherwise), preserving in-file action ordering semantics.
- **Dead `skipped` variable removed**: The always-zero `skipped` counter in `FileIngestor` was removed; error skip counts now read correctly from `upserter.skipped() + deleter.skipped()` in both success and failure audit paths.
- **`perTargetPool` shutdown moved to `finally`**: The per-job target thread pool is now shut down in a `finally` block, preventing thread leaks when exceptions occur mid-run.

### API Fixes
- **`restart()` now works for STOPPED jobs**: `POST /api/jobs/restart` previously used `updateStatus()` which silently no-ops for STOPPED jobs. It now calls `forceRequeue()` (a new unconditional UPDATE) for STOPPED jobs and returns the actual resulting DB status instead of a hardcoded `QUEUED` claim.

### Code Quality
- **`FileIngestor.ingestWithParser()` decomposed**: The 200-line god method is split into `buildColumnMapping()`, `processRows()`, and `reconcileNullAssets()` private methods, each with a focused responsibility.
- **SQL status literals**: Concatenated Java constant strings in `JobDao` and `FileAuditDao` SQL queries replaced with literal values (`'QUEUED'`, `'STARTED'`, etc.) for clarity.
- **`Resilience` config block removed**: `IngestProperties.Resilience` (containing never-wired `fallbackOnBatchFail` and `maxSkippedRowsPerFile`) was removed entirely to eliminate dead configuration.
- **PRICING Kind.INT documented**: Added comment in `FileScanner.classify()` explaining that PRICING files are always `Kind.INT` by design.

### Configuration Changes
| Key | Default | Description |
|---|---|---|
| `ingest.auditLookbackDays` | `60` | Window (days) for idempotency audit lookback |
| `ingest.maxBusinessDateAgeDays` | `30` | Maximum allowed age of a job's business date |

Removed: `ingest.resilience.fallbackOnBatchFail`, `ingest.resilience.maxSkippedRowsPerFile`

---

### Previous 1.1.0 Changes

### Added
- **Pricing Ingestion Support**:
    - Full support for `EIS_INT_*_PRICING` data files (`PRC` kind).
    - Asynchronous Phase 2 Ingestion: Pricing files are processed in a dedicated, application-scoped executor thread pool to avoid blocking reference data updates.
    - Conditional Upsert: Implemented `IF(VALUES(trade_date) >= trade_date, ...)` logic in `SqlBuilder` to ensure only the latest price is retained in the database.
    - 17-column `lseg_pricing` schema with optimized indexing on `quote_id` and `trade_date`.
- **Target Completion Events**:
    - Introduced `TargetIngestCompletedEvent` published via Spring's `ApplicationEventPublisher`.
    - Enables downstream listeners to react to the completion of specific targets (e.g., triggering analytics after PRICING is ready).
- **Hardened Persistence**:
    - Added Docker volume support (`mariadb_data`) to `docker-compose.yml` ensuring database persistence across container restarts.

### Changed
- **Quotes Deduplication (Asset Priority Strategy)**:
    - Refactored `lseg_quotes` to prioritize records with valid `asset_id` over `NULL` asset records.
    - Added a virtual column `asset_id_v` to handle `NULL` uniqueness, preventing multiple "anonymous" records per quote.
    - Implemented post-ingestion reconciliation logic in `FileIngestor` to automatically purge `NULL` records when a higher-quality anchored record arrives.
    - Preserved legitimate multi-asset mappings (where one quote belongs to multiple distinct assets).
- **Architectural Refinement**:
    - Narrowed `lseg_pricing` unique index to `quote_id` to strictly enforce the "Latest Price Only" rule and prevent historical data bloat.
    - Updated `GEMINI.md` architectural map with new data integrity and priority invariants.

### Fixed
- **PipeFileParser PRC Support**: Fixed metadata parsing errors where `PRC` kind was rejected and row counts were read from the wrong index.
- **FileSanityCheck Bypass**: Exempted PRICING files from standard `INT`/`REF` kind validation while maintaining business-date and header checks.
- **Docker Environment**: Fixed a critical issue where database data was lost on restart due to missing volumes.

## [1.0.0] - 2026-04-25
- Initial release with support for ORGS, ASSETS, QUOTES, and DSS_BONDS.
- Multi-instance cluster locking via MariaDB `GET_LOCK`.
- Resilient batch execution with row-by-row fallback.
