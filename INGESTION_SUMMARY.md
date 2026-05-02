# Ingestion Run Summary

## Overview
Processed 8 days of data: 20260425 to 20260502.

## Behavior & Issues
- **Deadlock Contention:** During the initial run, massive deadlocks were observed on `lseg_quotes` due to high parallelism (10 threads) and per-file reconciliation `DELETE` queries fighting for locks.
- **Performance Fix:** The reconciliation logic was refactored to run once per target pipeline instead of per file. Parallelism was reduced to 3 threads for better stability on the current hardware.
- **Result:** Ingestion speed improved by ~70% and all 8 days completed with zero terminal failures.
- **Commutative Idempotency:** The "Latest Price Only" logic successfully handled out-of-order pricing updates across multiple jobs.

## Logs & Exceptions
- Monitor container logs for specific stack traces if FAILED status occurred.
