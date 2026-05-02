# LSEG Quotes Refactor — Unique Key & NULL Asset Handling

## Problem Statement

The `lseg_quotes` table initially used a loose composite unique key on `(asset_id, quote_id)`. 
Because MariaDB treats `NULL` values as distinct, multiple rows with the same `quote_id` could accumulate if their `asset_id` was `NULL`. 

While `quote_id` is mostly unique, there are legitimate cases (~13 in current datasets) where a single `quote_id` maps to multiple distinct `asset_id` values. Simply narrowing the index to `quote_id` would cause data loss for these records.

However, the accumulation of redundant `NULL` asset records alongside "anchored" (non-NULL) asset records for the same quote is a data quality issue.

## Solution: Asset Priority Strategy

We have implemented a hybrid approach that preserves legitimate multi-asset mappings while strictly enforcing the priority of anchored records over NULL ones.

### 1. Unique NULL Prevention (Schema Level)
We added a virtual column `asset_id_v` that treats `NULL` as an empty string for uniqueness purposes.
- **Index**: `UNIQUE KEY uniq_quotes_quote_id_asset_v (quote_id, asset_id_v)`
- **Result**: A single `quote_id` can have one `NULL` entry and multiple *distinct* non-NULL entries. Multiple `NULL` entries for the same `quote_id` are now prevented and will trigger an `UPDATE`.

### 2. NULL yielding to Asset (Application Level)
In `FileIngestor.java`, a post-ingestion reconciliation step is executed after each file batch:
```sql
DELETE q1 FROM lseg_quotes q1 
JOIN lseg_quotes q2 ON q1.quote_id = q2.quote_id 
WHERE q1.asset_id IS NULL AND q2.asset_id IS NOT NULL;
```
- **Scenario A**: If a quote has a `NULL` asset record and a new record with an `asset_id` arrives, the `NULL` record is deleted.
- **Scenario B**: If a quote already has an anchored record, any incoming `NULL` records for that quote are ignored (deleted immediately after ingestion).

## Implementation Details

### 1. Database Migrations
- **009-fix-quotes-null-duplicates.sql**: 
    - Deduplicates existing redundant NULLs.
    - Adds the `asset_id_v` virtual column.
    - Replaces the loose index with the strict `(quote_id, asset_id_v)` index.

### 2. Java Code
- **FileIngestor.java**: Added the `11b. Reconciliation` block to execute the deletion of "shadowed" NULL records.
- **Target.java**: Maintains `QUOTES` with `(asset_id, quote_id)` unique keys to ensure the `UPSERT` logic correctly identifies the separate identities before the reconciliation step.

## Impact
- **Data Integrity**: Prioritizes higher-quality (anchored) metadata while tolerating valid multi-asset mappings.
- **Consistency**: Handles the `NULL -> Asset` transition as a logical replacement.
- **Efficiency**: Uses set-based SQL for reconciliation to maintain high ingestion throughput.
