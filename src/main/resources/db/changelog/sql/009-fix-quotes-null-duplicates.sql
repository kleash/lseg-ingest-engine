-- liquibase formatted sql

-- changeset lseg-ingest:009-fix-quotes-null-duplicates runOnChange:false
-- Description: Prevent multiple NULL asset_ids for the same quote using a virtual column.

-- 1. Deduplicate: If multiple rows have NULL asset_id for the same quote_id, keep only the latest.
DELETE q1 FROM lseg_quotes q1
JOIN (
    SELECT quote_id, MAX(id) as max_id
    FROM lseg_quotes
    WHERE asset_id IS NULL
    GROUP BY quote_id
    HAVING COUNT(*) > 1
) q2 ON q1.quote_id = q2.quote_id
WHERE q1.asset_id IS NULL AND q1.id < q2.max_id;

-- 2. Add virtual column to treat NULL as an empty string for uniqueness purposes.
ALTER TABLE lseg_quotes ADD COLUMN asset_id_v
    VARCHAR(255) AS (IFNULL(asset_id, '')) VIRTUAL;

-- 3. Replace the loose composite index with a strict one using the virtual column.
DROP INDEX IF EXISTS uniq_quotes_asset_id_quote_id ON lseg_quotes;
ALTER TABLE lseg_quotes ADD UNIQUE KEY uniq_quotes_quote_id_asset_v (quote_id, asset_id_v);
