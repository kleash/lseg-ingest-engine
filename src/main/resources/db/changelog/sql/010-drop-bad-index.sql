-- liquibase formatted sql

-- changeset lseg-ingest:010-drop-incorrect-quotes-index
-- Description: Drop the accidental single-column unique index on quote_id that blocks multi-asset mappings.

DROP INDEX IF EXISTS uniq_quotes_quote_id ON lseg_quotes;
