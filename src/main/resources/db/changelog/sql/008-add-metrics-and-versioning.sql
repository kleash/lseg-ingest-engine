-- liquibase formatted sql

-- changeset lseg-ingest:008-add-metrics-and-versioning
ALTER TABLE lseg_file_audit ADD COLUMN updated_rows INT DEFAULT 0 AFTER inserted_rows;
ALTER TABLE lseg_file_audit ADD COLUMN unchanged_rows INT DEFAULT 0 AFTER updated_rows;
