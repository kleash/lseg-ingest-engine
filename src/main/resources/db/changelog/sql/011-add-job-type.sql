-- liquibase formatted sql

-- changeset sa:11-add-job-type
-- Job type distinguishes the main ingestion run (MAIN) from the isolated delisted run (DELISTED).
-- Defaulting to MAIN keeps all pre-existing job rows valid.
ALTER TABLE lseg_jobs ADD COLUMN job_type VARCHAR(32) NOT NULL DEFAULT 'MAIN';
-- rollback ALTER TABLE lseg_jobs DROP COLUMN job_type;
