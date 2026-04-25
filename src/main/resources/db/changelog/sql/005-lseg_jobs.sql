-- liquibase formatted sql
-- changeset sa:5
CREATE TABLE IF NOT EXISTS lseg_jobs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    status VARCHAR(20) NOT NULL,                    -- QUEUED, RUNNING, COMPLETED, STOPPED, FAILED
    node_id VARCHAR(100),
    business_date VARCHAR(8),                       -- yyyymmdd carried per job; populated on queue
    input_dir VARCHAR(512),                         -- carried per job; populated on queue
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP NULL,
    finished_at TIMESTAMP NULL,
    last_heartbeat_at TIMESTAMP NULL,
    error_message TEXT,
    KEY idx_jobs_status (status),
    KEY idx_jobs_heartbeat (last_heartbeat_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
