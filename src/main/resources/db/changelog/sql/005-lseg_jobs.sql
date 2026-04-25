-- liquibase formatted sql
-- changeset sa:5
CREATE TABLE IF NOT EXISTS lseg_jobs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    status VARCHAR(20) NOT NULL, -- QUEUED, RUNNING, COMPLETED, STOPPED, FAILED
    node_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP NULL,
    finished_at TIMESTAMP NULL,
    error_message TEXT
);
