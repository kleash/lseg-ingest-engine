-- liquibase formatted sql
-- changeset lseg-ingest:004-create-lseg_file_audit
CREATE TABLE IF NOT EXISTS lseg_file_audit (
    id BIGINT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    dataset VARCHAR(64),
    target_table VARCHAR(16),
    kind VARCHAR(8),
    seq INT,
    business_date DATE,
    declared_rows INT,
    parsed_rows INT,
    inserted_rows INT,
    skipped_rows INT,
    ins_count INT DEFAULT 0,
    upd_count INT DEFAULT 0,
    del_count INT DEFAULT 0,
    status VARCHAR(16),
    error_message TEXT,
    started_at TIMESTAMP(3) NULL,
    finished_at TIMESTAMP(3) NULL,
    UNIQUE KEY uniq_file_audit_file_name (file_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- rollback DROP TABLE lseg_file_audit;
