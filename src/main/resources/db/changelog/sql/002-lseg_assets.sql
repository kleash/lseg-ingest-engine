-- liquibase formatted sql

-- changeset lseg-ingest:002-create-lseg_assets
CREATE TABLE IF NOT EXISTS lseg_assets (
    id BIGINT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    asset_id VARCHAR(255),
    entity_id VARCHAR(255),
    entity_perm_id VARCHAR(255),
    issue_perm_id VARCHAR(255),
    level VARCHAR(255),
    quote_id VARCHAR(255),
    quote_perm_id VARCHAR(255),
    cusip VARCHAR(255),
    ipo_listing_date VARCHAR(255),
    isin VARCHAR(255),
    rcs_code VARCHAR(255),
    rights_allocated VARCHAR(255),
    security_long_description VARCHAR(255),
    settlement_type VARCHAR(255),
    is_deleted TINYINT(1) NOT NULL DEFAULT 0,
    UNIQUE KEY uniq_assets_asset_id (asset_id),
    KEY idx_assets_issue_perm_id (issue_perm_id),
    KEY idx_assets_is_deleted (is_deleted)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- rollback DROP TABLE lseg_assets;
