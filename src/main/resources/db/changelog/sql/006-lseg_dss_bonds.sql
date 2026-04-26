-- liquibase formatted sql

-- changeset lseg-ingest:006-create-lseg_dss_bonds
CREATE TABLE IF NOT EXISTS lseg_dss_bonds (
    id BIGINT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    isin VARCHAR(255),
    instrument_id VARCHAR(255),
    instrument_id_type VARCHAR(255),
    ric VARCHAR(255),
    ticker VARCHAR(255),
    security_description VARCHAR(255),
    instrument_full_name_esma VARCHAR(255),
    security_source VARCHAR(255),
    asset_id VARCHAR(255),
    asset_type VARCHAR(255),
    asset_type_description VARCHAR(255),
    currency_code VARCHAR(255),
    issuer_name VARCHAR(255),
    issuer_lei VARCHAR(255),
    issuer_short_name VARCHAR(255),
    is_deleted TINYINT(1) NOT NULL DEFAULT 0,
    UNIQUE KEY uniq_dss_bonds_key (isin, instrument_id, instrument_id_type, ric),
    KEY idx_dss_bonds_ric (ric),
    KEY idx_dss_bonds_is_deleted (is_deleted)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- rollback DROP TABLE lseg_dss_bonds;
