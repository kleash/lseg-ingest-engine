-- liquibase formatted sql

-- changeset lseg-ingest:001-create-lseg_orgs
CREATE TABLE IF NOT EXISTS lseg_orgs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    company_legal_domicile VARCHAR(255),
    company_short_name VARCHAR(255),
    country_of_incorporation VARCHAR(255),
    dow_jones_industrial_code VARCHAR(255),
    entity_id VARCHAR(255),
    entity_perm_id VARCHAR(255),
    finsbury_company_code VARCHAR(255),
    gics_industry_code VARCHAR(255),
    icb_code VARCHAR(255),
    icb_code_2019 VARCHAR(255),
    issuer_lei VARCHAR(255),
    issuer_name VARCHAR(255),
    issuer_orgid VARCHAR(255),
    level VARCHAR(255),
    organization_sub_type VARCHAR(255),
    organization_type VARCHAR(255),
    reuters_editorial_ric VARCHAR(255),
    sicc_sector_code VARCHAR(255),
    subscription_id VARCHAR(255),
    trbc_code VARCHAR(255),
    asset_id VARCHAR(255),
    issue_perm_id VARCHAR(255),
    quote_id VARCHAR(255),
    quote_perm_id VARCHAR(255),
    is_deleted TINYINT(1) NOT NULL DEFAULT 0,
    UNIQUE KEY uniq_orgs_entity_id (entity_id),
    KEY idx_orgs_entity_perm_id (entity_perm_id),
    KEY idx_orgs_is_deleted (is_deleted)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- rollback DROP TABLE lseg_orgs;
