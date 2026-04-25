-- liquibase formatted sql

-- changeset lseg-ingest:003-create-lseg_quotes
CREATE TABLE IF NOT EXISTS lseg_quotes (
    id BIGINT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    asset_id VARCHAR(255),
    entity_id VARCHAR(255),
    entity_perm_id VARCHAR(255),
    issue_perm_id VARCHAR(255),
    level VARCHAR(255),
    quote_id VARCHAR(255),
    quote_perm_id VARCHAR(255),
    asset_category VARCHAR(255),
    currency_code VARCHAR(255),
    exchange_code VARCHAR(255),
    market_segment_mic VARCHAR(255),
    rcs_code VARCHAR(255),
    ric VARCHAR(255),
    round_lot_size VARCHAR(255),
    sedol VARCHAR(255),
    strike_price VARCHAR(255),
    strike_price_multiplier VARCHAR(255),
    ticker VARCHAR(255),
    trading_status VARCHAR(255),
    exercise_begin_date VARCHAR(255),
    expiration_date VARCHAR(255),
    security_description VARCHAR(255),
    warrant_issue_date VARCHAR(255),
    is_deleted TINYINT(1) NOT NULL DEFAULT 0,
    UNIQUE KEY uniq_quotes_asset_id_quote_id (asset_id, quote_id),
    KEY idx_quotes_quote_id (quote_id),
    KEY idx_quotes_asset_id (asset_id),
    KEY idx_quotes_quote_perm_id (quote_perm_id),
    KEY idx_quotes_is_deleted (is_deleted)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- rollback DROP TABLE lseg_quotes;
