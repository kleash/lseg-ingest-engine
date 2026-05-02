-- liquibase formatted sql

-- changeset lseg-ingest:007-create-lseg_pricing
CREATE TABLE IF NOT EXISTS lseg_pricing (
    id                          BIGINT AUTO_INCREMENT PRIMARY KEY NOT NULL,
    quote_id                    VARCHAR(255),
    quote_perm_id               VARCHAR(255),
    trade_date                  VARCHAR(20),
    alternate_close_price       VARCHAR(255),
    ask_price                   VARCHAR(255),
    bid_price                   VARCHAR(255),
    close_auction_price         VARCHAR(255),
    close_auction_price_grade   VARCHAR(255),
    close_price                 VARCHAR(255),
    close_price_timestamp       VARCHAR(255),
    close_price_timestamp_grade VARCHAR(255),
    high_price                  VARCHAR(255),
    low_price                   VARCHAR(255),
    mid_price                   VARCHAR(255),
    offer_price                 VARCHAR(255),
    open_price                  VARCHAR(255),
    settlement_price            VARCHAR(255),
    is_deleted                  TINYINT(1) NOT NULL DEFAULT 0,
    UNIQUE KEY uniq_pricing_quote_id (quote_id),
    KEY idx_pricing_trade_date    (trade_date),
    KEY idx_pricing_quote_perm_id (quote_perm_id),
    KEY idx_pricing_is_deleted    (is_deleted)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
-- rollback DROP TABLE lseg_pricing;
