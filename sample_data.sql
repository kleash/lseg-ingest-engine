-- Sample some rows from each table to verify data looks sane
SELECT 'Sample Orgs' as msg;
SELECT entity_id, entity_perm_id, issuer_name, country_of_incorporation FROM lseg_orgs LIMIT 5;

SELECT 'Sample Assets' as msg;
SELECT asset_id, issue_perm_id, security_long_description, isin FROM lseg_assets LIMIT 5;

SELECT 'Sample Quotes' as msg;
SELECT quote_id, quote_perm_id, ric, ticker, currency_code FROM lseg_quotes LIMIT 5;

-- Verify column types are indeed VARCHAR/TEXT as requested
SELECT table_name, column_name, data_type, character_maximum_length
FROM information_schema.columns
WHERE table_schema = 'lseg'
  AND table_name IN ('lseg_orgs', 'lseg_assets', 'lseg_quotes')
  AND column_name NOT IN ('id')
LIMIT 10;
