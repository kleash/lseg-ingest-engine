package com.lseg.ingest.plan;

import java.util.List;

/**
 * Per-target metadata: table name, the unique-key columns used by ON DUPLICATE KEY UPDATE
 * and DELETE WHERE, and the matching source-header names used to find those columns in
 * the LSEG file.
 *
 * Unique keys are composite to match the table-side UNIQUE constraints:
 *   ORGS    : entity_id
 *   ASSETS  : asset_id
 *   QUOTES  : (asset_id, quote_id)
 *
 * Rows with NULL values in any unique-key column are still inserted; MariaDB's UNIQUE
 * constraint treats NULLs as distinct, so duplicates are tolerated by design.
 */
public enum Target {
    ORGS("lseg_orgs",
            List.of("entity_id"),
            List.of("Entity_ID")),
    ASSETS("lseg_assets",
            List.of("asset_id"),
            List.of("Asset_ID")),
    QUOTES("lseg_quotes",
            List.of("asset_id", "quote_id"),
            List.of("Asset_ID", "Quote_ID"));

    public final String table;
    public final List<String> uniqueKeyColumns;        // db column names
    public final List<String> uniqueKeySourceHeaders;  // file header column names, same order

    Target(String table, List<String> uniqueKeyColumns, List<String> uniqueKeySourceHeaders) {
        if (uniqueKeyColumns.size() != uniqueKeySourceHeaders.size()) {
            throw new IllegalArgumentException("uniqueKeyColumns and uniqueKeySourceHeaders must align");
        }
        this.table = table;
        this.uniqueKeyColumns = uniqueKeyColumns;
        this.uniqueKeySourceHeaders = uniqueKeySourceHeaders;
    }
}
