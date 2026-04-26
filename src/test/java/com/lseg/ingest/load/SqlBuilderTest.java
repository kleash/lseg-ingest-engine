package com.lseg.ingest.load;

import com.lseg.ingest.plan.Target;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SqlBuilderTest {

    @Test
    void upsertContainsIntersectedColumnsAndOnDuplicateUpdatesNonKeyColumns() {
        var cols = TargetSchema.intersect(Target.QUOTES,
                Set.of("Asset_ID", "Quote_ID", "RIC", "Ticker", "Currency_Code"));
        String sql = SqlBuilder.upsert(Target.QUOTES, cols);
        assertTrue(sql.startsWith("INSERT INTO lseg_quotes ("));
        assertTrue(sql.contains("asset_id"));
        assertTrue(sql.contains("quote_id"));
        assertTrue(sql.contains("ric"));
        assertTrue(sql.contains("ON DUPLICATE KEY UPDATE"));
        String dupClause = sql.substring(sql.indexOf("ON DUPLICATE"));
        // Composite key columns must NOT appear in ON DUPLICATE clause.
        assertFalse(dupClause.contains("asset_id=VALUES(asset_id)"));
        assertFalse(dupClause.contains("quote_id=VALUES(quote_id)"));
        assertTrue(dupClause.contains("ric=VALUES(ric)"));
        assertTrue(dupClause.contains("is_deleted=0"));
    }

    @Test
    void deleteIsKeyedOnUniqueKeyColumns() {
        assertEquals("UPDATE lseg_orgs SET is_deleted = 1 WHERE entity_id = ?", SqlBuilder.delete(Target.ORGS));
        assertEquals("UPDATE lseg_assets SET is_deleted = 1 WHERE asset_id = ?", SqlBuilder.delete(Target.ASSETS));
        assertEquals("UPDATE lseg_quotes SET is_deleted = 1 WHERE asset_id = ? AND quote_id = ?", SqlBuilder.delete(Target.QUOTES));
        assertEquals("UPDATE lseg_dss_bonds SET is_deleted = 1 WHERE isin = ? AND instrument_id = ? AND instrument_id_type = ? AND ric = ?", SqlBuilder.delete(Target.DSS_BONDS));
    }

    @Test
    void upsertDssBonds() {
        var cols = TargetSchema.intersect(Target.DSS_BONDS,
                Set.of("ISIN", "Instrument ID", "Instrument ID Type", "RIC", "Ticker"));
        String sql = SqlBuilder.upsert(Target.DSS_BONDS, cols);
        assertTrue(sql.contains("INSERT INTO lseg_dss_bonds"));
        assertTrue(sql.contains("ON DUPLICATE KEY UPDATE"));
        String dupClause = sql.substring(sql.indexOf("ON DUPLICATE"));
        assertFalse(dupClause.contains("isin=VALUES(isin)"));
        assertTrue(dupClause.contains("ticker=VALUES(ticker)"));
    }
}
