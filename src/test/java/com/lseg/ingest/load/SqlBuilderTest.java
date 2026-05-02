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
        // Both asset_id and quote_id are key columns — neither should appear in ON DUPLICATE clause.
        assertFalse(dupClause.contains("quote_id=VALUES(quote_id)"));
        assertFalse(dupClause.contains("asset_id=VALUES(asset_id)"));
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

    @Test
    void upsertPricingUsesConditionalIfAndTradeDateLast() {
        var cols = TargetSchema.intersect(Target.PRICING,
                Set.of("Quote_ID", "Trade_Date", "Close_Price", "Ask_Price"));
        String sql = SqlBuilder.upsert(Target.PRICING, cols);

        assertTrue(sql.contains("INSERT INTO lseg_pricing"));
        assertTrue(sql.contains("ON DUPLICATE KEY UPDATE"));
        String dupClause = sql.substring(sql.indexOf("ON DUPLICATE"));

        // quote_id is a key, shouldn't be in UPDATE clause
        assertFalse(dupClause.contains("quote_id = IF"));
        assertFalse(dupClause.contains("quote_id=VALUES(quote_id)"));

        // trade_date must be last and use IF
        assertTrue(dupClause.endsWith("trade_date = IF(VALUES(trade_date) >= trade_date, VALUES(trade_date), trade_date)"));

        // other columns use IF
        assertTrue(dupClause.contains("close_price = IF(VALUES(trade_date) >= trade_date, VALUES(close_price), close_price)"));
        assertTrue(dupClause.contains("ask_price = IF(VALUES(trade_date) >= trade_date, VALUES(ask_price), ask_price)"));
        assertTrue(dupClause.contains("is_deleted = IF(VALUES(trade_date) >= trade_date, 0, is_deleted)"));
    }
}
