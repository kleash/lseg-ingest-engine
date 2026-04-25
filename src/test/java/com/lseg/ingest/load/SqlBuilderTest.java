package com.lseg.ingest.load;

import com.lseg.ingest.plan.Target;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SqlBuilderTest {

    @Test
    void upsertContainsIntersectedColumnsAndOnDuplicateUpdatesNonKeyColumns() {
        var cols = TargetSchema.intersect(Target.QUOTES,
                Set.of("Quote_Perm_ID", "RIC", "Ticker", "Currency_Code"));
        String sql = SqlBuilder.upsert(Target.QUOTES, cols);
        assertTrue(sql.startsWith("INSERT INTO lseg_quotes ("));
        assertTrue(sql.contains("quote_perm_id"));
        assertTrue(sql.contains("ric"));
        assertTrue(sql.contains("ON DUPLICATE KEY UPDATE"));
        // Key column must NOT appear in the ON DUPLICATE clause.
        String dupClause = sql.substring(sql.indexOf("ON DUPLICATE"));
        assertFalse(dupClause.contains("quote_perm_id=VALUES(quote_perm_id)"));
        assertTrue(dupClause.contains("ric=VALUES(ric)"));
        assertTrue(dupClause.contains("is_deleted=0"));
    }

    @Test
    void deleteIsKeyedOnPermIdColumn() {
        assertEquals("UPDATE lseg_orgs SET is_deleted = 1 WHERE entity_perm_id = ?", SqlBuilder.delete(Target.ORGS));
        assertEquals("UPDATE lseg_assets SET is_deleted = 1 WHERE issue_perm_id = ?", SqlBuilder.delete(Target.ASSETS));
        assertEquals("UPDATE lseg_quotes SET is_deleted = 1 WHERE quote_perm_id = ?", SqlBuilder.delete(Target.QUOTES));
    }
}
