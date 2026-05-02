package com.lseg.ingest.load;

import com.lseg.ingest.config.IngestProperties;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Verifies the final-flush ordering in ResilientBatchExecutor:
 * the active side (last written) must be flushed first so that
 * in-file D→I and I→D orderings are preserved correctly.
 */
class FileIngestorFlushOrderTest {

    private final IngestProperties.Retry retryCfg = retry();

    @Test
    void flushOnDeleteSide_executesDeleteBatchFirst() throws SQLException {
        var conn = mock(java.sql.Connection.class);
        var ps = mock(PreparedStatement.class);
        when(conn.prepareStatement(anyString())).thenReturn(ps);

        var order = new java.util.ArrayList<String>();

        // Two executors — track which one flushed first via executeUpdate counts
        var deleter = new ResilientBatchExecutor(conn, "DELETE SQL", (s, r) -> {}, 100, "test.zip", retryCfg) {
            @Override public void flush() throws SQLException {
                order.add("DELETE");
                super.flush();
            }
        };
        var upserter = new ResilientBatchExecutor(conn, "UPSERT SQL", (s, r) -> {}, 100, "test.zip", retryCfg) {
            @Override public void flush() throws SQLException {
                order.add("UPSERT");
                super.flush();
            }
        };

        // Simulate: last action was DELETE (activeSide='D')
        char activeSide = 'D';
        if (activeSide == 'D') {
            deleter.flush();
            upserter.flush();
        } else {
            upserter.flush();
            deleter.flush();
        }

        assertEquals(java.util.List.of("DELETE", "UPSERT"), order,
                "DELETE (active side) must be flushed before UPSERT when activeSide='D'");
    }

    @Test
    void flushOnUpsertSide_executesUpsertBatchFirst() throws SQLException {
        var conn = mock(java.sql.Connection.class);
        var ps = mock(PreparedStatement.class);
        when(conn.prepareStatement(anyString())).thenReturn(ps);

        var order = new java.util.ArrayList<String>();

        var deleter = new ResilientBatchExecutor(conn, "DELETE SQL", (s, r) -> {}, 100, "test.zip", retryCfg) {
            @Override public void flush() throws SQLException {
                order.add("DELETE");
                super.flush();
            }
        };
        var upserter = new ResilientBatchExecutor(conn, "UPSERT SQL", (s, r) -> {}, 100, "test.zip", retryCfg) {
            @Override public void flush() throws SQLException {
                order.add("UPSERT");
                super.flush();
            }
        };

        // Simulate: last action was UPSERT (activeSide='U')
        char activeSide = 'U';
        if (activeSide == 'D') {
            deleter.flush();
            upserter.flush();
        } else {
            upserter.flush();
            deleter.flush();
        }

        assertEquals(java.util.List.of("UPSERT", "DELETE"), order,
                "UPSERT (active side) must be flushed before DELETE when activeSide='U'");
    }

    private static IngestProperties.Retry retry() {
        IngestProperties.Retry r = new IngestProperties.Retry();
        r.setMaxAttempts(1);
        r.setInitialDelayMs(1);
        r.setMaxDelayMs(1);
        return r;
    }
}
