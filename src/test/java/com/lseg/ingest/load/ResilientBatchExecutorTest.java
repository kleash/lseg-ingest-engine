package com.lseg.ingest.load;

import com.lseg.ingest.config.IngestProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class ResilientBatchExecutorTest {

    private IngestProperties.Retry retryCfg;

    @BeforeEach
    void setUp() {
        retryCfg = new IngestProperties.Retry();
        retryCfg.setMaxAttempts(1); // No retries for basic success/failure tests
    }

    @Test
    void batchSucceedsOnNormalExecution() throws SQLException {
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        when(conn.prepareStatement(anyString())).thenReturn(ps);

        AtomicInteger bindCount = new AtomicInteger();
        ResilientBatchExecutor.RowBinder binder = (p, r) -> bindCount.incrementAndGet();

        try (ResilientBatchExecutor executor = new ResilientBatchExecutor(conn, "INSERT...", binder, 2, "test.zip", retryCfg)) {
            executor.add(new PendingRow(new String[]{"a"}, 1, "k1"));
            executor.add(new PendingRow(new String[]{"b"}, 2, "k2")); // should trigger flush
            
            assertEquals(2, bindCount.get());
            verify(ps, times(1)).executeBatch();
            assertEquals(2, executor.succeeded());
            assertEquals(0, executor.skipped());
        }
    }

    @Test
    void fallsBackToRowByRowOnBatchFailure() throws SQLException {
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        when(conn.prepareStatement(anyString())).thenReturn(ps);

        // First executeBatch fails
        when(ps.executeBatch()).thenThrow(new SQLException("Batch failed"));
        
        // During fallback:
        // row 1 succeeds
        // row 2 fails
        AtomicInteger updateCalls = new AtomicInteger();
        when(ps.executeUpdate()).thenAnswer(inv -> {
            if (updateCalls.incrementAndGet() == 2) throw new SQLException("Row 2 failed");
            return 1;
        });

        ResilientBatchExecutor.RowBinder binder = (p, r) -> {};

        try (ResilientBatchExecutor executor = new ResilientBatchExecutor(conn, "INSERT...", binder, 2, "test.zip", retryCfg)) {
            executor.add(new PendingRow(new String[]{"a"}, 1, "k1"));
            executor.add(new PendingRow(new String[]{"b"}, 2, "k2")); // triggers flush
            
            assertEquals(1, executor.succeeded());
            assertEquals(1, executor.skipped());
            
            verify(ps, times(1)).executeBatch();
            verify(ps, times(2)).executeUpdate();
            verify(ps, times(1)).clearBatch();
        }
    }

    @Test
    void retriesTransientBatchErrorBeforeFallback() throws Exception {
        Connection conn = mock(Connection.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        when(conn.prepareStatement(anyString())).thenReturn(ps);

        retryCfg.setMaxAttempts(2);
        retryCfg.setInitialDelayMs(1);

        // First attempt fails with transient error, second succeeds
        when(ps.executeBatch())
                .thenThrow(new SQLException("Deadlock", "40001", 1213))
                .thenReturn(new int[]{1, 1});

        ResilientBatchExecutor.RowBinder binder = (p, r) -> {};

        try (ResilientBatchExecutor executor = new ResilientBatchExecutor(conn, "INSERT...", binder, 2, "test.zip", retryCfg)) {
            executor.add(new PendingRow(new String[]{"a"}, 1, "k1"));
            executor.add(new PendingRow(new String[]{"b"}, 2, "k2")); // triggers flush
            
            assertEquals(2, executor.succeeded());
            verify(ps, times(2)).executeBatch();
            verify(ps, never()).executeUpdate(); // No fallback needed
        }
    }
}
