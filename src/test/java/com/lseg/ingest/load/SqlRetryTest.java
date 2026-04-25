package com.lseg.ingest.load;

import com.lseg.ingest.config.IngestProperties;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class SqlRetryTest {

    private IngestProperties.Retry cfg() {
        IngestProperties.Retry r = new IngestProperties.Retry();
        r.setMaxAttempts(3);
        r.setInitialDelayMs(1);
        r.setMaxDelayMs(2);
        return r;
    }

    @Test
    void transientErrorIsRetriedAndEventuallySucceeds() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        String result = SqlRetry.withRetry(cfg(), "test", () -> {
            int n = calls.incrementAndGet();
            if (n < 3) throw new SQLTransientConnectionException("transient");
            return "ok";
        });
        assertEquals("ok", result);
        assertEquals(3, calls.get());
    }

    @Test
    void deadlockIsTransient() {
        SQLException e = new SQLException("deadlock", "40001", 1213);
        assertTrue(SqlRetry.isTransient(e));
    }

    @Test
    void lockWaitTimeoutIsTransient() {
        SQLException e = new SQLException("lock wait", "HY000", 1205);
        assertTrue(SqlRetry.isTransient(e));
    }

    @Test
    void connectionStateIsTransient() {
        SQLException e = new SQLException("conn lost", "08S01");
        assertTrue(SqlRetry.isTransient(e));
    }

    @Test
    void uniqueConstraintIsNotTransient() {
        SQLException e = new SQLException("dup", "23000", 1062);
        assertFalse(SqlRetry.isTransient(e));
    }

    @Test
    void nonTransientPropagatesImmediately() {
        AtomicInteger calls = new AtomicInteger();
        assertThrows(SQLException.class, () -> SqlRetry.withRetry(cfg(), "test", () -> {
            calls.incrementAndGet();
            throw new SQLException("dup", "23000", 1062);
        }));
        assertEquals(1, calls.get());
    }
}
