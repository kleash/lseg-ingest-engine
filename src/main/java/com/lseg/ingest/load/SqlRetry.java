package com.lseg.ingest.load;

import com.lseg.ingest.config.IngestProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.Set;

/**
 * Retries a fallible operation on transient DB errors with exponential backoff.
 * Transient = MariaDB error codes 1213 (deadlock), 1205 (lock wait timeout),
 * SQL state class '08' (connection), or any java.sql.SQLTransientException.
 *
 * Non-transient SQL errors (constraint violations, syntax) propagate immediately.
 */
public class SqlRetry {

    private static final Logger log = LoggerFactory.getLogger(SqlRetry.class);

    private static final Set<Integer> TRANSIENT_VENDOR_CODES = Set.of(1205, 1213);

    @FunctionalInterface
    public interface Op<T> {
        T run() throws Exception;
    }

    public static <T> T withRetry(IngestProperties.Retry cfg, String label, Op<T> op) throws Exception {
        int attempts = Math.max(1, cfg.getMaxAttempts());
        long delay = cfg.getInitialDelayMs();
        Exception last = null;
        for (int i = 1; i <= attempts; i++) {
            try {
                return op.run();
            } catch (Exception e) {
                last = e;
                if (!isTransient(e) || i == attempts) throw e;
                log.warn("Transient error on '{}' attempt {}/{}: {}. Backing off {}ms",
                        label, i, attempts, e.getMessage(), delay);
                try { Thread.sleep(delay); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
                delay = Math.min(delay * 2, cfg.getMaxDelayMs());
            }
        }
        throw last;
    }

    static boolean isTransient(Throwable t) {
        while (t != null) {
            if (t instanceof SQLTransientException) return true;
            if (t instanceof SQLException sql) {
                if (TRANSIENT_VENDOR_CODES.contains(sql.getErrorCode())) return true;
                String state = sql.getSQLState();
                if (state != null && state.startsWith("08")) return true; // connection class
                if (state != null && state.equals("40001")) return true; // serialization
            }
            t = t.getCause();
        }
        return false;
    }
}
