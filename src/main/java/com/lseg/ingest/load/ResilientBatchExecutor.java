package com.lseg.ingest.load;

import com.lseg.ingest.config.IngestProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Builds a PreparedStatement, batches rows, and on batch failure re-runs the same N rows individually
 * so a single bad row does not abort the file. Caller controls flush timing.
 *
 * Retries transient errors (deadlocks, etc.) at both the batch level and the individual row level
 * before giving up or marking as skipped.
 */
public class ResilientBatchExecutor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ResilientBatchExecutor.class);

    @FunctionalInterface
    public interface RowBinder {
        void bind(PreparedStatement ps, PendingRow row) throws SQLException;
    }

    private final Connection conn;
    private final PreparedStatement ps;
    private final RowBinder binder;
    private final int flushAt;
    private final List<PendingRow> buffered = new ArrayList<>();
    private final String fileName;
    private final IngestProperties.Retry retryCfg;
    private final int maxSkippedRows;

    private int succeeded;
    private int skipped;

    public ResilientBatchExecutor(Connection conn, String sql, RowBinder binder, int flushAt, 
                                  String fileName, IngestProperties.Retry retryCfg, int maxSkippedRows) throws SQLException {
        this.conn = conn;
        this.ps = conn.prepareStatement(sql);
        this.binder = binder;
        this.flushAt = flushAt;
        this.fileName = fileName;
        this.retryCfg = retryCfg;
        this.maxSkippedRows = maxSkippedRows;
    }

    /** Bind one row from raw header-aligned String values. */
    public void add(PendingRow row) throws SQLException {
        binder.bind(ps, row);
        ps.addBatch();
        buffered.add(row);
        if (buffered.size() >= flushAt) flush();
    }

    public void flush() throws SQLException {
        if (buffered.isEmpty()) return;
        try {
            // 1. MASSIVE SPEED: Attempt to send the entire batch (e.g., 5000 rows) to the DB at once.
            // We use SqlRetry to handle transient errors like deadlocks automatically.
            SqlRetry.withRetry(retryCfg, "batch:" + fileName, () -> {
                ps.executeBatch();
                return null;
            });
            succeeded += buffered.size();
        } catch (Exception batchEx) {
            // 2. THE FALLBACK: If the batch failed (e.g., one row had bad data or a permanent constraint violation),
            // we catch the exception, clear the batch, and switch to "Safe Mode" (Row-by-Row).
            if (!SqlRetry.isTransient(batchEx)) {
                log.warn("PERMANENT BATCH FAILURE in {} after {} rows. Falling back to row-by-row. Error: {}", 
                        fileName, buffered.size(), batchEx.getMessage());
            } else {
                log.error("TRANSIENT BATCH FAILURE in {} exhausted retries. Falling back to row-by-row. Error: {}", 
                        fileName, batchEx.getMessage());
            }
            
            ps.clearBatch();
            for (PendingRow row : buffered) {
                try {
                    // 3. ISOLATION: Attempt to insert/update/delete just this one specific row.
                    // We also apply retries here in case of individual row deadlocks.
                    SqlRetry.withRetry(retryCfg, "row:" + fileName + ":" + row.lineNumber(), () -> {
                        binder.bind(ps, row);
                        ps.executeUpdate();
                        return null;
                    });
                    succeeded++;
                } catch (Exception rowEx) {
                    // 4. POISON ROW IMMUNITY: This specific row is completely broken (e.g., data too large).
                    // We log the exact line number, key, and data so it can be fixed manually, 
                    // but we DO NOT stop the entire file UNLESS the threshold is exceeded.
                    skipped++;
                    log.error("ROW FAILURE in {} line={} key={}. REASON: {}. DATA: {}", 
                            fileName, row.lineNumber(), row.keyValue(), rowEx.getMessage(), Arrays.toString(row.values()));
                    if (skipped > maxSkippedRows) {
                        throw new RuntimeException(String.format(
                                "maxSkippedRowsPerFile exceeded (%d > %d) for %s. Aborting file.",
                                skipped, maxSkippedRows, fileName));
                    }
                }
            }
        } finally {
            // 5. Cleanup: Always clear the buffer so the next batch can start fresh.
            buffered.clear();
        }
    }

    public int succeeded() { return succeeded; }
    public int skipped() { return skipped; }

    @Override
    public void close() throws SQLException {
        ps.close();
    }
}
