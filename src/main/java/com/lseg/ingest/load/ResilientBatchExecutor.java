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

    private int succeeded;
    private int skipped;

    public ResilientBatchExecutor(Connection conn, String sql, RowBinder binder, int flushAt, 
                                  String fileName, IngestProperties.Retry retryCfg) throws SQLException {
        this.conn = conn;
        this.ps = conn.prepareStatement(sql);
        this.binder = binder;
        this.flushAt = flushAt;
        this.fileName = fileName;
        this.retryCfg = retryCfg;
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
            SqlRetry.withRetry(retryCfg, "batch:" + fileName, () -> {
                ps.executeBatch();
                return null;
            });
            succeeded += buffered.size();
        } catch (Exception batchEx) {
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
                    SqlRetry.withRetry(retryCfg, "row:" + fileName + ":" + row.lineNumber(), () -> {
                        binder.bind(ps, row);
                        ps.executeUpdate();
                        return null;
                    });
                    succeeded++;
                } catch (Exception rowEx) {
                    skipped++;
                    log.error("ROW FAILURE in {} line={} key={}. REASON: {}. DATA: {}", 
                            fileName, row.lineNumber(), row.keyValue(), rowEx.getMessage(), Arrays.toString(row.values()));
                }
            }
        } finally {
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
