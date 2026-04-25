package com.lseg.ingest.load;

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
 */
public class ResilientBatchExecutor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ResilientBatchExecutor.class);

    private final Connection conn;
    private final PreparedStatement ps;
    private final List<TargetSchema.Column> cols;
    private final int flushAt;
    private final List<PendingRow> buffered = new ArrayList<>();
    private final String fileName;

    private int succeeded;
    private int skipped;

    public ResilientBatchExecutor(Connection conn, String sql, List<TargetSchema.Column> cols, int flushAt, String fileName) throws SQLException {
        this.conn = conn;
        this.ps = conn.prepareStatement(sql);
        this.cols = cols;
        this.flushAt = flushAt;
        this.fileName = fileName;
    }

    /** Bind one row from raw header-aligned String values. The values array length must equal cols.size(). */
    public void add(PendingRow row) throws SQLException {
        bind(row);
        ps.addBatch();
        buffered.add(row);
        if (buffered.size() >= flushAt) flush();
    }

    public void flush() throws SQLException {
        if (buffered.isEmpty()) return;
        try {
            ps.executeBatch();
            succeeded += buffered.size();
        } catch (SQLException batchEx) {
            log.warn("BATCH FAILURE in {} after {} rows. Attempting recovery via row-by-row fallback. Error: {}", 
                    fileName, buffered.size(), batchEx.getMessage());
            ps.clearBatch();
            for (PendingRow row : buffered) {
                try {
                    bind(row);
                    ps.executeUpdate();
                    succeeded++;
                } catch (SQLException rowEx) {
                    skipped++;
                    log.error("ROW FAILURE in {} line={} key={}. REASON: {}. DATA: {}", 
                            fileName, row.lineNumber(), row.keyValue(), rowEx.getMessage(), Arrays.toString(row.values()));
                }
            }
        } finally {
            buffered.clear();
        }
    }

    private void bind(PendingRow row) throws SQLException {
        String[] v = row.values();
        for (int i = 0; i < cols.size(); i++) {
            cols.get(i).binder().bind(ps, i + 1, v[i]);
        }
    }

    public int succeeded() { return succeeded; }
    public int skipped() { return skipped; }

    @Override
    public void close() throws SQLException {
        ps.close();
    }
}
