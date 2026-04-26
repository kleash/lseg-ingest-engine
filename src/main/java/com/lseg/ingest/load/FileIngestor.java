package com.lseg.ingest.load;

import com.lseg.ingest.audit.FileAuditDao;
import com.lseg.ingest.audit.JobDao;
import com.lseg.ingest.config.IngestProperties;
import com.lseg.ingest.filter.RicCaretFilter;
import com.lseg.ingest.io.CsvFileParser;
import com.lseg.ingest.io.FileParser;
import com.lseg.ingest.io.PipeFileParser;
import com.lseg.ingest.io.ZipLineReader;
import com.lseg.ingest.plan.IngestFile;
import com.lseg.ingest.plan.Kind;
import com.lseg.ingest.plan.Target;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.sql.Connection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.lseg.ingest.Constants.*;

/**
 * Loads a single LSEG file into the target table.
 *
 * Idempotent: upserts via composite UNIQUE(target.uniqueKeyColumns); soft-delete sets is_deleted=1.
 *
 * In-file action ordering is preserved: when the action stream flips between {I,U} and {D},
 * the active buffer is flushed before the other side starts batching. This guarantees that
 * "D <key>" then "I <key>" for the same key in the same file produces a live (un-deleted) row,
 * and "I <key>" then "D <key>" produces a soft-deleted row — matching file order semantics.
 *
 * Cooperative cancellation: every props.cancel.checkRows the loop polls JobDao.isStopped(jobId);
 * on stop, the current transaction is rolled back and an exception is thrown.
 */
@Component
public class FileIngestor {

    private static final Logger log = LoggerFactory.getLogger(FileIngestor.class);

    private final DataSource ds;
    private final IngestProperties props;
    private final FileAuditDao audit;
    private final JobDao jobDao;
    private final MeterRegistry registry;

    public FileIngestor(DataSource ds, IngestProperties props, FileAuditDao audit, JobDao jobDao, MeterRegistry registry) {
        this.ds = ds;
        this.props = props;
        this.audit = audit;
        this.jobDao = jobDao;
        this.registry = registry;
    }

    public void ingest(IngestFile file, long jobId, String businessDate) throws Exception {
        MDC.put(MDC_FILE, file.fileName());
        MDC.put(MDC_JOB_ID, String.valueOf(jobId));
        try {
            SqlRetry.withRetry(props.getRetry(), "ingest:" + file.fileName(), () -> {
                doIngest(file, jobId, businessDate);
                return null;
            });
        } finally {
            MDC.remove(MDC_FILE);
            MDC.remove(MDC_JOB_ID);
        }
    }

    private void doIngest(IngestFile file, long jobId, String businessDate) throws Exception {
        log.info("Ingestion started: file={} target={} kind={}", file.fileName(), file.target(), file.kind());

        Charset charset = Charset.forName(props.getCharset());
        if (file.fileName().endsWith(".zip")) {
            try (ZipLineReader z = new ZipLineReader(file.path(), charset)) {
                ingestWithParser(new PipeFileParser(z.reader()), file, jobId, businessDate);
            }
        } else if (file.fileName().endsWith(".csv")) {
            try (InputStream is = Files.newInputStream(file.path());
                 BufferedReader reader = new BufferedReader(new InputStreamReader(is, charset))) {
                ingestWithParser(new CsvFileParser(reader, file.fileName()), file, jobId, businessDate);
            }
        } else {
            throw new IllegalArgumentException("Unsupported file format: " + file.fileName());
        }
    }

    private void ingestWithParser(FileParser parser, IngestFile file, long jobId, String businessDate) throws Exception {
        parser.initialize(50);
        FileParser.Metadata md = parser.metadata();
        int declared = md != null ? md.declaredRows() : -1;
        audit.markStarted(file, businessDate, declared);

        Set<String> headerSet = new HashSet<>(parser.headerColumns());
        List<TargetSchema.Column> cols = TargetSchema.intersect(file.target(), headerSet);
        if (cols.isEmpty()) {
            String msg = "No overlapping columns found. File headers: " + parser.headerColumns()
                    + ". Expected for " + file.target() + ": " + TargetSchema.schemaSummary(file.target());
            log.error("ABORT {}: {}", file.fileName(), msg);
            audit.markFinished(file, AUDIT_FAILED, 0, 0, 0, 0, 0, 0, truncate(msg));
            return;
        }
        log.info("Mapped {}/{} columns for {}", cols.size(), TargetSchema.columnsFor(file.target()).size(), file.fileName());

        Map<String, Integer> headerIdx = parser.headerIndex();
            int[] srcIndex = new int[cols.size()];
            for (int i = 0; i < cols.size(); i++) {
                srcIndex[i] = headerIdx.get(cols.get(i).sourceHeader());
            }

            // Precompute row-side indices for the unique-key source headers (in declared order).
            Target target = file.target();
            int[] keySrcIdx = new int[target.uniqueKeySourceHeaders.size()];
            for (int i = 0; i < keySrcIdx.length; i++) {
                keySrcIdx[i] = headerIdx.getOrDefault(target.uniqueKeySourceHeaders.get(i), -1);
            }

            int actionIdx = headerIdx.getOrDefault(COL_ACTION, -1);
            int ricIdx = headerIdx.getOrDefault(COL_RIC, -1);
            boolean applyRicFilter = props.isRicCaretFilter()
                    && file.kind() == Kind.INT
                    && target == Target.QUOTES
                    && ricIdx >= 0;
            RicCaretFilter ricFilter = new RicCaretFilter(ricIdx);

            String upsertSql = SqlBuilder.upsert(target, cols);
            String deleteSql = SqlBuilder.delete(target);

            int parsed = 0, skipped = 0, filterSkips = 0;
            int insCount = 0, updCount = 0, delCount = 0;
            String errorMessage = null;
            int cancelCheckEvery = Math.max(1, props.getCancel().getCheckRows());

            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);

                ResilientBatchExecutor.RowBinder upsertBinder = (ps, row) -> {
                    String[] v = row.values();
                    for (int i = 0; i < cols.size(); i++) {
                        cols.get(i).binder().bind(ps, i + 1, v[i]);
                    }
                };

                ResilientBatchExecutor.RowBinder deleteBinder = (ps, row) -> {
                    String[] v = row.values();
                    for (int i = 0; i < v.length; i++) {
                        if (v[i] == null || v[i].isEmpty()) ps.setNull(i + 1, java.sql.Types.VARCHAR);
                        else ps.setString(i + 1, v[i]);
                    }
                };

                try (ResilientBatchExecutor upserter = new ResilientBatchExecutor(
                             conn, upsertSql, upsertBinder, props.getBatch().getUpsertSize(), file.fileName(), props.getRetry());
                     ResilientBatchExecutor deleter = new ResilientBatchExecutor(
                             conn, deleteSql, deleteBinder, props.getBatch().getDeleteSize(), file.fileName(), props.getRetry())) {

                    // 'U' -> upsert, 'D' -> delete. Track current batch side so we flush on flip.
                    char activeSide = 0; // 0=none, 'U'=upsert, 'D'=delete

                    // 5. Read the file line by line until the end.
                    // The parser handles transparent ZIP decompression and pipe-splitting.
                    String[] row;
                    while ((row = parser.nextRow()) != null) {
                        parsed++;

                        // 6. Check for stop signal.
                        // We poll the database every N rows to see if a human operator requested a STOP.
                        if ((parsed % cancelCheckEvery) == 0) {
                            if (jobDao.isStopped(jobId)) {
                                throw new InterruptedException("Stop signaled mid-file at row " + parsed);
                            }
                            log.info("Progress: parsed={}, inserted={}, skipped={}, filterSkips={} for {}",
                                    parsed, (upserter.succeeded() + deleter.succeeded()), (upserter.skipped() + deleter.skipped()), filterSkips, file.fileName());
                        }

                        // 7. Apply RIC Caret Filter.
                        // Some feeds contain internal records marked with '^' which we must ignore.
                        if (applyRicFilter && ricFilter.shouldSkip(row)) {
                            filterSkips++;
                            continue;
                        }

                        // 8. Determine Action (Insert/Update vs Delete).
                        // Reference data uses 'I', 'U', or 'D'. We default to 'I' if unknown.
                        String actionStr = (actionIdx >= 0 && actionIdx < row.length && row[actionIdx] != null && !row[actionIdx].isEmpty())
                                ? row[actionIdx] : ACTION_INSERT;
                        char action = actionStr.charAt(0);

                        // Key value for logging/mapping.
                        String firstKey = (keySrcIdx.length > 0 && keySrcIdx[0] >= 0 && keySrcIdx[0] < row.length)
                                ? row[keySrcIdx[0]] : null;

                        if (action == ACTION_DELETE.charAt(0)) {
                            // 9. Process Delete.
                            // If the previous row was an Upsert, we must flush the database buffer now.
                            if (activeSide == 'U') {
                                upserter.flush();
                                activeSide = 0;
                            }
                            delCount++;
                            String[] keyVals = new String[keySrcIdx.length];
                            for (int k = 0; k < keySrcIdx.length; k++) {
                                int idx = keySrcIdx[k];
                                keyVals[k] = (idx >= 0 && idx < row.length) ? row[idx] : null;
                            }
                            deleter.add(new PendingRow(keyVals, parser.currentLine(), firstKey));
                            activeSide = 'D';
                        } else {
                            // 10. Process Upsert (Insert or Update).
                            if (action == ACTION_UPDATE.charAt(0)) updCount++; else insCount++;

                            // If the previous row was a Delete, flush that buffer first to preserve order.
                            if (activeSide == 'D') {
                                deleter.flush();
                                activeSide = 0;
                            }
                            String[] vals = new String[cols.size()];
                            for (int i = 0; i < cols.size(); i++) {
                                int idx = srcIndex[i];
                                vals[i] = (idx >= 0 && idx < row.length) ? row[idx] : null;
                            }
                            upserter.add(new PendingRow(vals, parser.currentLine(), firstKey));
                            activeSide = 'U';
                        }
                    }

                    // 11. Final Flush.
                    // Send any remaining rows in the buffers to the database.
                    if (activeSide == 'D') {
                        upserter.flush();
                        deleter.flush();
                    } else {
                        deleter.flush();
                        upserter.flush();
                    }

                    // 12. Commit.
                    // Actually finalize the transaction in MariaDB.
                    conn.commit();

                    int inserted = upserter.succeeded();
                    int deleted = deleter.succeeded();
                    int errorSkips = skipped + upserter.skipped() + deleter.skipped();
                    int totalSkipped = errorSkips + filterSkips;

                    String t = target.name();
                    registry.counter(METRIC_ROWS_PARSED, TAG_TARGET, t).increment(parsed);
                    registry.counter(METRIC_ROWS_INSERTED, TAG_TARGET, t).increment(inserted + deleted);
                    registry.counter(METRIC_ROWS_SKIPPED_ERROR, TAG_TARGET, t).increment(errorSkips);
                    registry.counter(METRIC_ROWS_SKIPPED_FILTER, TAG_TARGET, t).increment(filterSkips);
                    registry.counter(METRIC_ROWS_OPS, TAG_TARGET, t, TAG_OP, ACTION_INSERT).increment(insCount);
                    registry.counter(METRIC_ROWS_OPS, TAG_TARGET, t, TAG_OP, ACTION_UPDATE).increment(updCount);
                    registry.counter(METRIC_ROWS_OPS, TAG_TARGET, t, TAG_OP, ACTION_DELETE).increment(delCount);

                    audit.markFinished(file, AUDIT_SUCCESS, parsed, inserted + deleted, totalSkipped, insCount, updCount, delCount, null);
                    log.info("FINISHED {}: parsed={} inserted={} ins={} upd={} del={} error_skips={} filter_skips={} declared={}",
                            file.fileName(), parsed, inserted + deleted, insCount, updCount, delCount, errorSkips, filterSkips, declared);

                } catch (Exception e) {
                    conn.rollback();
                    errorMessage = e.getClass().getSimpleName() + ": " + e.getMessage();
                    log.error("TERMINATED {}: {}. Transaction rolled back.", file.fileName(), errorMessage);
                    audit.markFinished(file, AUDIT_FAILED, parsed, 0, skipped + filterSkips, insCount, updCount, delCount, truncate(errorMessage));
                    throw e;
                }
            }
        }

    private static String truncate(String s) {
        if (s == null) return null;
        return s.length() > 4000 ? s.substring(0, 4000) : s;
    }
}
