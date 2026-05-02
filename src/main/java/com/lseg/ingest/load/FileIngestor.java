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

    // ── Column mapping resolved from the file header ──────────────────────────

    private record ColumnMapping(
            List<TargetSchema.Column> cols,
            int[] srcIndex,
            int[] keySrcIdx,
            int actionIdx,
            int ricIdx,
            boolean applyRicFilter) {}

    // ── Row-loop statistics returned to the coordinator ───────────────────────

    private record RowStats(int parsed, int insCount, int updCount, int delCount, int filterSkips) {}

    // ── Public entry point ────────────────────────────────────────────────────

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

    // ── Coordinator ───────────────────────────────────────────────────────────

    private void ingestWithParser(FileParser parser, IngestFile file, long jobId, String businessDate) throws Exception {
        parser.initialize(50);
        FileParser.Metadata md = parser.metadata();
        int declared = md != null ? md.declaredRows() : -1;
        audit.markStarted(file, businessDate, declared);

        ColumnMapping cm = buildColumnMapping(parser, file);
        if (cm == null) {
            String msg = "No overlapping columns found. File headers: " + parser.headerColumns()
                    + ". Expected for " + file.target() + ": " + TargetSchema.schemaSummary(file.target());
            log.error("ABORT {}: {}", file.fileName(), msg);
            audit.markFinished(file, AUDIT_FAILED, 0, 0, 0, 0, 0, 0, 0, 0, truncate(msg));
            return;
        }
        log.info("Mapped {}/{} columns for {}", cm.cols().size(), TargetSchema.columnsFor(file.target()).size(), file.fileName());

        String upsertSql = SqlBuilder.upsert(file.target(), cm.cols());
        String deleteSql = SqlBuilder.delete(file.target());
        String errorMessage = null;

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            ResilientBatchExecutor.RowBinder upsertBinder = (ps, row) -> {
                String[] v = row.values();
                for (int i = 0; i < cm.cols().size(); i++) {
                    cm.cols().get(i).binder().bind(ps, i + 1, v[i]);
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
                         conn, upsertSql, upsertBinder, props.getBatch().getUpsertSize(), file.fileName(), props.getRetry(), props.getBatch().getMaxSkippedRowsPerFile());
                 ResilientBatchExecutor deleter = new ResilientBatchExecutor(
                         conn, deleteSql, deleteBinder, props.getBatch().getDeleteSize(), file.fileName(), props.getRetry(), props.getBatch().getMaxSkippedRowsPerFile())) {

                RowStats stats = processRows(parser, cm, upserter, deleter, file, jobId);

                conn.commit();

                int insTotal = upserter.inserted() + deleter.inserted();
                int updTotal = upserter.updated() + deleter.updated();
                int unchangedTotal = upserter.unchanged() + deleter.unchanged();
                int errorSkips = upserter.skipped() + deleter.skipped();
                int totalSkipped = errorSkips + stats.filterSkips();
                String t = file.target().name();

                registry.counter(METRIC_ROWS_PARSED, TAG_TARGET, t).increment(stats.parsed());
                registry.counter(METRIC_ROWS_INSERTED, TAG_TARGET, t).increment(insTotal);
                registry.counter(METRIC_ROWS_UPDATED, TAG_TARGET, t).increment(updTotal);
                registry.counter(METRIC_ROWS_SKIPPED_ERROR, TAG_TARGET, t).increment(errorSkips);
                registry.counter(METRIC_ROWS_SKIPPED_FILTER, TAG_TARGET, t).increment(stats.filterSkips());
                registry.counter(METRIC_ROWS_OPS, TAG_TARGET, t, TAG_OP, ACTION_INSERT).increment(stats.insCount());
                registry.counter(METRIC_ROWS_OPS, TAG_TARGET, t, TAG_OP, ACTION_UPDATE).increment(stats.updCount());
                registry.counter(METRIC_ROWS_OPS, TAG_TARGET, t, TAG_OP, ACTION_DELETE).increment(stats.delCount());

                audit.markFinished(file, AUDIT_SUCCESS, stats.parsed(), insTotal, updTotal, unchangedTotal, totalSkipped,
                        stats.insCount(), stats.updCount(), stats.delCount(), null);
                log.info("FINISHED {}: parsed={} inserted={} updated={} unchanged={} ins={} upd={} del={} error_skips={} filter_skips={} declared={}",
                        file.fileName(), stats.parsed(), insTotal, updTotal, unchangedTotal, stats.insCount(),
                        stats.updCount(), stats.delCount(), errorSkips, stats.filterSkips(), declared);

            } catch (Exception e) {
                conn.rollback();
                errorMessage = e.getClass().getSimpleName() + ": " + e.getMessage();
                log.error("TERMINATED {}: {}. Transaction rolled back.", file.fileName(), errorMessage);
                audit.markFinished(file, AUDIT_FAILED, 0, 0, 0, 0, 0, 0, 0, 0, truncate(errorMessage));
                throw e;
            }
        }
    }

    // ── Column mapping ────────────────────────────────────────────────────────

    private ColumnMapping buildColumnMapping(FileParser parser, IngestFile file) {
        Set<String> headerSet = new HashSet<>(parser.headerColumns());
        List<TargetSchema.Column> cols = TargetSchema.intersect(file.target(), headerSet);
        if (cols.isEmpty()) return null;

        Map<String, Integer> headerIdx = parser.headerIndex();
        int[] srcIndex = new int[cols.size()];
        for (int i = 0; i < cols.size(); i++) {
            srcIndex[i] = headerIdx.get(cols.get(i).sourceHeader());
        }

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

        return new ColumnMapping(cols, srcIndex, keySrcIdx, actionIdx, ricIdx, applyRicFilter);
    }

    // ── Row processing loop ───────────────────────────────────────────────────

    private RowStats processRows(FileParser parser, ColumnMapping cm,
                                 ResilientBatchExecutor upserter, ResilientBatchExecutor deleter,
                                 IngestFile file, long jobId) throws Exception {
        RicCaretFilter ricFilter = new RicCaretFilter(cm.ricIdx());
        int parsed = 0, filterSkips = 0, insCount = 0, updCount = 0, delCount = 0;
        int cancelCheckEvery = Math.max(1, props.getCancel().getCheckRows());
        char activeSide = 0; // 0=none, 'U'=upsert, 'D'=delete

        String[] row;
        while ((row = parser.nextRow()) != null) {
            parsed++;

            if ((parsed % cancelCheckEvery) == 0) {
                if (jobDao.isStopped(jobId)) {
                    throw new InterruptedException("Stop signaled mid-file at row " + parsed);
                }
                log.info("Progress: parsed={}, inserted={}, updated={}, unchanged={}, skipped={}, filterSkips={} for {}",
                        parsed, (upserter.inserted() + deleter.inserted()),
                        (upserter.updated() + deleter.updated()),
                        (upserter.unchanged() + deleter.unchanged()),
                        (upserter.skipped() + deleter.skipped()), filterSkips, file.fileName());
            }

            if (cm.applyRicFilter() && ricFilter.shouldSkip(row)) {
                filterSkips++;
                continue;
            }

            String actionStr = (cm.actionIdx() >= 0 && cm.actionIdx() < row.length
                    && row[cm.actionIdx()] != null && !row[cm.actionIdx()].isEmpty())
                    ? row[cm.actionIdx()] : ACTION_INSERT;
            char action = actionStr.charAt(0);

            String firstKey = (cm.keySrcIdx().length > 0 && cm.keySrcIdx()[0] >= 0
                    && cm.keySrcIdx()[0] < row.length) ? row[cm.keySrcIdx()[0]] : null;

            if (action == ACTION_DELETE.charAt(0)) {
                if (activeSide == 'U') {
                    upserter.flush();
                    activeSide = 0;
                }
                delCount++;
                String[] keyVals = new String[cm.keySrcIdx().length];
                for (int k = 0; k < cm.keySrcIdx().length; k++) {
                    int idx = cm.keySrcIdx()[k];
                    keyVals[k] = (idx >= 0 && idx < row.length) ? row[idx] : null;
                }
                deleter.add(new PendingRow(keyVals, parser.currentLine(), firstKey));
                activeSide = 'D';
            } else {
                if (action == ACTION_UPDATE.charAt(0)) updCount++; else insCount++;
                if (activeSide == 'D') {
                    deleter.flush();
                    activeSide = 0;
                }
                String[] vals = new String[cm.cols().size()];
                for (int i = 0; i < cm.cols().size(); i++) {
                    int idx = cm.srcIndex()[i];
                    vals[i] = (idx >= 0 && idx < row.length) ? row[idx] : null;
                }
                upserter.add(new PendingRow(vals, parser.currentLine(), firstKey));
                activeSide = 'U';
            }
        }

        // Final flush — active side first to preserve in-file ordering semantics
        if (activeSide == 'D') {
            deleter.flush();
            upserter.flush();
        } else {
            upserter.flush();
            deleter.flush();
        }

        return new RowStats(parsed, insCount, updCount, delCount, filterSkips);
    }

    // ── Utilities ─────────────────────────────────────────────────────────────

    private static String truncate(String s) {
        if (s == null) return null;
        return s.length() > 4000 ? s.substring(0, 4000) : s;
    }
}
