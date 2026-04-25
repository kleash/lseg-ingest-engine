package com.lseg.ingest.load;

import com.lseg.ingest.audit.FileAuditDao;
import com.lseg.ingest.audit.JobDao;
import com.lseg.ingest.config.IngestProperties;
import com.lseg.ingest.filter.RicCaretFilter;
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
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    public void ingest(IngestFile file, long jobId) throws Exception {
        MDC.put("file", file.fileName());
        MDC.put("jobId", String.valueOf(jobId));
        try {
            SqlRetry.withRetry(props.getRetry(), "ingest:" + file.fileName(), () -> {
                doIngest(file, jobId);
                return null;
            });
        } finally {
            MDC.remove("file");
            MDC.remove("jobId");
        }
    }

    private void doIngest(IngestFile file, long jobId) throws Exception {
        String businessDate = props.getBusinessDate();
        log.info("Ingestion started: file={} target={} kind={}", file.fileName(), file.target(), file.kind());

        Charset charset = Charset.forName(props.getCharset());
        try (ZipLineReader z = new ZipLineReader(file.path(), charset)) {
            PipeFileParser parser = new PipeFileParser(z.reader());
            parser.initialize(50);
            PipeFileParser.Metadata md = parser.metadata();
            int declared = md != null ? md.declaredRows() : -1;
            audit.markStarted(file, businessDate, declared);

            Set<String> headerSet = new HashSet<>(parser.headerColumns());
            List<TargetSchema.Column> cols = TargetSchema.intersect(file.target(), headerSet);
            if (cols.isEmpty()) {
                String msg = "No overlapping columns found. File headers: " + parser.headerColumns()
                        + ". Expected for " + file.target() + ": " + TargetSchema.schemaSummary(file.target());
                log.error("ABORT {}: {}", file.fileName(), msg);
                audit.markFinished(file, "FAILED", 0, 0, 0, 0, 0, 0, truncate(msg));
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

            int actionIdx = headerIdx.getOrDefault("Action", -1);
            int ricIdx = headerIdx.getOrDefault("RIC", -1);
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
            int maxSkipsPerFile = props.getResilience().getMaxSkippedRowsPerFile();
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

                    String[] row;
                    while ((row = parser.nextRow()) != null) {
                        parsed++;
                        if ((parsed % cancelCheckEvery) == 0 && jobDao.isStopped(jobId)) {
                            throw new InterruptedException("Stop signaled mid-file at row " + parsed);
                        }

                        if (applyRicFilter && ricFilter.shouldSkip(row)) {
                            filterSkips++;
                            continue;
                        }

                        char action = (actionIdx >= 0 && actionIdx < row.length && row[actionIdx] != null && !row[actionIdx].isEmpty())
                                ? row[actionIdx].charAt(0) : 'I';

                        String firstKey = (keySrcIdx.length > 0 && keySrcIdx[0] >= 0 && keySrcIdx[0] < row.length)
                                ? row[keySrcIdx[0]] : null;

                        if (action == 'D') {
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
                            if (action == 'U') updCount++; else insCount++;
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

                        if (upserter.skipped() + deleter.skipped() + skipped > maxSkipsPerFile) {
                            throw new RuntimeException("maxSkippedRowsPerFile exceeded: " + (upserter.skipped() + deleter.skipped() + skipped));
                        }
                    }

                    if (activeSide == 'D') {
                        upserter.flush();
                        deleter.flush();
                    } else {
                        deleter.flush();
                        upserter.flush();
                    }
                    conn.commit();

                    int inserted = upserter.succeeded();
                    int deleted = deleter.succeeded();
                    int errorSkips = skipped + upserter.skipped() + deleter.skipped();
                    int totalSkipped = errorSkips + filterSkips;

                    String t = target.name();
                    registry.counter("ingest.rows.parsed", "target", t).increment(parsed);
                    registry.counter("ingest.rows.inserted", "target", t).increment(inserted);
                    registry.counter("ingest.rows.skipped.error", "target", t).increment(errorSkips);
                    registry.counter("ingest.rows.skipped.filter", "target", t).increment(filterSkips);
                    registry.counter("ingest.rows.ops", "target", t, "op", "I").increment(insCount);
                    registry.counter("ingest.rows.ops", "target", t, "op", "U").increment(updCount);
                    registry.counter("ingest.rows.ops", "target", t, "op", "D").increment(delCount);

                    audit.markFinished(file, "SUCCESS", parsed, inserted, totalSkipped, insCount, updCount, delCount, null);
                    log.info("FINISHED {}: parsed={} inserted={} ins={} upd={} del={} error_skips={} filter_skips={} declared={}",
                            file.fileName(), parsed, inserted, insCount, updCount, delCount, errorSkips, filterSkips, declared);

                } catch (Exception e) {
                    conn.rollback();
                    errorMessage = e.getClass().getSimpleName() + ": " + e.getMessage();
                    log.error("TERMINATED {}: {}. Transaction rolled back.", file.fileName(), errorMessage);
                    audit.markFinished(file, "FAILED", parsed, 0, skipped + filterSkips, insCount, updCount, delCount, truncate(errorMessage));
                    throw e;
                }
            }
        }
    }

    private static String truncate(String s) {
        if (s == null) return null;
        return s.length() > 4000 ? s.substring(0, 4000) : s;
    }
}
