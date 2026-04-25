package com.lseg.ingest.load;

import com.lseg.ingest.audit.FileAuditDao;
import com.lseg.ingest.config.IngestProperties;
import com.lseg.ingest.filter.RicCaretFilter;
import com.lseg.ingest.io.PipeFileParser;
import com.lseg.ingest.io.ZipLineReader;
import com.lseg.ingest.plan.IngestFile;
import com.lseg.ingest.plan.Kind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Loads a single LSEG file (INT or DELTA) into the target table. Idempotent — uses ON DUPLICATE KEY UPDATE
 * keyed on the natural perm-id column; safe to re-run.
 */
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;

@Component
public class FileIngestor {

    private static final Logger log = LoggerFactory.getLogger(FileIngestor.class);

    private final DataSource ds;
    private final IngestProperties props;
    private final FileAuditDao audit;
    private final MeterRegistry registry;

    public FileIngestor(DataSource ds, IngestProperties props, FileAuditDao audit, MeterRegistry registry) {
        this.ds = ds;
        this.props = props;
        this.audit = audit;
        this.registry = registry;
    }

    public void ingest(IngestFile file) throws Exception {
        String businessDate = props.getBusinessDate();
        log.info("Ingestion started: file={} target={} kind={}", file.fileName(), file.target(), file.kind());
        
        try (ZipLineReader z = new ZipLineReader(file.path())) {
            PipeFileParser parser = new PipeFileParser(z.reader());
            parser.initialize(50);
            PipeFileParser.Metadata md = parser.metadata();
            int declared = md != null ? md.declaredRows() : -1;
            audit.markStarted(file, businessDate, declared);

            // Resolve column intersection between schema and this file.
            Set<String> headerSet = new HashSet<>(parser.headerColumns());
            List<TargetSchema.Column> cols = TargetSchema.intersect(file.target(), headerSet);
            if (cols.isEmpty()) {
                String msg = "No overlapping columns found. File headers: " + parser.headerColumns() + 
                             ". Expected for " + file.target() + ": " + TargetSchema.schemaSummary(file.target());
                log.error("ABORT {}: {}", file.fileName(), msg);
                audit.markFinished(file, "FAILED", 0, 0, 0, 0, 0, 0, msg);
                return;
            }
            log.info("Mapped {}/{} columns for {}", cols.size(), TargetSchema.columnsFor(file.target()).size(), file.fileName());

            // Pre-compute source-header → file column index for fast row binding.
            Map<String, Integer> headerIdx = parser.headerIndex();
            int[] srcIndex = new int[cols.size()];
            int keyIdxInBoundCols = -1;
            for (int i = 0; i < cols.size(); i++) {
                srcIndex[i] = headerIdx.get(cols.get(i).sourceHeader());
                if (cols.get(i).dbColumn().equals(file.target().permIdColumn)) keyIdxInBoundCols = srcIndex[i];
            }
            int actionIdx = headerIdx.getOrDefault("Action", -1);
            int ricIdx = headerIdx.getOrDefault("RIC", -1);
            boolean applyRicFilter = props.isRicCaretFilter()
                    && file.kind() == Kind.INT
                    && file.target() == com.lseg.ingest.plan.Target.QUOTES
                    && ricIdx >= 0;
            RicCaretFilter ricFilter = new RicCaretFilter(ricIdx);

            String upsertSql = SqlBuilder.upsert(file.target(), cols);
            String deleteSql = SqlBuilder.delete(file.target());

            int parsed = 0, inserted = 0, skipped = 0, filterSkips = 0;
            int insCount = 0, updCount = 0, delCount = 0;
            String errorMessage = null;

            try (Connection conn = ds.getConnection()) {
                conn.setAutoCommit(false);
                try (ResilientBatchExecutor upserter = new ResilientBatchExecutor(conn, upsertSql, cols, props.getBatch().getUpsertSize(), file.fileName());
                     PreparedStatement deletePs = conn.prepareStatement(deleteSql)) {

                    int deleteBatchSize = 0;
                    int deleteBatchLimit = props.getBatch().getDeleteSize();

                    String[] row;
                    while ((row = parser.nextRow()) != null) {
                        parsed++;
                        if (applyRicFilter && ricFilter.shouldSkip(row)) { 
                            filterSkips++; 
                            continue; 
                        }

                        char action = (actionIdx >= 0 && row[actionIdx] != null && !row[actionIdx].isEmpty())
                                ? row[actionIdx].charAt(0) : 'I';

                        if (action == 'D') {
                            delCount++;
                            String key = (keyIdxInBoundCols >= 0) ? row[keyIdxInBoundCols] : null;
                            if (key == null || key.isBlank()) { 
                                log.warn("Missing natural key for DELETE in {} line={}", file.fileName(), parser.currentLine());
                                skipped++; 
                                continue; 
                            }
                            deletePs.setString(1, key);
                            deletePs.addBatch();
                            deleteBatchSize++;
                            if (deleteBatchSize >= deleteBatchLimit) {
                                deletePs.executeBatch();
                                deleteBatchSize = 0;
                            }
                        } else {
                            if (action == 'U') updCount++; else insCount++;
                            // I or U: bind the cols-aligned values and queue an upsert.
                            String[] vals = new String[cols.size()];
                            for (int i = 0; i < cols.size(); i++) vals[i] = row[srcIndex[i]];
                            String key = keyIdxInBoundCols >= 0 ? row[keyIdxInBoundCols] : null;
                            upserter.add(new PendingRow(vals, parser.currentLine(), key));
                        }
                    }

                    upserter.flush();
                    if (deleteBatchSize > 0) deletePs.executeBatch();
                    conn.commit();

                    inserted = upserter.succeeded();
                    int errorSkips = skipped + upserter.skipped();
                    skipped = errorSkips + filterSkips;

                    // Publish row metrics
                    String t = file.target().name();
                    registry.counter("ingest.rows.parsed", "target", t).increment(parsed);
                    registry.counter("ingest.rows.inserted", "target", t).increment(inserted);
                    registry.counter("ingest.rows.skipped.error", "target", t).increment(errorSkips);
                    registry.counter("ingest.rows.skipped.filter", "target", t).increment(filterSkips);
                    registry.counter("ingest.rows.ops", "target", t, "op", "I").increment(insCount);
                    registry.counter("ingest.rows.ops", "target", t, "op", "U").increment(updCount);
                    registry.counter("ingest.rows.ops", "target", t, "op", "D").increment(delCount);

                } catch (Exception e) {
                    conn.rollback();
                    errorMessage = e.getClass().getSimpleName() + ": " + e.getMessage();
                    log.error("TERMINATED {}: {}. Transaction rolled back.", file.fileName(), errorMessage);
                    audit.markFinished(file, "FAILED", parsed, inserted, skipped, insCount, updCount, delCount, errorMessage);
                    throw e;
                }
            }

            audit.markFinished(file, "SUCCESS", parsed, inserted, skipped, insCount, updCount, delCount, null);
            log.info("FINISHED {}: parsed={} inserted={} ins={} upd={} del={} error_skips={} filter_skips={} declared={}", 
                    file.fileName(), parsed, inserted, insCount, updCount, delCount, (skipped - filterSkips), filterSkips, declared);
        }
    }
}
