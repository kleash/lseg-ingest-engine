package com.lseg.ingest.orchestrator;

import com.lseg.ingest.audit.FileAuditDao;
import com.lseg.ingest.config.IngestProperties;
import com.lseg.ingest.load.FileIngestor;
import com.lseg.ingest.plan.*;
import com.lseg.ingest.sanity.FileSanityCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;

/**
 * Drives the end-to-end run:
 *   1. Scan + classify files; drop those already SUCCESSful in the audit table.
 *   2. Sanity-check every file (header / metadata / key column); mark SKIPPED_SANITY for failures.
 *   3. For each Target in parallel: run all INT files (parallel within target), then all DELTA files (sequential by seq).
 *      DELTA never starts for a target while INT for that target is still running.
 */
import com.lseg.ingest.audit.FileAuditDao;
import com.lseg.ingest.audit.JobDao;
import com.lseg.ingest.config.IngestProperties;
import com.lseg.ingest.load.FileIngestor;
import com.lseg.ingest.plan.*;
import com.lseg.ingest.sanity.FileSanityCheck;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.concurrent.*;

@Component
public class IngestOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(IngestOrchestrator.class);

    private final FileScanner scanner;
    private final FileSanityCheck sanity;
    private final FileAuditDao audit;
    private final JobDao jobDao;
    private final FileIngestor ingestor;
    private final IngestProperties props;
    private final MeterRegistry registry;

    public IngestOrchestrator(FileScanner scanner, FileSanityCheck sanity, FileAuditDao audit, JobDao jobDao,
                              FileIngestor ingestor, IngestProperties props, MeterRegistry registry) {
        this.scanner = scanner;
        this.sanity = sanity;
        this.audit = audit;
        this.jobDao = jobDao;
        this.ingestor = ingestor;
        this.props = props;
        this.registry = registry;
    }

    public void run(long jobId) throws Exception {
        Timer.Sample overallSample = Timer.start(registry);
        try {
            checkStop(jobId);
            List<IngestFile> all = scanner.scan();
            Set<String> alreadyDone = audit.loadSuccessFileNames();
            List<IngestFile> remaining = new ArrayList<>(all.size());
            for (IngestFile f : all) {
                if (alreadyDone.contains(f.fileName())) {
                    log.debug("Skipping already-ingested file {}", f.fileName());
                } else {
                    remaining.add(f);
                }
            }
            log.info("Production Status - Files: total={} already-ingested={} remaining={}", 
                    all.size(), all.size() - remaining.size(), remaining.size());

            // Sanity-check every file before any ingestion starts.
            List<IngestFile> good = new ArrayList<>(remaining.size());
            for (IngestFile f : remaining) {
                checkStop(jobId);
                FileSanityCheck.Result r = sanity.check(f, props.getBusinessDate());
                if (!r.ok()) {
                    log.warn("CRITICAL SANITY FAIL {}: {}", f.fileName(), r.reason());
                    audit.markSkippedSanity(f, r.reason(), props.getBusinessDate());
                    registry.counter("ingest.sanity.failures", "target", f.target().name()).increment();
                } else {
                    good.add(f);
                }
            }
            IngestPlan plan = new IngestPlan(good);
            log.info("Ingestion Plan built: {}", plan.summary());

            ExecutorService perTargetPool = Executors.newFixedThreadPool(
                    Math.max(1, props.getThreads().getDeltaTargetsParallel()),
                    namedThreads("target"));

            List<Future<?>> targetFutures = new ArrayList<>();
            for (Target t : Target.values()) {
                targetFutures.add(perTargetPool.submit(() -> runTarget(t, plan, jobId)));
            }
            for (Future<?> f : targetFutures) {
                try { f.get(); } catch (ExecutionException e) {
                    log.error("Target pipeline failed unexpectedly", e.getCause());
                    registry.counter("ingest.target.errors").increment();
                }
            }
            perTargetPool.shutdown();
        } catch (Exception e) {
            log.error("CRITICAL: Orchestrator encountered a fatal error", e);
            registry.counter("ingest.orchestrator.errors").increment();
            throw e;
        } finally {
            overallSample.stop(registry.timer("ingest.overall.duration"));
            log.info("Ingestion session finished. Global metrics published.");
        }
    }

    private void runTarget(Target t, IngestPlan plan, long jobId) {
        log.info("Starting target pipeline for {}", t);
        try {
            checkStop(jobId);
            runIntPhase(t, plan.intFor(t), jobId);    // blocks until done — gates DELTA
            checkStop(jobId);
            runDeltaPhase(t, plan.deltaFor(t), jobId);
        } catch (Exception e) {
            log.error("Pipeline for {} aborted with error", t, e);
            throw new RuntimeException(e);
        } finally {
            log.info("Target pipeline for {} finished", t);
        }
    }

    private void runIntPhase(Target t, List<IngestFile> files, long jobId) throws InterruptedException {
        if (files.isEmpty()) {
            log.info("No INT files to process for {}", t);
            return;
        }
        int threads = Math.min(props.getThreads().getIntPerTable(), files.size());
        log.info("Target={} INT phase starting with {} files using {} threads", t, files.size(), threads);
        
        ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(threads, namedThreads("int-" + t.name().toLowerCase()));
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (IngestFile f : files) {
                futures.add(pool.submit(() -> {
                    checkStop(jobId);
                    safeIngest(f);
                }));
            }
            for (Future<?> f : futures) {
                try { f.get(); } catch (ExecutionException e) { 
                    log.error("Internal INT task failed", e.getCause()); 
                }
            }
        } finally {
            log.info("Target={} INT phase complete. Pool status: completed={}", t, pool.getCompletedTaskCount());
            pool.shutdown();
        }
    }

    private void runDeltaPhase(Target t, List<IngestFile> files, long jobId) throws Exception {
        if (files.isEmpty()) {
            log.info("No DELTA files to process for {}", t);
            return;
        }
        log.info("Target={} DELTA phase starting (sequential, {} files)", t, files.size());
        for (IngestFile f : files) {
            checkStop(jobId);
            safeIngest(f);
        }
        log.info("Target={} DELTA phase complete", t);
    }

    private void safeIngest(IngestFile f) {
        Timer.Sample sample = Timer.start(registry);
        String status = "SUCCESS";
        try {
            ingestor.ingest(f);
            registry.counter("ingest.files.total", "target", f.target().name(), "kind", f.kind().name(), "status", "success").increment();
            archive(f);
        } catch (Exception e) {
            status = "FAILED";
            log.error("Ingestion failed for " + f.fileName(), e);
            registry.counter("ingest.files.total", "target", f.target().name(), "kind", f.kind().name(), "status", "failed").increment();
        } finally {
            sample.stop(registry.timer("ingest.file.duration", "target", f.target().name(), "kind", f.kind().name(), "status", status));
        }
    }

    private void archive(IngestFile f) {
        if (props.getArchiveDir() == null) return;
        try {
            Path archivePath = Paths.get(props.getArchiveDir());
            if (!Files.exists(archivePath)) {
                Files.createDirectories(archivePath);
            }
            Files.move(f.path(), archivePath.resolve(f.fileName()), StandardCopyOption.REPLACE_EXISTING);
            log.info("Archived file: {}", f.fileName());
        } catch (Exception e) {
            log.error("Failed to archive file: " + f.fileName(), e);
        }
    }

    private void checkStop(long jobId) {
        if (jobDao.isStopped(jobId)) {
            throw new RuntimeException("Job " + jobId + " was force-stopped");
        }
    }

    private static ThreadFactory namedThreads(String prefix) {
        return new ThreadFactory() {
            private final java.util.concurrent.atomic.AtomicInteger n = new java.util.concurrent.atomic.AtomicInteger();
            @Override public Thread newThread(Runnable r) {
                Thread th = new Thread(r, "ingest-" + prefix + "-" + n.incrementAndGet());
                th.setDaemon(false);
                return th;
            }
        };
    }
}
