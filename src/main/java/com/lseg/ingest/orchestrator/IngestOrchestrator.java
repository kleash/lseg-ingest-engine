package com.lseg.ingest.orchestrator;

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
import org.slf4j.MDC;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.lseg.ingest.Constants.*;

/**
 * Drives the end-to-end run for one queued job.
 *
 * Flow:
 *   1. Acquire cluster GET_LOCK (strict one-at-a-time across the cluster).
 *   2. Start a heartbeat task that updates lseg_jobs.last_heartbeat_at.
 *   3. Scan + classify files; drop already-SUCCESSful files (audit table).
 *   4. Pre-ingest sanity check; failures get SKIPPED_SANITY.
 *   5. For each Target in parallel: run all INT files (parallel within target),
 *      then all DELTA files (sequential by seq). DELTA never runs while INT is in flight
 *      for the same target.
 *   6. Cooperative stop: jobDao.isStopped(jobId) is polled at submit boundaries AND inside
 *      the row loop, so a stop signal aborts within seconds, not minutes.
 */
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
    private final ClusterLock clusterLock;

    public IngestOrchestrator(FileScanner scanner, FileSanityCheck sanity, FileAuditDao audit, JobDao jobDao,
                              FileIngestor ingestor, IngestProperties props, MeterRegistry registry,
                              ClusterLock clusterLock) {
        this.scanner = scanner;
        this.sanity = sanity;
        this.audit = audit;
        this.jobDao = jobDao;
        this.ingestor = ingestor;
        this.props = props;
        this.registry = registry;
        this.clusterLock = clusterLock;
    }

    /** @return true if the run completed (success or failed); false if the cluster lock was unavailable. */
    public boolean run(long jobId) throws Exception {
        // 1. Setup logging context so every log line shows the current Job ID.
        MDC.put(MDC_JOB_ID, String.valueOf(jobId));
        Timer.Sample overallSample = Timer.start(registry);
        ScheduledExecutorService heartbeat = null;

        // 2. CONSTRAINT: Acquire a global database lock. 
        // This ensures that even in a multi-instance cluster, only ONE node processes files at a time.
        try (ClusterLock.Handle lock = clusterLock.tryAcquire()) {
            if (!lock.acquired()) {
                log.warn("Cluster lock unavailable; another node is running. Leaving job {} QUEUED.", jobId);
                // Roll job back to QUEUED so it's re-tried on next poll.
                jobDao.updateStatus(jobId, STATUS_QUEUED, null);
                return false;
            }

            // 3. Start a background heartbeat. 
            // If this node crashes, the heartbeat stops, allowing the JobReaper to eventually unlock the job.
            heartbeat = startHeartbeat(jobId);
            checkStop(jobId);

            // 4. Retrieve job's stored values (Date and Input Directory).
            String businessDate = jobDao.getBusinessDate(jobId);
            String jobDir = jobDao.getInputDir(jobId);
            if (jobDir != null && !jobDir.isEmpty() && !jobDir.equals(props.getInputDir())) {
                log.info("Job {} inputDir={} overrides config inputDir={}", jobId, jobDir, props.getInputDir());
                props.setInputDir(jobDir);
            }

            // 5. Look at the folder and parse file names into Java objects.
            List<IngestFile> all = scanner.scan();
            
            // 6. VALIDATION/IDEMPOTENCY: Query the database to find files we've already successfully finished.
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

            // 7. SANITY CHECK: Before doing expensive database work, check the files.
            // This reads the first 50 lines looking for the correct headers.
            List<IngestFile> good = new ArrayList<>(remaining.size());
            for (IngestFile f : remaining) {
                checkStop(jobId);
                FileSanityCheck.Result r = sanity.check(f, businessDate);
                if (!r.ok()) {
                    log.warn("CRITICAL SANITY FAIL {}: {}", f.fileName(), r.reason());
                    audit.markSkippedSanity(f, r.reason(), businessDate);
                    registry.counter(METRIC_SANITY_FAILURES, TAG_TARGET, f.target().name()).increment();
                } else {
                    good.add(f);
                }
            }
            
            // 8. Organizes the healthy files into an IngestPlan (sorting by Target and Sequence).
            IngestPlan plan = new IngestPlan(good);
            log.info("Ingestion Plan built: {}", plan.summary());

            // 9. PARALLEL EXECUTION: Fire off a separate thread for each target table.
            ExecutorService perTargetPool = Executors.newFixedThreadPool(
                    Math.max(1, props.getThreads().getDeltaTargetsParallel()),
                    daemonThreads("target"));

            List<Future<?>> targetFutures = new ArrayList<>();
            for (Target t : Target.values()) {
                targetFutures.add(perTargetPool.submit(() -> runTarget(t, plan, jobId, businessDate)));
            }
            
            // 10. Wait for all target pipelines to finish.
            boolean anyTargetError = false;
            for (Future<?> f : targetFutures) {
                try {
                    waitWithHeartbeat(f, jobId, "target pipeline");
                } catch (ExecutionException e) {
                    anyTargetError = true;
                    log.error("Target pipeline failed unexpectedly", e.getCause());
                    registry.counter(METRIC_TARGET_ERRORS).increment();
                }
            }
            perTargetPool.shutdown();
            if (!perTargetPool.awaitTermination(2, TimeUnit.MINUTES)) {
                perTargetPool.shutdownNow();
            }

            if (jobDao.isStopped(jobId)) {
                log.warn("Job {} ended via STOP signal", jobId);
                return false; // return false so JobWorker doesn't flip STOPPED to COMPLETED
            }
            if (anyTargetError) {
                throw new RuntimeException("One or more target pipelines failed");
            }
            return true;
        } catch (Exception e) {
            log.error("CRITICAL: Orchestrator encountered a fatal error", e);
            registry.counter(METRIC_ORCHESTRATOR_ERRORS).increment();
            throw e;
        } finally {
            if (heartbeat != null) heartbeat.shutdownNow();
            overallSample.stop(registry.timer(METRIC_OVERALL_DURATION));
            log.info("Ingestion session finished for job {}.", jobId);
            MDC.remove(MDC_JOB_ID);
        }
    }

    private void waitWithHeartbeat(Future<?> f, long jobId, String description) throws Exception {
        while (!f.isDone()) {
            try {
                f.get(1, TimeUnit.MINUTES);
            } catch (TimeoutException e) {
                log.info("Job {} is still running {}...", jobId, description);
            }
        }
    }

    private ScheduledExecutorService startHeartbeat(long jobId) {
        long intervalSec = Math.max(5, props.getCluster().getHeartbeatIntervalSeconds());
        ScheduledExecutorService ex = Executors.newSingleThreadScheduledExecutor(daemonThreads("heartbeat"));
        ex.scheduleAtFixedRate(() -> {
            try { jobDao.heartbeat(jobId); } catch (Exception e) { log.warn("Heartbeat failed for {}", jobId, e); }
        }, intervalSec, intervalSec, TimeUnit.SECONDS);
        return ex;
    }

    private void runTarget(Target t, IngestPlan plan, long jobId, String businessDate) {
        log.info("Starting target pipeline for {}", t);
        try {
            checkStop(jobId);
            runIntPhase(t, plan.intFor(t), jobId, businessDate);
            checkStop(jobId);
            runDeltaPhase(t, plan.deltaFor(t), jobId, businessDate);
        } catch (Exception e) {
            log.error("Pipeline for {} aborted with error", t, e);
            throw new RuntimeException(e);
        } finally {
            log.info("Target pipeline for {} finished", t);
        }
    }

    private void runIntPhase(Target t, List<IngestFile> files, long jobId, String businessDate) throws InterruptedException {
        if (files.isEmpty()) {
            log.info("No INT files to process for {}", t);
            return;
        }
        int threads = Math.min(props.getThreads().getIntPerTable(), files.size());
        log.info("Target={} INT phase starting with {} files using {} threads", t, files.size(), threads);

        ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(threads,
                daemonThreads("int-" + t.name().toLowerCase()));
        AtomicBoolean stopped = new AtomicBoolean(false);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (IngestFile f : files) {
                if (stopped.get() || jobDao.isStopped(jobId)) break;
                futures.add(pool.submit(() -> {
                    try {
                        if (jobDao.isStopped(jobId)) return;
                        safeIngest(f, jobId, businessDate);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            boolean anyIntError = false;
            for (Future<?> f : futures) {
                try {
                    waitWithHeartbeat(f, jobId, "INT task for " + t);
                } catch (ExecutionException e) {
                    anyIntError = true;
                    log.error("Internal INT task failed", e.getCause());
                } catch (Exception e) {
                    anyIntError = true;
                    log.error("Error waiting for INT task", e);
                }
            }
            if (anyIntError) {
                throw new RuntimeException("One or more INT tasks failed for " + t);
            }
        } finally {
            log.info("Target={} INT phase complete. Pool status: completed={}", t, pool.getCompletedTaskCount());
            pool.shutdown();
        }
    }

    private void runDeltaPhase(Target t, List<IngestFile> files, long jobId, String businessDate) throws Exception {
        if (files.isEmpty()) {
            log.info("No DELTA files to process for {}", t);
            return;
        }
        log.info("Target={} DELTA phase starting (sequential, {} files)", t, files.size());
        for (IngestFile f : files) {
            if (jobDao.isStopped(jobId)) {
                log.warn("Stop signaled; aborting DELTA phase for {} at file {}", t, f.fileName());
                break;
            }
            safeIngest(f, jobId, businessDate);
        }
        log.info("Target={} DELTA phase complete", t);
    }

    private void safeIngest(IngestFile f, long jobId, String businessDate) {
        Timer.Sample sample = Timer.start(registry);
        String status = AUDIT_SUCCESS;
        try {
            ingestor.ingest(f, jobId, businessDate);
            registry.counter(METRIC_FILES_TOTAL, TAG_TARGET, f.target().name(), TAG_KIND, f.kind().name(), TAG_STATUS, "success").increment();
            archive(f);
        } catch (Exception e) {
            status = AUDIT_FAILED;
            log.error("Ingestion failed for " + f.fileName(), e);
            registry.counter(METRIC_FILES_TOTAL, TAG_TARGET, f.target().name(), TAG_KIND, f.kind().name(), TAG_STATUS, "failed").increment();
            throw new RuntimeException(e);
        } finally {
            sample.stop(registry.timer("ingest.file.duration", TAG_TARGET, f.target().name(), TAG_KIND, f.kind().name(), TAG_STATUS, status.toLowerCase()));
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
            registry.counter(METRIC_ARCHIVE_ERRORS).increment();
        }
    }

    private void checkStop(long jobId) {
        if (jobDao.isStopped(jobId)) {
            throw new RuntimeException("Job " + jobId + " was force-stopped");
        }
    }

    private static ThreadFactory daemonThreads(String prefix) {
        return new ThreadFactory() {
            private final java.util.concurrent.atomic.AtomicInteger n = new java.util.concurrent.atomic.AtomicInteger();
            @Override public Thread newThread(Runnable r) {
                Thread th = new Thread(r, "ingest-" + prefix + "-" + n.incrementAndGet());
                th.setDaemon(true);
                return th;
            }
        };
    }
}
