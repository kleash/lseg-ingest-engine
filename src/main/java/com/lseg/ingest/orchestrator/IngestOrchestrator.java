package com.lseg.ingest.orchestrator;

import com.lseg.ingest.audit.FileAuditDao;
import com.lseg.ingest.audit.JobDao;
import com.lseg.ingest.config.IngestProperties;
import com.lseg.ingest.event.TargetIngestCompletedEvent;
import com.lseg.ingest.load.FileIngestor;
import com.lseg.ingest.plan.*;
import com.lseg.ingest.sanity.FileSanityCheck;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
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
 *   3. Validate business date is within the configured maxBusinessDateAgeDays window.
 *   4. Scan + classify files; drop already-SUCCESSful files within auditLookbackDays.
 *   5. Pre-ingest sanity check; failures get SKIPPED_SANITY.
 *   6. Phase 1 [parallel]: ORGS, ASSETS, QUOTES, DSS_BONDS — each runs INT files in
 *      parallel then DELTA files sequentially. DELTA never runs while INT is in flight.
 *   7. Phase 2 [async]: PRICING files are submitted to a dedicated 3-thread background
 *      executor ({@code pricingExecutor}) and run() returns immediately. The cluster lock
 *      is released before PRICING starts, so new Phase 1 jobs can begin within seconds.
 *      {@code TargetIngestCompletedEvent(PRICING)} fires from the background thread when
 *      all pricing files finish — after the job row shows COMPLETED in lseg_jobs.
 *      Note: synchronous {@code @EventListener} handlers for PRICING events execute on a
 *      pricingExecutor thread; keep them fast or annotate with {@code @Async}.
 *   8. Cooperative stop: jobDao.isStopped(jobId) is polled at submit boundaries AND inside
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
    private final ApplicationEventPublisher eventPublisher;
    private final ExecutorService pricingExecutor;

    public IngestOrchestrator(FileScanner scanner, FileSanityCheck sanity, FileAuditDao audit, JobDao jobDao,
                              FileIngestor ingestor, IngestProperties props, MeterRegistry registry,
                              ClusterLock clusterLock, ApplicationEventPublisher eventPublisher) {
        this.scanner = scanner;
        this.sanity = sanity;
        this.audit = audit;
        this.jobDao = jobDao;
        this.ingestor = ingestor;
        this.props = props;
        this.registry = registry;
        this.clusterLock = clusterLock;
        this.eventPublisher = eventPublisher;
        this.pricingExecutor = Executors.newFixedThreadPool(
                Math.max(1, props.getThreads().getPricingThreads()),
                daemonThreads("pricing"));
    }

    @PreDestroy
    public void shutdownPricingExecutor() {
        pricingExecutor.shutdown();
        try {
            if (!pricingExecutor.awaitTermination(5, TimeUnit.MINUTES)) {
                pricingExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            pricingExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /** @return true if the run completed (success or failed); false if the cluster lock was unavailable. */
    public boolean run(long jobId) throws Exception {
        MDC.put(MDC_JOB_ID, String.valueOf(jobId));
        Timer.Sample overallSample = Timer.start(registry);
        ScheduledExecutorService heartbeat = null;

        try (ClusterLock.Handle lock = clusterLock.tryAcquire()) {
            if (!lock.acquired()) {
                log.warn("Cluster lock unavailable; another node is running. Leaving job {} QUEUED.", jobId);
                jobDao.updateStatus(jobId, STATUS_QUEUED, null);
                return false;
            }

            heartbeat = startHeartbeat(jobId);
            checkStop(jobId);

            String businessDate = jobDao.getBusinessDate(jobId);
            String jobDir = jobDao.getInputDir(jobId);
            String effectiveInputDir = (jobDir != null && !jobDir.isEmpty())
                    ? jobDir : props.getInputDir();
            if (!effectiveInputDir.equals(props.getInputDir())) {
                log.info("Job {} inputDir={} overrides config inputDir={}", jobId, effectiveInputDir, props.getInputDir());
            }

            // Reject jobs whose business date is too old to prevent processing stale data.
            LocalDate bd = FileAuditDao.parseBusinessDate(businessDate);
            long ageDays = ChronoUnit.DAYS.between(bd, LocalDate.now());
            if (ageDays > props.getMaxBusinessDateAgeDays()) {
                throw new IllegalStateException(String.format(
                        "Business date %s is %d days old (max allowed: %d). Failing fast.",
                        businessDate, ageDays, props.getMaxBusinessDateAgeDays()));
            }

            List<IngestFile> all = scanner.scan(effectiveInputDir);

            // Load only SUCCESS records within the audit lookback window — avoids a full table scan
            // as the audit table grows over time. Files outside the window are re-ingested (idempotent).
            Set<String> alreadyDone = audit.loadSuccessFileNames(props.getAuditLookbackDays());
            List<IngestFile> remaining = new ArrayList<>(all.size());
            for (IngestFile f : all) {
                if (alreadyDone.contains(f.fileName())) {
                    log.debug("Skipping already-ingested file {}", f.fileName());
                } else {
                    remaining.add(f);
                }
            }
            log.info("Files: total={} already-ingested={} remaining={}",
                    all.size(), all.size() - remaining.size(), remaining.size());

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

            IngestPlan plan = new IngestPlan(good);
            log.info("Ingestion Plan built: {}", plan.summary());

            ExecutorService perTargetPool = Executors.newFixedThreadPool(
                    Math.max(1, props.getThreads().getDeltaTargetsParallel()),
                    daemonThreads("target"));

            List<Future<?>> phase1Futures = new ArrayList<>();
            for (Target t : Target.values()) {
                if (t == Target.PRICING) continue;
                phase1Futures.add(perTargetPool.submit(() -> runTarget(t, plan, jobId, businessDate)));
            }

            boolean anyTargetError = false;
            try {
                for (Future<?> f : phase1Futures) {
                    try {
                        waitWithHeartbeat(f, jobId, "target pipeline");
                    } catch (ExecutionException e) {
                        anyTargetError = true;
                        log.error("Target pipeline failed unexpectedly", e.getCause());
                        registry.counter(METRIC_TARGET_ERRORS).increment();
                    }
                }
            } finally {
                perTargetPool.shutdown();
                if (!perTargetPool.awaitTermination(2, TimeUnit.MINUTES)) {
                    perTargetPool.shutdownNow();
                }
            }

            // PRICING files are always Kind.INT — the LSEG pricing feed has no delta variant.
            // plan.deltaFor(Target.PRICING) is always empty by design.
            if (!jobDao.isStopped(jobId)) {
                submitPricingAsync(plan, jobId, businessDate);
            } else {
                eventPublisher.publishEvent(
                        new TargetIngestCompletedEvent(Target.PRICING, businessDate, jobId, false, 0));
            }

            if (jobDao.isStopped(jobId)) {
                log.warn("Job {} ended via STOP signal", jobId);
                return false;
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

    private void submitPricingAsync(IngestPlan plan, long jobId, String businessDate) {
        List<IngestFile> pricingFiles = plan.intFor(Target.PRICING);
        int totalFiles = pricingFiles.size();

        if (pricingFiles.isEmpty()) {
            log.info("No PRICING files to process; firing event immediately");
            eventPublisher.publishEvent(
                    new TargetIngestCompletedEvent(Target.PRICING, businessDate, jobId, true, 0));
            return;
        }

        log.info("Submitting {} PRICING files to background executor (threads={})",
                totalFiles, props.getThreads().getPricingThreads());

        AtomicBoolean anyFailed = new AtomicBoolean(false);
        List<CompletableFuture<Void>> futures = new ArrayList<>(totalFiles);
        for (IngestFile f : pricingFiles) {
            futures.add(CompletableFuture.runAsync(() -> {
                MDC.put(MDC_JOB_ID, String.valueOf(jobId));
                try {
                    safeIngest(f, jobId, businessDate);
                } catch (Exception e) {
                    anyFailed.set(true);
                    log.error("Background PRICING ingest failed for {}", f.fileName(), e);
                } finally {
                    MDC.remove(MDC_JOB_ID);
                }
            }, pricingExecutor));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                .whenComplete((v, ex) -> {
                    if (ex != null) {
                        log.error("Background PRICING executor error", ex);
                    }
                    boolean success = !anyFailed.get() && ex == null;
                    log.info("Background PRICING complete: files={} success={}", totalFiles, success);
                    eventPublisher.publishEvent(
                            new TargetIngestCompletedEvent(Target.PRICING, businessDate, jobId, success, totalFiles));
                });
    }

    private void runTarget(Target t, IngestPlan plan, long jobId, String businessDate) {
        log.info("Starting target pipeline for {}", t);
        boolean success = false;
        int fileCount = plan.intFor(t).size() + plan.deltaFor(t).size();
        try {
            checkStop(jobId);
            runIntPhase(t, plan.intFor(t), jobId, businessDate);
            checkStop(jobId);
            runDeltaPhase(t, plan.deltaFor(t), jobId, businessDate);
            success = true;
        } catch (Exception e) {
            log.error("Pipeline for {} aborted with error", t, e);
            throw new RuntimeException(e);
        } finally {
            log.info("Target pipeline for {} finished (success={}, files={})", t, success, fileCount);
            eventPublisher.publishEvent(
                    new TargetIngestCompletedEvent(t, businessDate, jobId, success, fileCount));
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
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (IngestFile f : files) {
                if (jobDao.isStopped(jobId)) break;
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
