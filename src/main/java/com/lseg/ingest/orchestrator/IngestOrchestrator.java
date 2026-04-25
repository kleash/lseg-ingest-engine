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
        MDC.put("jobId", String.valueOf(jobId));
        Timer.Sample overallSample = Timer.start(registry);
        ScheduledExecutorService heartbeat = null;
        try (ClusterLock.Handle lock = clusterLock.tryAcquire()) {
            if (!lock.acquired()) {
                log.warn("Cluster lock unavailable; another node is running. Leaving job {} QUEUED.", jobId);
                // Roll job back to QUEUED so it's re-tried on next poll.
                jobDao.updateStatus(jobId, "QUEUED", null);
                return false;
            }

            heartbeat = startHeartbeat(jobId);
            checkStop(jobId);

            // Override businessDate / inputDir with the job's stored values, if present.
            String jobDate = jobDao.getBusinessDate(jobId);
            if (jobDate != null && !jobDate.isEmpty() && !jobDate.equals(props.getBusinessDate())) {
                log.info("Job {} businessDate={} overrides config businessDate={}", jobId, jobDate, props.getBusinessDate());
                props.setBusinessDate(jobDate);
            }
            String jobDir = jobDao.getInputDir(jobId);
            if (jobDir != null && !jobDir.isEmpty() && !jobDir.equals(props.getInputDir())) {
                log.info("Job {} inputDir={} overrides config inputDir={}", jobId, jobDir, props.getInputDir());
                props.setInputDir(jobDir);
            }

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
                    daemonThreads("target"));

            List<Future<?>> targetFutures = new ArrayList<>();
            for (Target t : Target.values()) {
                targetFutures.add(perTargetPool.submit(() -> runTarget(t, plan, jobId)));
            }
            boolean anyTargetError = false;
            for (Future<?> f : targetFutures) {
                try {
                    f.get();
                } catch (ExecutionException e) {
                    anyTargetError = true;
                    log.error("Target pipeline failed unexpectedly", e.getCause());
                    registry.counter("ingest.target.errors").increment();
                }
            }
            perTargetPool.shutdown();
            if (!perTargetPool.awaitTermination(2, TimeUnit.MINUTES)) {
                perTargetPool.shutdownNow();
            }

            if (jobDao.isStopped(jobId)) {
                log.warn("Job {} ended via STOP signal", jobId);
                return true; // run "completed" (was stopped); JobWorker won't override STOPPED.
            }
            if (anyTargetError) {
                throw new RuntimeException("One or more target pipelines failed");
            }
            return true;
        } catch (Exception e) {
            log.error("CRITICAL: Orchestrator encountered a fatal error", e);
            registry.counter("ingest.orchestrator.errors").increment();
            throw e;
        } finally {
            if (heartbeat != null) heartbeat.shutdownNow();
            overallSample.stop(registry.timer("ingest.overall.duration"));
            log.info("Ingestion session finished for job {}.", jobId);
            MDC.remove("jobId");
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

    private void runTarget(Target t, IngestPlan plan, long jobId) {
        log.info("Starting target pipeline for {}", t);
        try {
            checkStop(jobId);
            runIntPhase(t, plan.intFor(t), jobId);
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

        ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(threads,
                daemonThreads("int-" + t.name().toLowerCase()));
        AtomicBoolean stopped = new AtomicBoolean(false);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (IngestFile f : files) {
                if (stopped.get() || jobDao.isStopped(jobId)) break;
                futures.add(pool.submit(() -> {
                    if (jobDao.isStopped(jobId)) return;
                    safeIngest(f, jobId);
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

    private void runDeltaPhase(Target t, List<IngestFile> files, long jobId) {
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
            safeIngest(f, jobId);
        }
        log.info("Target={} DELTA phase complete", t);
    }

    private void safeIngest(IngestFile f, long jobId) {
        Timer.Sample sample = Timer.start(registry);
        String status = "SUCCESS";
        try {
            ingestor.ingest(f, jobId);
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
            registry.counter("ingest.archive.errors").increment();
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
