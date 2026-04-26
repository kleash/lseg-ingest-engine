package com.lseg.ingest.orchestrator;

import com.lseg.ingest.audit.JobDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.Optional;
import java.util.UUID;

@Component
public class JobWorker {

    private static final Logger log = LoggerFactory.getLogger(JobWorker.class);

    private final String nodeId;
    private final JobDao jobDao;
    private final IngestOrchestrator orchestrator;

    public JobWorker(JobDao jobDao, IngestOrchestrator orchestrator) {
        this.jobDao = jobDao;
        this.orchestrator = orchestrator;
        this.nodeId = generateNodeId();
        log.info("JobWorker started with nodeId={}", nodeId);
    }

    @Scheduled(fixedDelay = 10000) // Poll every 10 seconds
    public void pollAndExecute() {
        Optional<Long> jobIdOpt;
        try {
            jobIdOpt = jobDao.claimJob(nodeId);
        } catch (Exception e) {
            log.warn("claimJob failed; will retry on next tick", e);
            return;
        }
        if (jobIdOpt.isEmpty()) return;

        long jobId = jobIdOpt.get();
        log.info("Node {} claimed job {}", nodeId, jobId);
        try {
            boolean ran = orchestrator.run(jobId);
            if (!ran) {
                log.info("Job {} not run on this tick (cluster lock unavailable); reverting to QUEUED", jobId);
                jobDao.updateStatus(jobId, "QUEUED", null);
                return;
            }
            // updateStatus is STOPPED-aware: it won't overwrite a stop.
            jobDao.updateStatus(jobId, "COMPLETED", null);
        } catch (Exception e) {
            log.error("Job {} failed", jobId, e);
            jobDao.updateStatus(jobId, "FAILED", truncate(e.getClass().getSimpleName() + ": " + e.getMessage()));
        }
    }

    private static String truncate(String s) {
        if (s == null) return null;
        return s.length() > 4000 ? s.substring(0, 4000) : s;
    }

    private String generateNodeId() {
        try {
            return InetAddress.getLocalHost().getHostName() + "-" + UUID.randomUUID().toString().substring(0, 8);
        } catch (Exception e) {
            return "unknown-" + UUID.randomUUID().toString().substring(0, 8);
        }
    }
}
