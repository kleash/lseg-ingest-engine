package com.lseg.ingest.orchestrator;

import com.lseg.ingest.audit.JobDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.Optional;
import java.util.UUID;

import static com.lseg.ingest.Constants.*;

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

    @Scheduled(fixedDelay = 1000) // Poll every 1 second
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
                // If it didn't run, orchestrator already rolled it back to QUEUED or it was stopped.
                return;
            }
            // If we get here, orchestrator.run() returned true, meaning it finished successfully.
            // Note: updateStatus is STOPPED-aware; it won't overwrite a STOPPED status.
            jobDao.updateStatus(jobId, STATUS_COMPLETED, null);
        } catch (Exception e) {
            log.error("Job {} failed with exception: {}", jobId, e.getMessage(), e);
            jobDao.updateStatus(jobId, STATUS_FAILED, truncate(e.getClass().getSimpleName() + ": " + e.getMessage()));
        }
    }

    private static String truncate(String s) {
        if (s == null) return null;
        return s.length() > 4000 ? s.substring(0, 4000) : s;
    }

    private String generateNodeId() {
        try {
            String host = InetAddress.getLocalHost().getHostName();
            return host + "-" + System.currentTimeMillis() % 100000 + "-" + UUID.randomUUID().toString().substring(0, 4);
        } catch (Exception e) {
            return "unknown-" + UUID.randomUUID().toString().substring(0, 8);
        }
    }
}
