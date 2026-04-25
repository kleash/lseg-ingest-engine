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
    }

    @Scheduled(fixedDelay = 10000) // Poll every 10 seconds
    public void pollAndExecute() {
        Optional<Long> jobIdOpt = jobDao.claimJob(nodeId);
        if (jobIdOpt.isPresent()) {
            long jobId = jobIdOpt.get();
            log.info("Node {} claimed job {}", nodeId, jobId);
            try {
                orchestrator.run(jobId);
                jobDao.updateStatus(jobId, "COMPLETED", null);
            } catch (Exception e) {
                log.error("Job {} failed", jobId, e);
                jobDao.updateStatus(jobId, "FAILED", e.getMessage());
            }
        }
    }

    private String generateNodeId() {
        try {
            return InetAddress.getLocalHost().getHostName() + "-" + UUID.randomUUID().toString().substring(0, 8);
        } catch (Exception e) {
            return "unknown-" + UUID.randomUUID().toString().substring(0, 8);
        }
    }
}
