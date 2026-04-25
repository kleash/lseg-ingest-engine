package com.lseg.ingest.orchestrator;

import com.lseg.ingest.audit.JobDao;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobDao jobDao;

    public JobController(JobDao jobDao) {
        this.jobDao = jobDao;
    }

    @PostMapping("/trigger")
    public Map<String, Object> trigger() {
        long jobId = jobDao.queueJob();
        return Map.of("jobId", jobId, "status", "QUEUED", "message", "Job queued successfully");
    }

    @PostMapping("/stop")
    public Map<String, String> stop() {
        jobDao.forceStopAll();
        return Map.of("message", "Signal sent to stop all running jobs");
    }

    @PostMapping("/restart")
    public Map<String, Object> restart(@RequestParam(required = false) Long jobId) {
        if (jobId != null) {
            jobDao.updateStatus(jobId, "QUEUED", null);
            return Map.of("jobId", jobId, "status", "QUEUED");
        } else {
            long newId = jobDao.queueJob();
            return Map.of("jobId", newId, "status", "QUEUED");
        }
    }
}
