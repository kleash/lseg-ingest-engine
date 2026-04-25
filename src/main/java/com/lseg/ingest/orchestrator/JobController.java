package com.lseg.ingest.orchestrator;

import com.lseg.ingest.audit.JobDao;
import com.lseg.ingest.config.IngestProperties;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Job control endpoints. NOTE: not yet authenticated — security is a later phase.
 * The cluster GET_LOCK and the stop-aware status writes mean these endpoints are
 * safe against accidental concurrent triggers, but they ARE still capable of
 * stopping running work and must be locked down before production.
 */
@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobDao jobDao;
    private final IngestProperties props;

    public JobController(JobDao jobDao, IngestProperties props) {
        this.jobDao = jobDao;
        this.props = props;
    }

    @PostMapping("/trigger")
    public Map<String, Object> trigger(@RequestParam(required = false) String businessDate,
                                       @RequestParam(required = false) String inputDir) {
        String date = (businessDate != null && !businessDate.isEmpty()) ? businessDate : props.getBusinessDate();
        String dir = (inputDir != null && !inputDir.isEmpty()) ? inputDir : props.getInputDir();
        long jobId = jobDao.queueJob(date, dir);
        return Map.of("jobId", jobId, "status", "QUEUED", "businessDate", date, "inputDir", dir);
    }

    /** Stop one specific job by id. */
    @PostMapping("/stop")
    public Map<String, Object> stop(@RequestParam(required = false) Long jobId) {
        if (jobId == null) {
            jobDao.forceStopAll();
            return Map.of("scope", "all", "message", "Signal sent to stop all running/queued jobs");
        }
        jobDao.forceStop(jobId);
        return Map.of("scope", "single", "jobId", jobId, "message", "Stop signal sent");
    }

    @PostMapping("/restart")
    public Map<String, Object> restart(@RequestParam(required = false) Long jobId) {
        if (jobId != null) {
            jobDao.updateStatus(jobId, "QUEUED", null);
            return Map.of("jobId", jobId, "status", "QUEUED");
        }
        long newId = jobDao.queueJob(props.getBusinessDate(), props.getInputDir());
        return Map.of("jobId", newId, "status", "QUEUED");
    }

    @GetMapping("/status")
    public Map<String, Object> status(@RequestParam long jobId) {
        return Map.of("jobId", jobId, "status", jobDao.getStatus(jobId));
    }
}
