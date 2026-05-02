package com.lseg.ingest.orchestrator;

import com.lseg.ingest.audit.JobDao;
import com.lseg.ingest.config.IngestProperties;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

import static com.lseg.ingest.Constants.*;

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
        if (businessDate == null || businessDate.isEmpty()){
            return Map.of("result", "failed, business date is required");
        }

        String dir = (inputDir != null && !inputDir.isEmpty()) ? inputDir : props.getInputDir();
        long jobId = jobDao.queueJob(businessDate, dir);
        return Map.of("jobId", jobId, "status", STATUS_QUEUED, "businessDate", businessDate, "inputDir", dir);
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

    /**
     * Re-queue a job for re-processing. Works for any terminal status including STOPPED.
     * Returns the actual resulting status read back from the database.
     */
    @PostMapping("/restart")
    public Map<String, Object> restart(@RequestParam Long jobId) {
        String current = jobDao.getStatus(jobId);
        if (STATUS_STOPPED.equals(current)) {
            jobDao.forceRequeue(jobId);
        } else {
            jobDao.updateStatus(jobId, STATUS_QUEUED, null);
        }
        return Map.of("jobId", jobId, "status", jobDao.getStatus(jobId));
    }

    @GetMapping("/status")
    public Map<String, Object> status(@RequestParam long jobId) {
        return Map.of("jobId", jobId, "status", jobDao.getStatus(jobId));
    }
}
