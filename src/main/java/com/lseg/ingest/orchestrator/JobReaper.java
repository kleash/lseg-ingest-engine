package com.lseg.ingest.orchestrator;

import com.lseg.ingest.audit.JobDao;
import com.lseg.ingest.config.IngestProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import static com.lseg.ingest.Constants.*;

/**
 * Periodically marks RUNNING jobs FAILED if their heartbeat is older than the configured
 * staleJobTimeoutSeconds (default 3 hours). Provides automatic recovery for nodes that
 * die without releasing their job rows. Operators can still flip jobs manually via
 * forceStop / restart endpoints; this is a safety net, not the primary path.
 */
@Component
public class JobReaper {

    private static final Logger log = LoggerFactory.getLogger(JobReaper.class);

    private final JobDao jobDao;
    private final IngestProperties props;

    public JobReaper(JobDao jobDao, IngestProperties props) {
        this.jobDao = jobDao;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "#{${ingest.reaper.pollIntervalSeconds:60} * 1000}")
    public void reap() {
        if (!props.getReaper().isEnabled()) return;
        long stale = props.getReaper().getStaleJobTimeoutSeconds();
        try {
            int reaped = jobDao.reapStale(stale);
            if (reaped > 0) {
                log.warn("Reaped {} stale RUNNING job(s) (heartbeat older than {}s)", reaped, stale);
            }
        } catch (Exception e) {
            log.error("Reaper iteration failed", e);
        }
    }
}
