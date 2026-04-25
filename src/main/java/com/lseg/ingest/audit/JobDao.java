package com.lseg.ingest.audit;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class JobDao {

    private final JdbcTemplate jdbc;

    public JobDao(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public long queueJob() {
        jdbc.update("INSERT INTO lseg_jobs (status) VALUES ('QUEUED')");
        return jdbc.queryForObject("SELECT LAST_INSERT_ID()", Long.class);
    }

    public Optional<Long> claimJob(String nodeId) {
        // Find the oldest QUEUED job
        Long jobId = jdbc.query("SELECT id FROM lseg_jobs WHERE status = 'QUEUED' ORDER BY id ASC LIMIT 1",
                (rs, rowNum) -> rs.getLong("id")).stream().findFirst().orElse(null);

        if (jobId == null) return Optional.empty();

        // Atomic claim
        int updated = jdbc.update("UPDATE lseg_jobs SET status = 'RUNNING', node_id = ?, started_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'QUEUED'",
                nodeId, jobId);

        return updated > 0 ? Optional.of(jobId) : Optional.empty();
    }

    public void updateStatus(long jobId, String status, String error) {
        jdbc.update("UPDATE lseg_jobs SET status = ?, finished_at = CASE WHEN ? IN ('COMPLETED', 'FAILED', 'STOPPED') THEN CURRENT_TIMESTAMP ELSE finished_at END, error_message = ? WHERE id = ?",
                status, status, error, jobId);
    }

    public String getStatus(long jobId) {
        return jdbc.queryForObject("SELECT status FROM lseg_jobs WHERE id = ?", String.class, jobId);
    }

    public boolean isStopped(long jobId) {
        String status = getStatus(jobId);
        return "STOPPED".equals(status);
    }
    
    public void forceStopAll() {
        jdbc.update("UPDATE lseg_jobs SET status = 'STOPPED' WHERE status IN ('RUNNING', 'QUEUED')");
    }
}
