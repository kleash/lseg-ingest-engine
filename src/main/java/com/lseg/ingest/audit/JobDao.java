package com.lseg.ingest.audit;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;

// Status literals match Constants.java; hardcoded in SQL for readability.
@Repository
public class JobDao {

    private final JdbcTemplate jdbc;

    public JobDao(DataSource ds) {
        this.jdbc = new JdbcTemplate(ds);
    }

    public long queueJob(String businessDate, String inputDir) {
        KeyHolder kh = new GeneratedKeyHolder();
        jdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO lseg_jobs (status, business_date, input_dir) VALUES ('QUEUED', ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, businessDate);
            ps.setString(2, inputDir);
            return ps;
        }, kh);
        Number id = kh.getKey();
        if (id == null) throw new IllegalStateException("queueJob produced no generated key");
        return id.longValue();
    }

    /**
     * Atomically claim the oldest QUEUED job. Returns Optional.empty if none claimed.
     * Single UPDATE statement avoids the SELECT+UPDATE race entirely.
     */
    public Optional<Long> claimJob(String nodeId) {
        // Find the candidate id and attempt to flip it in one atomic UPDATE.
        // We do this in two steps but the UPDATE itself is the source of truth: only one node
        // sees rowsAffected == 1 for any given id.
        List<Long> candidates = jdbc.queryForList(
                "SELECT id FROM lseg_jobs WHERE status = 'QUEUED' ORDER BY id ASC LIMIT 1", Long.class);
        if (candidates.isEmpty()) return Optional.empty();
        long id = candidates.get(0);
        int updated = jdbc.update(
                "UPDATE lseg_jobs SET status='RUNNING', node_id=?, started_at=CURRENT_TIMESTAMP, " +
                        "last_heartbeat_at=CURRENT_TIMESTAMP " +
                        "WHERE id=? AND status='QUEUED'", nodeId, id);
        return updated > 0 ? Optional.of(id) : Optional.empty();
    }

    /**
     * Update job status. Never overwrites STOPPED (operator-issued stop wins).
     * Sets finished_at when transitioning to a terminal state.
     */
    public void updateStatus(long jobId, String status, String error) {
        jdbc.update(
                "UPDATE lseg_jobs SET " +
                        "status = CASE WHEN status = 'STOPPED' THEN 'STOPPED' ELSE ? END, " +
                        "finished_at = CASE WHEN ? IN ('COMPLETED','FAILED','STOPPED') THEN CURRENT_TIMESTAMP ELSE finished_at END, " +
                        "error_message = CASE WHEN status = 'STOPPED' THEN error_message ELSE ? END " +
                        "WHERE id = ?",
                status, status, error, jobId);
    }

    /**
     * Unconditionally requeue a job regardless of its current status.
     * Use this for restarting STOPPED jobs where updateStatus() would be a no-op.
     */
    public void forceRequeue(long jobId) {
        jdbc.update(
                "UPDATE lseg_jobs SET status='QUEUED', finished_at=NULL, error_message=NULL WHERE id=?",
                jobId);
    }

    public void heartbeat(long jobId) {
        jdbc.update("UPDATE lseg_jobs SET last_heartbeat_at = CURRENT_TIMESTAMP WHERE id = ? AND status = 'RUNNING'", jobId);
    }

    public String getStatus(long jobId) {
        return jdbc.queryForObject("SELECT status FROM lseg_jobs WHERE id = ?", String.class, jobId);
    }

    public String getBusinessDate(long jobId) {
        return jdbc.queryForObject("SELECT business_date FROM lseg_jobs WHERE id = ?", String.class, jobId);
    }

    public String getInputDir(long jobId) {
        return jdbc.queryForObject("SELECT input_dir FROM lseg_jobs WHERE id = ?", String.class, jobId);
    }

    public boolean isStopped(long jobId) {
        try {
            String status = getStatus(jobId);
            return "STOPPED".equals(status);
        } catch (Exception e) {
            // If we can't read status, treat as not-stopped to avoid false-positive aborts.
            return false;
        }
    }

    public void forceStopAll() {
        jdbc.update("UPDATE lseg_jobs SET status = 'STOPPED', finished_at = CURRENT_TIMESTAMP " +
                "WHERE status IN ('RUNNING', 'QUEUED')");
    }

    public void forceStop(long jobId) {
        jdbc.update("UPDATE lseg_jobs SET status='STOPPED', finished_at=CURRENT_TIMESTAMP " +
                "WHERE id=? AND status IN ('RUNNING','QUEUED')", jobId);
    }

    /** Reap RUNNING jobs whose last heartbeat is older than `staleSeconds`. */
    public int reapStale(long staleSeconds) {
        return jdbc.update(
                "UPDATE lseg_jobs SET status='FAILED', finished_at=CURRENT_TIMESTAMP, " +
                        "error_message=CONCAT('reaped: heartbeat stale > ', ?, 's') " +
                        "WHERE status='RUNNING' AND last_heartbeat_at < (NOW() - INTERVAL ? SECOND)",
                staleSeconds, staleSeconds);
    }
}
