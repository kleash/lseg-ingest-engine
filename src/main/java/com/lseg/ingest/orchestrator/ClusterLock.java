package com.lseg.ingest.orchestrator;

import com.lseg.ingest.config.IngestProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Cluster-wide singleton lock implemented via MariaDB GET_LOCK / RELEASE_LOCK.
 *
 * Holds a dedicated Connection for the lifetime of an acquired lock. The DB releases
 * a session-level GET_LOCK automatically when the connection drops, so a crashed node
 * cannot keep the lock indefinitely (bounded by the DB's wait_timeout). When held, no
 * other node can acquire it, enforcing strict one-job-at-a-time across the cluster.
 *
 * Usage:
 *   try (ClusterLock.Handle h = clusterLock.tryAcquire()) {
 *       if (!h.acquired()) ... // back off
 *       ... do work ...
 *   }
 */
@Component
public class ClusterLock {

    private static final Logger log = LoggerFactory.getLogger(ClusterLock.class);

    private final DataSource ds;
    private final IngestProperties props;

    public ClusterLock(DataSource ds, IngestProperties props) {
        this.ds = ds;
        this.props = props;
    }

    public Handle tryAcquire() throws SQLException {
        String name = props.getCluster().getLockName();
        Connection conn = ds.getConnection();
        try {
            try (PreparedStatement ps = conn.prepareStatement("SELECT GET_LOCK(?, 0)")) {
                ps.setString(1, name);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        int v = rs.getInt(1);
                        boolean ok = v == 1;
                        if (!ok) {
                            log.info("ClusterLock '{}' not acquired (held elsewhere)", name);
                            conn.close();
                            return new Handle(null, name, false);
                        }
                        log.info("ClusterLock '{}' acquired", name);
                        return new Handle(conn, name, true);
                    }
                    conn.close();
                    return new Handle(null, name, false);
                }
            }
        } catch (SQLException e) {
            try { conn.close(); } catch (SQLException ignore) {}
            throw e;
        }
    }

    public static final class Handle implements AutoCloseable {
        private final Connection conn;
        private final String name;
        private final boolean acquired;

        Handle(Connection conn, String name, boolean acquired) {
            this.conn = conn;
            this.name = name;
            this.acquired = acquired;
        }

        public boolean acquired() { return acquired; }

        @Override
        public void close() {
            if (conn == null) return;
            try (PreparedStatement ps = conn.prepareStatement("SELECT RELEASE_LOCK(?)")) {
                ps.setString(1, name);
                ps.execute();
                log.info("ClusterLock '{}' released", name);
            } catch (SQLException e) {
                log.warn("Failed to release cluster lock '{}': {}", name, e.getMessage());
            } finally {
                try { conn.close(); } catch (SQLException ignore) {}
            }
        }
    }
}
