package com.lseg.ingest.audit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.sql.DataSource;

import static com.lseg.ingest.Constants.*;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class JobDaoTest {

    @Container
    static MariaDBContainer<?> mariadb = new MariaDBContainer<>("mariadb:10.11")
            .withDatabaseName("lseg_test")
            .withUsername("test")
            .withPassword("test");

    private JobDao jobDao;
    private JdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(mariadb.getJdbcUrl());
        cfg.setUsername(mariadb.getUsername());
        cfg.setPassword(mariadb.getPassword());
        DataSource ds = new HikariDataSource(cfg);
        jdbc = new JdbcTemplate(ds);
        jobDao = new JobDao(ds);
        createSchema();
    }

    private void createSchema() {
        jdbc.execute("DROP TABLE IF EXISTS lseg_jobs");
        jdbc.execute("""
            CREATE TABLE lseg_jobs (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                status VARCHAR(20) NOT NULL,
                business_date VARCHAR(10),
                input_dir VARCHAR(500),
                job_type VARCHAR(32) NOT NULL DEFAULT 'MAIN',
                node_id VARCHAR(200),
                started_at TIMESTAMP NULL,
                finished_at TIMESTAMP NULL,
                last_heartbeat_at TIMESTAMP NULL,
                error_message TEXT
            )""");
    }

    @Test
    void forceRequeue_stoppedJob_transitionsToQueued() {
        long id = jobDao.queueJob("20260430", "/tmp", JOB_TYPE_MAIN);
        jobDao.forceStop(id);
        assertEquals(STATUS_STOPPED, jobDao.getStatus(id));

        jobDao.forceRequeue(id);

        assertEquals(STATUS_QUEUED, jobDao.getStatus(id));
    }

    @Test
    void forceRequeue_clearsFinishedAtAndErrorMessage() {
        long id = jobDao.queueJob("20260430", "/tmp", JOB_TYPE_MAIN);
        // Simulate a failed job
        jdbc.update("UPDATE lseg_jobs SET status='FAILED', finished_at=NOW(), error_message='boom' WHERE id=?", id);

        jobDao.forceRequeue(id);

        String finishedAt = jdbc.queryForObject(
                "SELECT finished_at FROM lseg_jobs WHERE id=?", String.class, id);
        String errorMsg = jdbc.queryForObject(
                "SELECT error_message FROM lseg_jobs WHERE id=?", String.class, id);
        assertNull(finishedAt);
        assertNull(errorMsg);
        assertEquals(STATUS_QUEUED, jobDao.getStatus(id));
    }

    @Test
    void updateStatus_doesNotOverwriteStopped() {
        long id = jobDao.queueJob("20260430", "/tmp", JOB_TYPE_MAIN);
        jobDao.forceStop(id);
        assertEquals(STATUS_STOPPED, jobDao.getStatus(id));

        // updateStatus must not change a STOPPED job
        jobDao.updateStatus(id, STATUS_QUEUED, null);

        assertEquals(STATUS_STOPPED, jobDao.getStatus(id));
    }

    @Test
    void queueJob_persistsJobType() {
        long mainId = jobDao.queueJob("20260430", "/tmp", JOB_TYPE_MAIN);
        long delistedId = jobDao.queueJob("20260430", "/tmp", JOB_TYPE_DELISTED);

        assertEquals(JOB_TYPE_MAIN, jobDao.getJobType(mainId));
        assertEquals(JOB_TYPE_DELISTED, jobDao.getJobType(delistedId));
    }

    @Test
    void forceRequeue_worksForCompletedJobToo() {
        long id = jobDao.queueJob("20260430", "/tmp", JOB_TYPE_MAIN);
        jdbc.update("UPDATE lseg_jobs SET status='COMPLETED', finished_at=NOW() WHERE id=?", id);

        jobDao.forceRequeue(id);

        assertEquals(STATUS_QUEUED, jobDao.getStatus(id));
    }
}
