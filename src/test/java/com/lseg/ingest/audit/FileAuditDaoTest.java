package com.lseg.ingest.audit;

import com.lseg.ingest.plan.IngestFile;
import com.lseg.ingest.plan.Kind;
import com.lseg.ingest.plan.Target;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class FileAuditDaoTest {

    @Container
    static MariaDBContainer<?> mariadb = new MariaDBContainer<>("mariadb:10.11")
            .withDatabaseName("lseg_test")
            .withUsername("test")
            .withPassword("test");

    private FileAuditDao dao;
    private JdbcTemplate jdbc;

    @BeforeEach
    void setUp() {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(mariadb.getJdbcUrl());
        cfg.setUsername(mariadb.getUsername());
        cfg.setPassword(mariadb.getPassword());
        DataSource ds = new HikariDataSource(cfg);
        jdbc = new JdbcTemplate(ds);
        dao = new FileAuditDao(ds);
        createSchema();
    }

    private void createSchema() {
        jdbc.execute("DROP TABLE IF EXISTS lseg_file_audit");
        jdbc.execute("""
            CREATE TABLE lseg_file_audit (
                file_name VARCHAR(500) PRIMARY KEY,
                dataset VARCHAR(100),
                target_table VARCHAR(50),
                kind VARCHAR(10),
                seq INT,
                business_date DATE,
                declared_rows INT,
                status VARCHAR(30),
                started_at TIMESTAMP NULL,
                finished_at TIMESTAMP NULL,
                error_message TEXT,
                parsed_rows INT,
                inserted_rows INT,
                updated_rows INT DEFAULT 0,
                unchanged_rows INT DEFAULT 0,
                skipped_rows INT,
                ins_count INT DEFAULT 0,
                upd_count INT DEFAULT 0,
                del_count INT DEFAULT 0
            )""");
    }

    @Test
    void loadSuccessFileNames_returnsOnlyRecentRecords() {
        // Recent SUCCESS — within lookback window
        jdbc.update("INSERT INTO lseg_file_audit (file_name, status, business_date) VALUES ('recent.zip', 'SUCCESS', CURDATE())");
        // Old SUCCESS — outside 60-day window
        jdbc.update("INSERT INTO lseg_file_audit (file_name, status, business_date) VALUES ('old.zip', 'SUCCESS', DATE_SUB(CURDATE(), INTERVAL 61 DAY))");
        // FAILED — should never be returned
        jdbc.update("INSERT INTO lseg_file_audit (file_name, status, business_date) VALUES ('failed.zip', 'FAILED', CURDATE())");

        Set<String> result = dao.loadSuccessFileNames(60);

        assertTrue(result.contains("recent.zip"));
        assertFalse(result.contains("old.zip"));
        assertFalse(result.contains("failed.zip"));
    }

    @Test
    void loadSuccessFileNames_zeroLookback_returnsEmpty() {
        jdbc.update("INSERT INTO lseg_file_audit (file_name, status, business_date) VALUES ('any.zip', 'SUCCESS', CURDATE())");

        Set<String> result = dao.loadSuccessFileNames(0);

        assertTrue(result.isEmpty());
    }

    @Test
    void loadSuccessFileNames_exactBoundary_isInclusive() {
        // Record exactly at the lookback boundary should be included
        jdbc.update("INSERT INTO lseg_file_audit (file_name, status, business_date) VALUES ('boundary.zip', 'SUCCESS', DATE_SUB(CURDATE(), INTERVAL 30 DAY))");

        Set<String> result = dao.loadSuccessFileNames(30);

        assertTrue(result.contains("boundary.zip"));
    }

    @Test
    void markStarted_thenMarkFinished_updatesRecord() {
        IngestFile f = new IngestFile(Path.of("/x/test.zip"), "test.zip", "ORGS_DS", Target.ORGS, Kind.INT, 1);
        dao.markStarted(f, "20260430", 1000);

        String status = jdbc.queryForObject("SELECT status FROM lseg_file_audit WHERE file_name=?", String.class, "test.zip");
        assertEquals("STARTED", status);

        dao.markFinished(f, "SUCCESS", 1000, 900, 99, 0, 1, 500, 499, 0, null);
        status = jdbc.queryForObject("SELECT status FROM lseg_file_audit WHERE file_name=?", String.class, "test.zip");
        assertEquals("SUCCESS", status);
    }
}
