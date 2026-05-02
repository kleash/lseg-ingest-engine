package com.lseg.ingest.audit;

import com.lseg.ingest.plan.IngestFile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// Audit status literals match Constants.java; hardcoded in SQL for readability.
@Component
public class FileAuditDao {

    private final JdbcTemplate jdbc;

    public FileAuditDao(DataSource ds) {
        this.jdbc = new JdbcTemplate(ds);
    }

    public Set<String> loadSuccessFileNames(int lookbackDays) {
        List<String> names = jdbc.queryForList(
                "SELECT file_name FROM lseg_file_audit WHERE status = 'SUCCESS' " +
                "AND business_date >= CURDATE() - INTERVAL ? DAY",
                String.class, lookbackDays);
        return new HashSet<>(names);
    }

    public void markStarted(IngestFile f, String businessDate, int declaredRows) {
        jdbc.update(
                "INSERT INTO lseg_file_audit (file_name, dataset, target_table, kind, seq, business_date, declared_rows, status, started_at) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, 'STARTED', ?) " +
                        "ON DUPLICATE KEY UPDATE dataset=VALUES(dataset), target_table=VALUES(target_table), kind=VALUES(kind), seq=VALUES(seq), " +
                        "business_date=VALUES(business_date), declared_rows=VALUES(declared_rows), status='STARTED', started_at=VALUES(started_at), " +
                        "finished_at=NULL, error_message=NULL, parsed_rows=NULL, inserted_rows=NULL, skipped_rows=NULL, " +
                        "ins_count=0, upd_count=0, del_count=0",
                f.fileName(),
                f.dataset(),
                f.target().name().toLowerCase(),
                f.kind().name(),
                f.seq(),
                Date.valueOf(parseBusinessDate(businessDate)),
                declaredRows,
                new Timestamp(System.currentTimeMillis()));
    }

    public void markFinished(IngestFile f, String status, int parsed, int inserted, int skipped,
                             int ins, int upd, int del, String errorMessage) {
        jdbc.update(
                "UPDATE lseg_file_audit SET status=?, parsed_rows=?, inserted_rows=?, skipped_rows=?, " +
                        "ins_count=?, upd_count=?, del_count=?, error_message=?, finished_at=? WHERE file_name=?",
                status, parsed, inserted, skipped, ins, upd, del, errorMessage, new Timestamp(System.currentTimeMillis()), f.fileName());
    }

    public static LocalDate parseBusinessDate(String yyyymmdd) {
        return LocalDate.parse(yyyymmdd, DateTimeFormatter.ofPattern("yyyyMMdd"));
    }

    public void markSkippedSanity(IngestFile f, String reason, String businessDate) {
        jdbc.update(
                "INSERT INTO lseg_file_audit (file_name, dataset, target_table, kind, seq, business_date, status, error_message, started_at, finished_at) " +
                        "VALUES (?, ?, ?, ?, ?, ?, 'SKIPPED_SANITY', ?, ?, ?) " +
                        "ON DUPLICATE KEY UPDATE status='SKIPPED_SANITY', error_message=VALUES(error_message), finished_at=VALUES(finished_at)",
                f.fileName(), f.dataset(), f.target().name().toLowerCase(), f.kind().name(), f.seq(),
                Date.valueOf(parseBusinessDate(businessDate)), reason,
                new Timestamp(System.currentTimeMillis()), new Timestamp(System.currentTimeMillis()));
    }

    public void markManualSkip(String fileName, String reason) {
        jdbc.update(
                "INSERT INTO lseg_file_audit (file_name, status, error_message, finished_at) " +
                        "VALUES (?, 'SKIPPED', ?, ?) " +
                        "ON DUPLICATE KEY UPDATE status='SKIPPED', error_message=VALUES(error_message), finished_at=VALUES(finished_at)",
                fileName, reason, new Timestamp(System.currentTimeMillis()));
    }
}
