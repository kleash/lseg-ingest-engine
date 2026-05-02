package com.lseg.ingest.orchestrator;

import com.lseg.ingest.audit.FileAuditDao;
import com.lseg.ingest.audit.JobDao;
import com.lseg.ingest.config.IngestProperties;
import com.lseg.ingest.load.FileIngestor;
import com.lseg.ingest.plan.FileScanner;
import com.lseg.ingest.sanity.FileSanityCheck;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;

import javax.sql.DataSource;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class IngestOrchestratorBusinessDateTest {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyyMMdd");

    private JobDao jobDao;
    private FileScanner scanner;
    private FileAuditDao audit;
    private IngestOrchestrator orchestrator;
    private IngestProperties props;
    private DataSource ds;

    @BeforeEach
    void setUp() {
        jobDao = mock(JobDao.class);
        scanner = mock(FileScanner.class);
        audit = mock(FileAuditDao.class);
        ds = mock(DataSource.class);

        props = new IngestProperties();
        props.setInputDir("/tmp/input");
        props.setMaxBusinessDateAgeDays(30);
        props.setAuditLookbackDays(60);

        orchestrator = new IngestOrchestrator(
                ds,
                scanner,
                mock(FileSanityCheck.class),
                audit,
                jobDao,
                mock(FileIngestor.class),
                props,
                new SimpleMeterRegistry(),
                mockClusterLock(),
                mock(ApplicationEventPublisher.class));
    }

    @Test
    void businessDateOlderThanMaxFails_beforeScannerIsCalled() throws Exception {
        String staleDate = LocalDate.now().minusDays(31).format(FMT);
        when(jobDao.getBusinessDate(1L)).thenReturn(staleDate);
        when(jobDao.getInputDir(1L)).thenReturn(null);

        Exception ex = assertThrows(Exception.class, () -> orchestrator.run(1L));

        assertTrue(ex.getMessage().contains("days old"), "Expected age-check error, got: " + ex.getMessage());
        verifyNoInteractions(scanner);
    }

    @Test
    void businessDateExactlyAtMaxPasses() throws Exception {
        String borderDate = LocalDate.now().minusDays(30).format(FMT);
        when(jobDao.getBusinessDate(1L)).thenReturn(borderDate);
        when(jobDao.getInputDir(1L)).thenReturn(null);
        when(scanner.scan(anyString())).thenReturn(java.util.List.of());
        when(audit.loadSuccessFileNames(anyInt())).thenReturn(java.util.Set.of());

        // Should not throw; will proceed to scanning with zero files
        assertDoesNotThrow(() -> orchestrator.run(1L));
    }

    @Test
    void recentBusinessDatePasses() throws Exception {
        String recentDate = LocalDate.now().minusDays(1).format(FMT);
        when(jobDao.getBusinessDate(1L)).thenReturn(recentDate);
        when(jobDao.getInputDir(1L)).thenReturn(null);
        when(scanner.scan(anyString())).thenReturn(java.util.List.of());
        when(audit.loadSuccessFileNames(anyInt())).thenReturn(java.util.Set.of());

        assertDoesNotThrow(() -> orchestrator.run(1L));
    }

    @Test
    void unparseableBusinessDateThrowsIllegalArgument() throws Exception {
        when(jobDao.getBusinessDate(1L)).thenReturn("not-a-date");
        when(jobDao.getInputDir(1L)).thenReturn(null);

        assertThrows(Exception.class, () -> orchestrator.run(1L));
        verifyNoInteractions(scanner);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private ClusterLock mockClusterLock() {
        ClusterLock lock = mock(ClusterLock.class);
        try {
            ClusterLock.Handle handle = mock(ClusterLock.Handle.class);
            when(handle.acquired()).thenReturn(true);
            when(lock.tryAcquire()).thenReturn(handle);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return lock;
    }
}
