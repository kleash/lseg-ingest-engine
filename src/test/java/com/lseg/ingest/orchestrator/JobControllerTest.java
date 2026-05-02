package com.lseg.ingest.orchestrator;

import com.lseg.ingest.audit.JobDao;
import com.lseg.ingest.config.IngestProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.lseg.ingest.Constants.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class JobControllerTest {

    private JobDao jobDao;
    private JobController controller;

    @BeforeEach
    void setUp() {
        jobDao = mock(JobDao.class);
        controller = new JobController(jobDao, new IngestProperties());
    }

    @Test
    void restart_stoppedJob_callsForceRequeue() {
        when(jobDao.getStatus(42L)).thenReturn(STATUS_STOPPED, STATUS_QUEUED);

        Map<String, Object> result = controller.restart(42L);

        verify(jobDao).forceRequeue(42L);
        verify(jobDao, never()).updateStatus(anyLong(), anyString(), any());
        assertEquals(STATUS_QUEUED, result.get("status"));
    }

    @Test
    void restart_failedJob_callsUpdateStatus() {
        when(jobDao.getStatus(7L)).thenReturn(STATUS_FAILED, STATUS_QUEUED);

        Map<String, Object> result = controller.restart(7L);

        verify(jobDao).updateStatus(7L, STATUS_QUEUED, null);
        verify(jobDao, never()).forceRequeue(anyLong());
        assertEquals(STATUS_QUEUED, result.get("status"));
    }

    @Test
    void restart_completedJob_callsUpdateStatus() {
        when(jobDao.getStatus(3L)).thenReturn(STATUS_COMPLETED, STATUS_QUEUED);

        controller.restart(3L);

        verify(jobDao).updateStatus(3L, STATUS_QUEUED, null);
    }

    @Test
    void restart_returnsActualDbStatus_notHardcodedValue() {
        // If updateStatus is a no-op (e.g., job is already QUEUED), return the real status.
        when(jobDao.getStatus(5L)).thenReturn(STATUS_QUEUED, STATUS_QUEUED);

        Map<String, Object> result = controller.restart(5L);

        assertEquals(5L, result.get("jobId"));
        assertEquals(STATUS_QUEUED, result.get("status"));
    }
}
