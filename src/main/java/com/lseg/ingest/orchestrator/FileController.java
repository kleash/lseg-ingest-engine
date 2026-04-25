package com.lseg.ingest.orchestrator;

import com.lseg.ingest.audit.FileAuditDao;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/files")
public class FileController {

    private final FileAuditDao auditDao;

    public FileController(FileAuditDao auditDao) {
        this.auditDao = auditDao;
    }

    @PostMapping("/skip")
    public Map<String, String> skip(@RequestParam String fileName, @RequestParam String reason) {
        // We'll add a method to FileAuditDao to mark as skipped manually
        auditDao.markManualSkip(fileName, reason);
        return Map.of("fileName", fileName, "status", "SKIPPED", "message", "File marked as skipped");
    }
}
