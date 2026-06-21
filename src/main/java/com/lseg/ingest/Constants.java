package com.lseg.ingest;

/**
 * Centralized constants for the ingestion engine.
 */
public final class Constants {

    private Constants() {}

    // Job Statuses
    public static final String STATUS_QUEUED = "QUEUED";
    public static final String STATUS_RUNNING = "RUNNING";
    public static final String STATUS_COMPLETED = "COMPLETED";
    public static final String STATUS_FAILED = "FAILED";
    public static final String STATUS_STOPPED = "STOPPED";

    // Job Types — determines which targets the orchestrator runs for a job.
    // MAIN runs every target except QUOTES_DELISTED; DELISTED runs only QUOTES_DELISTED.
    public static final String JOB_TYPE_MAIN = "MAIN";
    public static final String JOB_TYPE_DELISTED = "DELISTED";

    // File Audit Statuses
    public static final String AUDIT_STARTED = "STARTED";
    public static final String AUDIT_SUCCESS = "SUCCESS";
    public static final String AUDIT_FAILED = "FAILED";
    public static final String AUDIT_SKIPPED = "SKIPPED";
    public static final String AUDIT_SKIPPED_SANITY = "SKIPPED_SANITY";

    // File Kinds
    public static final String KIND_INT = "INT";
    public static final String KIND_DELTA = "DELTA";
    public static final String KIND_REF = "REF";
    public static final String KIND_PRC = "PRC";

    // Action Types
    public static final String ACTION_INSERT = "I";
    public static final String ACTION_UPDATE = "U";
    public static final String ACTION_DELETE = "D";

    // Column Headers
    public static final String COL_ACTION = "Action";
    public static final String COL_RIC = "RIC";

    // MDC Keys
    public static final String MDC_JOB_ID = "jobId";
    public static final String MDC_FILE = "file";

    // Metrics Keys
    public static final String METRIC_SANITY_FAILURES = "ingest.sanity.failures";
    public static final String METRIC_TARGET_ERRORS = "ingest.target.errors";
    public static final String METRIC_ORCHESTRATOR_ERRORS = "ingest.orchestrator.errors";
    public static final String METRIC_FILES_TOTAL = "ingest.files.total";
    public static final String METRIC_ARCHIVE_ERRORS = "ingest.archive.errors";
    public static final String METRIC_ROWS_PARSED = "ingest.rows.parsed";
    public static final String METRIC_ROWS_INSERTED = "ingest.rows.inserted";
    public static final String METRIC_ROWS_UPDATED = "ingest.rows.updated";
    public static final String METRIC_ROWS_SKIPPED_ERROR = "ingest.rows.skipped.error";
    public static final String METRIC_ROWS_SKIPPED_FILTER = "ingest.rows.skipped.filter";
    public static final String METRIC_ROWS_OPS = "ingest.rows.ops";
    public static final String METRIC_OVERALL_DURATION = "ingest.overall.duration";

    // Metric Tags
    public static final String TAG_TARGET = "target";
    public static final String TAG_KIND = "kind";
    public static final String TAG_STATUS = "status";
    public static final String TAG_OP = "op";
}
