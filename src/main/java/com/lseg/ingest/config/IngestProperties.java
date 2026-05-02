package com.lseg.ingest.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties("ingest")
public class IngestProperties {

    private String inputDir;
    private String archiveDir;
    private String charset = "UTF-8";
    private List<String> skipPatterns;
    private boolean ricCaretFilter = true;
    private int auditLookbackDays = 60;
    private int maxBusinessDateAgeDays = 30;
    private Threads threads = new Threads();
    private Batch batch = new Batch();
    private Cancel cancel = new Cancel();
    private Reaper reaper = new Reaper();
    private Cluster cluster = new Cluster();
    private Retry retry = new Retry();

    public String getInputDir() { return inputDir; }
    public void setInputDir(String v) { this.inputDir = v; }

    public String getArchiveDir() { return archiveDir; }
    public void setArchiveDir(String v) { this.archiveDir = v; }
    public String getCharset() { return charset; }
    public void setCharset(String v) { this.charset = v; }
    public List<String> getSkipPatterns() { return skipPatterns; }
    public void setSkipPatterns(List<String> v) { this.skipPatterns = v; }
    public boolean isRicCaretFilter() { return ricCaretFilter; }
    public void setRicCaretFilter(boolean v) { this.ricCaretFilter = v; }
    public int getAuditLookbackDays() { return auditLookbackDays; }
    public void setAuditLookbackDays(int v) { this.auditLookbackDays = v; }
    public int getMaxBusinessDateAgeDays() { return maxBusinessDateAgeDays; }
    public void setMaxBusinessDateAgeDays(int v) { this.maxBusinessDateAgeDays = v; }
    public Threads getThreads() { return threads; }
    public void setThreads(Threads v) { this.threads = v; }
    public Batch getBatch() { return batch; }
    public void setBatch(Batch v) { this.batch = v; }
    public Cancel getCancel() { return cancel; }
    public void setCancel(Cancel v) { this.cancel = v; }
    public Reaper getReaper() { return reaper; }
    public void setReaper(Reaper v) { this.reaper = v; }
    public Cluster getCluster() { return cluster; }
    public void setCluster(Cluster v) { this.cluster = v; }
    public Retry getRetry() { return retry; }
    public void setRetry(Retry v) { this.retry = v; }

    public static class Threads {
        private int intPerTable = 4;
        private int deltaTargetsParallel = 3;
        private int pricingThreads = 3;
        public int getIntPerTable() { return intPerTable; }
        public void setIntPerTable(int v) { this.intPerTable = v; }
        public int getDeltaTargetsParallel() { return deltaTargetsParallel; }
        public void setDeltaTargetsParallel(int v) { this.deltaTargetsParallel = v; }
        public int getPricingThreads() { return pricingThreads; }
        public void setPricingThreads(int v) { this.pricingThreads = v; }
    }

    public static class Batch {
        private int upsertSize = 5000;
        private int deleteSize = 5000;
        private int maxSkippedRowsPerFile = 1000;
        public int getUpsertSize() { return upsertSize; }
        public void setUpsertSize(int v) { this.upsertSize = v; }
        public int getDeleteSize() { return deleteSize; }
        public void setDeleteSize(int v) { this.deleteSize = v; }
        public int getMaxSkippedRowsPerFile() { return maxSkippedRowsPerFile; }
        public void setMaxSkippedRowsPerFile(int v) { this.maxSkippedRowsPerFile = v; }
    }

    /** How often (in rows) FileIngestor polls JobDao.isStopped(). */
    public static class Cancel {
        private int checkRows = 5000;
        public int getCheckRows() { return checkRows; }
        public void setCheckRows(int v) { this.checkRows = v; }
    }

    /** Stuck-RUNNING-job reaper. */
    public static class Reaper {
        private boolean enabled = true;
        private long staleJobTimeoutSeconds = 10800;   // 3 hours default
        private long pollIntervalSeconds = 60;
        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean v) { this.enabled = v; }
        public long getStaleJobTimeoutSeconds() { return staleJobTimeoutSeconds; }
        public void setStaleJobTimeoutSeconds(long v) { this.staleJobTimeoutSeconds = v; }
        public long getPollIntervalSeconds() { return pollIntervalSeconds; }
        public void setPollIntervalSeconds(long v) { this.pollIntervalSeconds = v; }
    }

    /** Cluster-wide singleton. Only one node may run at a time. */
    public static class Cluster {
        private String lockName = "lseg-ingest-cluster";
        private long heartbeatIntervalSeconds = 30;
        public String getLockName() { return lockName; }
        public void setLockName(String v) { this.lockName = v; }
        public long getHeartbeatIntervalSeconds() { return heartbeatIntervalSeconds; }
        public void setHeartbeatIntervalSeconds(long v) { this.heartbeatIntervalSeconds = v; }
    }

    /** Retry on transient DB errors. */
    public static class Retry {
        private int maxAttempts = 3;
        private long initialDelayMs = 250;
        private long maxDelayMs = 5000;
        public int getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(int v) { this.maxAttempts = v; }
        public long getInitialDelayMs() { return initialDelayMs; }
        public void setInitialDelayMs(long v) { this.initialDelayMs = v; }
        public long getMaxDelayMs() { return maxDelayMs; }
        public void setMaxDelayMs(long v) { this.maxDelayMs = v; }
    }
}
