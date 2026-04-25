package com.lseg.ingest.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties("ingest")
public class IngestProperties {

    private String inputDir;
    private String archiveDir;
    private String businessDate;
    private List<String> skipPatterns;
    private boolean ricCaretFilter = true;
    private Threads threads = new Threads();
    private Batch batch = new Batch();
    private Resilience resilience = new Resilience();

    public String getInputDir() { return inputDir; }
    public void setInputDir(String v) { this.inputDir = v; }
    public String getArchiveDir() { return archiveDir; }
    public void setArchiveDir(String v) { this.archiveDir = v; }
    public String getBusinessDate() { return businessDate; }
    public void setBusinessDate(String v) { this.businessDate = v; }
    public List<String> getSkipPatterns() { return skipPatterns; }
    public void setSkipPatterns(List<String> v) { this.skipPatterns = v; }
    public boolean isRicCaretFilter() { return ricCaretFilter; }
    public void setRicCaretFilter(boolean v) { this.ricCaretFilter = v; }
    public Threads getThreads() { return threads; }
    public void setThreads(Threads v) { this.threads = v; }
    public Batch getBatch() { return batch; }
    public void setBatch(Batch v) { this.batch = v; }
    public Resilience getResilience() { return resilience; }
    public void setResilience(Resilience v) { this.resilience = v; }

    public static class Threads {
        private int intPerTable = 4;
        private int deltaTargetsParallel = 3;
        public int getIntPerTable() { return intPerTable; }
        public void setIntPerTable(int v) { this.intPerTable = v; }
        public int getDeltaTargetsParallel() { return deltaTargetsParallel; }
        public void setDeltaTargetsParallel(int v) { this.deltaTargetsParallel = v; }
    }

    public static class Batch {
        private int upsertSize = 5000;
        private int deleteSize = 5000;
        public int getUpsertSize() { return upsertSize; }
        public void setUpsertSize(int v) { this.upsertSize = v; }
        public int getDeleteSize() { return deleteSize; }
        public void setDeleteSize(int v) { this.deleteSize = v; }
    }

    public static class Resilience {
        private boolean fallbackOnBatchFail = true;
        private int maxSkippedRowsPerFile = 1000;
        public boolean isFallbackOnBatchFail() { return fallbackOnBatchFail; }
        public void setFallbackOnBatchFail(boolean v) { this.fallbackOnBatchFail = v; }
        public int getMaxSkippedRowsPerFile() { return maxSkippedRowsPerFile; }
        public void setMaxSkippedRowsPerFile(int v) { this.maxSkippedRowsPerFile = v; }
    }
}
