package com.lseg.ingest.load;

/** A bound row queued for batch execution. Holds raw String values aligned with the column list. */
public record PendingRow(String[] values, long lineNumber, String keyValue) {}
