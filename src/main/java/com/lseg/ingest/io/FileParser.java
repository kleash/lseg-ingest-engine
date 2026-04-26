package com.lseg.ingest.io;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Common interface for file parsers (Pipe vs CSV).
 */
public interface FileParser {

    record Metadata(String dataset, String kind, String feed, String businessDate, int seq, int declaredRows) {}

    void initialize(int maxLookahead) throws IOException;

    Metadata metadata();

    List<String> headerColumns();

    Map<String, Integer> headerIndex();

    /** Reads the next data row, returns null at EOF. */
    String[] nextRow() throws IOException;

    long currentLine();
}
