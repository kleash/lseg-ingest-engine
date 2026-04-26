package com.lseg.ingest.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;

import static com.lseg.ingest.Constants.*;

/**
 * Parses an LSEG-style pipe-delimited stream:
 *   - Looks for a metadata row of the form  <name>|<INT|REF>|<feed>|<date>|<seq>|<rowCount>|
 *   - Then locates the header row (the first row containing the "Action" token).
 *   - Tolerates extra leading lines, comments, etc.
 *
 * Header parsing is name-driven: data rows are returned as a String[] where index i corresponds to
 * the column name at headerColumns[i]. Trailing empty token from the trailing '|' is dropped.
 */
public class PipeFileParser {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(PipeFileParser.class);

    public record Metadata(String dataset, String kind, String feed, String businessDate, int seq, int declaredRows) {}

    private final BufferedReader reader;
    private Metadata metadata;
    private List<String> headerColumns;
    private Map<String, Integer> headerIndex;
    private long lineNumber;
    private boolean ready;

    public PipeFileParser(BufferedReader reader) {
        this.reader = reader;
    }

    /** Reads up to maxLookahead lines, locates metadata + header rows. */
    public void initialize(int maxLookahead) throws IOException {
        for (int i = 0; i < maxLookahead; i++) {
            String line = reader.readLine();
            if (line == null) break;
            lineNumber++;
            String trimmed = line.trim();
            if (trimmed.isEmpty()) continue;

            if (metadata == null && looksLikeMetadata(trimmed)) {
                metadata = parseMetadata(trimmed);
                continue;
            }
            if (looksLikeHeader(trimmed)) {
                headerColumns = splitTokens(trimmed);
                headerIndex = new HashMap<>(headerColumns.size() * 2);
                java.util.List<String> dups = new java.util.ArrayList<>();
                for (int j = 0; j < headerColumns.size(); j++) {
                    String h = headerColumns.get(j);
                    if (h == null) continue;
                    if (headerIndex.putIfAbsent(h, j) != null) dups.add(h);
                }
                if (!dups.isEmpty()) {
                    log.warn("Duplicate header column(s) ignored (kept first occurrence): {}", dups);
                }
                ready = true;
                return;
            }
        }
        throw new IOException("Header row not found within first " + maxLookahead + " lines");
    }

    public Metadata metadata() { return metadata; }
    public List<String> headerColumns() { return headerColumns; }
    public Map<String, Integer> headerIndex() { return headerIndex; }
    public long currentLine() { return lineNumber; }

    /** Reads the next data row, returns null at EOF. The Action column is included as headerIndex.get("Action"). */
    public String[] nextRow() throws IOException {
        if (!ready) throw new IllegalStateException("initialize() not called");
        String line;
        while ((line = reader.readLine()) != null) {
            lineNumber++;
            if (line.isEmpty()) continue;
            List<String> tokens = splitTokens(line);
            // Pad or truncate to header length so callers can address by header index safely.
            // An over-long row indicates an embedded '|' in a free-text field; we truncate
            // (alignment past the over-run is irrecoverable from a name-driven parser perspective)
            // and rely on the row-by-row fallback to flag rows that fail SQL binding.
            if (tokens.size() < headerColumns.size()) {
                while (tokens.size() < headerColumns.size()) tokens.add(null);
            } else if (tokens.size() > headerColumns.size()) {
                log.warn("Row at line {} has {} tokens > {} header columns; truncating",
                        lineNumber, tokens.size(), headerColumns.size());
                tokens = tokens.subList(0, headerColumns.size());
            }
            return tokens.toArray(new String[0]);
        }
        return null;
    }

    private static boolean looksLikeMetadata(String line) {
        // Rough check: 6 pipe-separated fields, second is INT or REF, fourth looks like date.
        String[] toks = line.split("\\|", -1);
        if (toks.length < 6) return false;
        if (!toks[1].equals(KIND_INT) && !toks[1].equals(KIND_REF)) return false;
        return toks[3].matches("\\d{8}");
    }

    private static Metadata parseMetadata(String line) {
        String[] t = line.split("\\|", -1);
        int seq = safeInt(t[4]);
        int rows = safeInt(t[5]);
        return new Metadata(t[0], t[1], t[2], t[3], seq, rows);
    }

    private static int safeInt(String s) {
        try { return Integer.parseInt(s.trim()); } catch (Exception e) { return -1; }
    }

    private static boolean looksLikeHeader(String line) {
        // Header rows in LSEG always include "Action" as the first column.
        if (!line.startsWith(COL_ACTION + "|")) return false;
        // Non-data: header tokens never repeat and are typically alphabetic; data rows after the header begin with single-letter tokens.
        return true;
    }

    /** Splits on '|', drops a single trailing empty token introduced by the trailing pipe. */
    static List<String> splitTokens(String line) {
        String[] arr = line.split("\\|", -1);
        // LSEG files end every row with '|', creating one phantom trailing empty token.
        int len = arr.length;
        if (len > 0 && arr[len - 1].isEmpty()) len--;
        List<String> out = new ArrayList<>(len);
        for (int i = 0; i < len; i++) {
            String v = arr[i];
            out.add(v.isEmpty() ? null : v);
        }
        return out;
    }
}
