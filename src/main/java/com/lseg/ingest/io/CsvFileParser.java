package com.lseg.ingest.io;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.lseg.ingest.Constants.*;

/**
 * Parses DSS-style CSV files using OpenCSV.
 * Synthesizes an "Action" column at the beginning of each row (value='I').
 */
public class CsvFileParser implements FileParser {

    private static final Pattern BONDS_CSV_DATE_PATTERN = Pattern.compile("SG_HK_Bonds_(\\d{8})");

    private final BufferedReader reader;
    private final String fileName;
    private CSVReader csvReader;
    private Metadata metadata;
    private List<String> headerColumns;
    private Map<String, Integer> headerIndex;
    private long lineNumber;
    private boolean ready;

    public CsvFileParser(BufferedReader reader, String fileName) {
        this.reader = reader;
        this.fileName = fileName;
    }

    @Override
    public void initialize(int maxLookahead) throws IOException {
        // Filename: SG_HK_Bonds_20260420 070012.csv
        Matcher m = BONDS_CSV_DATE_PATTERN.matcher(fileName);
        String date = m.find() ? m.group(1) : "99991231";
        
        this.metadata = new Metadata("SG_HK_Bonds", KIND_INT, "DSS", date, 0, -1);
        
        this.csvReader = new CSVReader(reader);
        try {
            String[] headers = csvReader.readNext();
            if (headers == null) throw new IOException("CSV file is empty: " + fileName);
            lineNumber++;
            
            // Synthesize "Action" column
            List<String> cols = new ArrayList<>();
            cols.add(COL_ACTION);
            cols.addAll(List.of(headers));
            
            this.headerColumns = Collections.unmodifiableList(cols);
            this.headerIndex = new HashMap<>(headerColumns.size() * 2);
            for (int i = 0; i < headerColumns.size(); i++) {
                headerIndex.put(headerColumns.get(i), i);
            }
            this.ready = true;
        } catch (CsvValidationException e) {
            throw new IOException("Failed to parse CSV header: " + fileName, e);
        }
    }

    @Override
    public Metadata metadata() { return metadata; }

    @Override
    public List<String> headerColumns() { return headerColumns; }

    @Override
    public Map<String, Integer> headerIndex() { return headerIndex; }

    @Override
    public String[] nextRow() throws IOException {
        if (!ready) throw new IllegalStateException("initialize() not called");
        try {
            String[] row = csvReader.readNext();
            if (row == null) return null;
            lineNumber++;
            
            // Prepend synthesized "Action" value
            String[] out = new String[headerColumns.size()];
            out[0] = ACTION_INSERT;
            int toCopy = Math.min(row.length, out.length - 1);
            System.arraycopy(row, 0, out, 1, toCopy);
            // Pad remaining if row was shorter than header
            for (int i = toCopy + 1; i < out.length; i++) out[i] = null;
            return out;
        } catch (CsvValidationException e) {
            throw new IOException("Failed to parse CSV row at line " + lineNumber, e);
        }
    }

    @Override
    public long currentLine() { return lineNumber; }
}
