package com.lseg.ingest.io;

import org.junit.jupiter.api.Test;
import java.io.BufferedReader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CsvFileParserTest {

    @Test
    void testCsvParsingWithQuotesAndCommas() throws Exception {
        String content = "ISIN,Instrument ID,Issuer Name\n" +
                "SG123,ID001,\"SINGAPORE, REPUBLIC OF\"\n" +
                "SG456,ID002,Simple Name\n";
        
        CsvFileParser parser = new CsvFileParser(new BufferedReader(new StringReader(content)), "SG_HK_Bonds_20260420.csv");
        parser.initialize(50);
        
        assertEquals("SG_HK_Bonds", parser.metadata().dataset());
        assertEquals("20260420", parser.metadata().businessDate());
        
        List<String> headers = parser.headerColumns();
        assertEquals(4, headers.size());
        assertEquals("Action", headers.get(0));
        assertEquals("ISIN", headers.get(1));
        assertEquals("Instrument ID", headers.get(2));
        assertEquals("Issuer Name", headers.get(3));
        
        Map<String, Integer> idx = parser.headerIndex();
        assertEquals(0, idx.get("Action"));
        assertEquals(3, idx.get("Issuer Name"));
        
        String[] row1 = parser.nextRow();
        assertNotNull(row1);
        assertEquals("I", row1[0]);
        assertEquals("SG123", row1[1]);
        assertEquals("ID001", row1[2]);
        assertEquals("SINGAPORE, REPUBLIC OF", row1[3]);
        
        String[] row2 = parser.nextRow();
        assertNotNull(row2);
        assertEquals("I", row2[0]);
        assertEquals("Simple Name", row2[3]);
        
        assertNull(parser.nextRow());
    }

    @Test
    void testFilenameParsing() throws Exception {
        String content = "ISIN\nSG123\n";
        CsvFileParser parser = new CsvFileParser(new BufferedReader(new StringReader(content)), "SG_HK_Bonds_20260501 123456.csv");
        parser.initialize(1);
        assertEquals("20260501", parser.metadata().businessDate());
    }

    @Test
    void testShortFilename() throws Exception {
        String content = "ISIN\nSG123\n";
        // Filename too short to contain date pattern or reach old offset
        CsvFileParser parser = new CsvFileParser(new BufferedReader(new StringReader(content)), "Bonds.csv");
        parser.initialize(1);
        assertEquals("99991231", parser.metadata().businessDate());
    }
}
