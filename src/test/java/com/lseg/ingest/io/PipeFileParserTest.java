package com.lseg.ingest.io;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.*;

class PipeFileParserTest {

    @Test
    void parsesMetadataAndHeaderAndRows() throws Exception {
        String content = """
                EIS_INT_US_EQU_QUOTE|INT|25967|20260425|1|2|
                Action|Asset_ID|Quote_Perm_ID|RIC|
                I|0xAA|123|FOO.N|
                I|0xBB|456|BAR.O|
                """;
        PipeFileParser p = new PipeFileParser(new BufferedReader(new StringReader(content)));
        p.initialize(50);
        assertEquals("INT", p.metadata().kind());
        assertEquals("20260425", p.metadata().businessDate());
        assertEquals(2, p.metadata().declaredRows());
        assertEquals(0, p.headerIndex().get("Action"));
        assertEquals(2, p.headerIndex().get("Quote_Perm_ID"));

        String[] r1 = p.nextRow();
        assertEquals("I", r1[0]);
        assertEquals("123", r1[2]);
        assertEquals("FOO.N", r1[3]);

        String[] r2 = p.nextRow();
        assertEquals("BAR.O", r2[3]);
        assertNull(p.nextRow());
    }

    @Test
    void toleratesExtraLeadingLines() throws Exception {
        String content = """
                # some commentary line

                Garbage
                EIS_DELTA_EU_QUOTE|REF|25964|20260425|3|1|
                some other junk
                Action|Quote_Perm_ID|RIC|
                U|999|XYZ.PA|
                """;
        PipeFileParser p = new PipeFileParser(new BufferedReader(new StringReader(content)));
        p.initialize(50);
        assertEquals("REF", p.metadata().kind());
        String[] r = p.nextRow();
        assertEquals("U", r[0]);
        assertEquals("999", r[1]);
    }

    @Test
    void blankFieldsBecomeNull() throws Exception {
        String content = """
                X|INT|1|20260425|1|1|
                Action|A|B|C|
                I||x||
                """;
        PipeFileParser p = new PipeFileParser(new BufferedReader(new StringReader(content)));
        p.initialize(10);
        String[] r = p.nextRow();
        assertEquals("I", r[0]);
        assertNull(r[1]);
        assertEquals("x", r[2]);
        assertNull(r[3]);
    }

    // --- PRC (pricing) format ---

    @Test
    void parsesPrcMetadataAndHeaderWithNoActionColumn() throws Exception {
        // Pricing files: PRC kind, extra chunk field in metadata, no Action column in headers
        String content = """
                EIS_INT_US_PRICING|PRC|25DA1|20260430|1|1|82010|
                Quote_ID|Quote_Perm_ID|Trade_Date|Close_Price|
                0x001|22166|20260429|0.19|
                0x002|22167|20260429|1.05|
                """;
        PipeFileParser p = new PipeFileParser(new BufferedReader(new StringReader(content)));
        p.initialize(50);

        assertEquals("PRC", p.metadata().kind());
        assertEquals("EIS_INT_US_PRICING", p.metadata().dataset());
        assertEquals("20260430", p.metadata().businessDate());
        assertEquals(82010, p.metadata().declaredRows());
        assertEquals(1, p.metadata().seq());

        // No Action column
        assertNull(p.headerIndex().get("Action"));
        assertEquals(0, p.headerIndex().get("Quote_ID"));
        assertEquals(3, p.headerIndex().get("Close_Price"));

        String[] r1 = p.nextRow();
        assertEquals("0x001", r1[0]);
        assertEquals("22166", r1[1]);
        assertEquals("20260429", r1[2]);
        assertEquals("0.19", r1[3]);

        String[] r2 = p.nextRow();
        assertEquals("0x002", r2[0]);
        assertEquals("1.05", r2[3]);
        assertNull(p.nextRow());
    }

    @Test
    void prcMetadataReadsRowCountFromIndex6() throws Exception {
        // Verifies row count is read from t[6] (not t[5]) for PRC format.
        // t[5]=chunk=99, t[6]=rows=12345
        String content = """
                EIS_INT_EU_PRICING|PRC|25DA0|20260430|5|99|12345|
                Quote_ID|Trade_Date|
                0xAAA|20260429|
                """;
        PipeFileParser p = new PipeFileParser(new BufferedReader(new StringReader(content)));
        p.initialize(10);
        assertEquals(12345, p.metadata().declaredRows());
        assertEquals(5, p.metadata().seq());
    }

    @Test
    void prcJunkBetweenMetadataAndHeaderIsSkipped() throws Exception {
        // A junk line (no pipes) between the metadata and header must be skipped
        String content = """
                EIS_INT_ASIA_PRICING|PRC|25D9F|20260430|3|7|500|
                this line has no pipes
                Quote_ID|Settlement_Price|
                0xBBB|1.23|
                """;
        PipeFileParser p = new PipeFileParser(new BufferedReader(new StringReader(content)));
        p.initialize(50);
        assertEquals("PRC", p.metadata().kind());
        assertEquals(0, p.headerIndex().get("Quote_ID"));
        String[] r = p.nextRow();
        assertEquals("0xBBB", r[0]);
        assertEquals("1.23", r[1]);
    }

    @Test
    void malformedPrcMetadataMissingRowCountField() throws Exception {
        // PRC metadata with only 6 fields (missing row count) — looksLikeMetadata must reject it
        // so initialize() falls through to looksLikeHeader and still finds the header
        String content = """
                EIS_INT_US_PRICING|PRC|25DA1|20260430|1|
                Action|Quote_ID|
                I|0xCCC|
                """;
        PipeFileParser p = new PipeFileParser(new BufferedReader(new StringReader(content)));
        p.initialize(50);
        // metadata not parsed (malformed) but header found via Action| fallback
        assertNull(p.metadata());
        assertEquals(0, p.headerIndex().get("Action"));
    }
}
