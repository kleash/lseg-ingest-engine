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
}
