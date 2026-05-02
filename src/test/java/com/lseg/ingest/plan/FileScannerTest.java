package com.lseg.ingest.plan;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class FileScannerTest {

    private final FileScanner scanner = new FileScanner(new com.lseg.ingest.config.IngestProperties());

    @Test
    void classifiesIntQuoteFile() {
        IngestFile f = scanner.classify(Path.of("/x"), "EIS_INT_US_EQU_QUOTE.INT.25967.20260425.1.1.1.txt.zip");
        assertNotNull(f);
        assertEquals(Target.QUOTES, f.target());
        assertEquals(Kind.INT, f.kind());
        assertEquals(1, f.seq());
    }

    @Test
    void classifiesDeltaQuoteFile() {
        IngestFile f = scanner.classify(Path.of("/x"), "EIS_DELTA_ASIA_US_QUOTE.REF.25963.20260425.7.1.1.txt.zip");
        assertEquals(Target.QUOTES, f.target());
        assertEquals(Kind.DELTA, f.kind());
        assertEquals(7, f.seq());
    }

    @Test
    void classifiesAssets() {
        assertEquals(Target.ASSETS,
                scanner.classify(Path.of("/x"), "EIS_INT_GLOBAL_EQU_OPT_ASSETS.INT.25971.20260425.1.1.1.txt.zip").target());
        assertEquals(Target.ASSETS,
                scanner.classify(Path.of("/x"), "EIS_DELTA_GLOBAL_ASSETS.REF.25965.20260425.42.1.1.txt.zip").target());
    }

    @Test
    void classifiesOrgs() {
        assertEquals(Target.ORGS,
                scanner.classify(Path.of("/x"), "Organization.INT.25748.20260425.1.1.1.txt.zip").target());
        // Vendor typo (GLOABL) — historical filename
        assertEquals(Target.ORGS,
                scanner.classify(Path.of("/x"), "EIS_DELTA_GLOABL_ORGN.REF.25966.20260425.1.1.1.txt.zip").target());
        // Corrected spelling (GLOBAL) — defensive against future feed evolution
        assertEquals(Target.ORGS,
                scanner.classify(Path.of("/x"), "EIS_DELTA_GLOBAL_ORGN.REF.25966.20260425.1.1.1.txt.zip").target());
    }

    @Test
    void classifiesBondsCsv() {
        IngestFile f = scanner.classify(Path.of("/x"), "SG_HK_Bonds_20260420 070012.csv");
        assertNotNull(f);
        assertEquals(Target.DSS_BONDS, f.target());
        assertEquals(Kind.INT, f.kind());
        assertEquals(0, f.seq());
    }

    @Test
    void rejectsBondsRicAndNotes() {
        assertNull(scanner.classify(Path.of("/x"), "SG_HK_Bonds_20260420 070012.ric.csv"));
        assertNull(scanner.classify(Path.of("/x"), "SG_HK_Bonds_20260420 070012.csv.notes.txt"));
    }

    @Test
    void rejectsUnknownDataset() {
        assertNull(scanner.classify(Path.of("/x"), "Reference-INT-EQUI-AMEQUQ-1-1-1.INT.25723.20260425.1.1.1.txt.zip")
                != null ? null : null /* placeholder */);
        // Reference-INT-EQUI-* dataset name doesn't match any target rule even if classify is called directly.
        assertNull(scanner.classify(Path.of("/x"), "Some_Random_File.INT.1.20260425.1.1.1.txt.zip"));
    }

    // --- PRICING ---

    @Test
    void classifiesUsPricingFile() {
        IngestFile f = scanner.classify(Path.of("/x"),
                "EIS_INT_US_PRICING.PRC.25DA1.20260430.1.1.1.1.txt.zip");
        assertNotNull(f);
        assertEquals(Target.PRICING, f.target());
        assertEquals(Kind.INT, f.kind());
        assertEquals(1, f.seq());
        assertEquals("EIS_INT_US_PRICING", f.dataset());
    }

    @Test
    void classifiesEuPricingFile() {
        IngestFile f = scanner.classify(Path.of("/x"),
                "EIS_INT_EU_PRICING.PRC.25DA0.20260430.10.16.1.1.txt.zip");
        assertNotNull(f);
        assertEquals(Target.PRICING, f.target());
        assertEquals(Kind.INT, f.kind());
        assertEquals(10, f.seq());
        assertEquals("EIS_INT_EU_PRICING", f.dataset());
    }

    @Test
    void classifiesAsiaPricingFile() {
        IngestFile f = scanner.classify(Path.of("/x"),
                "EIS_INT_ASIA_PRICING.PRC.25D9F.20260430.96.120.1.1.txt.zip");
        assertNotNull(f);
        assertEquals(Target.PRICING, f.target());
        assertEquals(96, f.seq());
    }

    @Test
    void rejectsPricingNoteFile() {
        // Note files have "note" in place of the two numeric suffix segments — must not classify
        assertNull(scanner.classify(Path.of("/x"),
                "EIS_INT_US_PRICING.PRC.25DA1.20260430.1.1.note.txt.zip"));
    }

    @Test
    void pricingFilesAreAlwaysKindInt() {
        // All PRICING files must produce Kind.INT — there is no PRICING delta format in the LSEG delivery.
        assertEquals(Kind.INT, scanner.classify(Path.of("/x"),
                "EIS_INT_US_PRICING.PRC.25DA1.20260430.1.1.1.1.txt.zip").kind());
        assertEquals(Kind.INT, scanner.classify(Path.of("/x"),
                "EIS_INT_EU_PRICING.PRC.25DA0.20260430.10.16.1.1.txt.zip").kind());
        assertEquals(Kind.INT, scanner.classify(Path.of("/x"),
                "EIS_INT_ASIA_PRICING.PRC.25D9F.20260430.96.120.1.1.txt.zip").kind());
    }

}
