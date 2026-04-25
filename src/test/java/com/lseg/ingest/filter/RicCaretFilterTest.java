package com.lseg.ingest.filter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RicCaretFilterTest {

    @Test
    void dropsRowsWithCaretInRic() {
        RicCaretFilter f = new RicCaretFilter(2);
        assertTrue(f.shouldSkip(new String[]{"I", "x", "ABC^.N"}));
        assertFalse(f.shouldSkip(new String[]{"I", "x", "ABC.N"}));
        assertFalse(f.shouldSkip(new String[]{"I", "x", null}));
    }

    @Test
    void noopWhenIndexUnset() {
        RicCaretFilter f = new RicCaretFilter(-1);
        assertFalse(f.shouldSkip(new String[]{"I", "x", "ABC^.N"}));
    }
}
