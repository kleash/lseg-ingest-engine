package com.lseg.ingest.filter;

/** Drops INT-quote rows whose RIC contains '^'. */
public class RicCaretFilter {

    private final int ricColumnIndex;

    /** -1 if RIC column is not present in the file (filter then never matches). */
    public RicCaretFilter(int ricColumnIndex) {
        this.ricColumnIndex = ricColumnIndex;
    }

    public boolean shouldSkip(String[] row) {
        if (ricColumnIndex < 0 || ricColumnIndex >= row.length) return false;
        String ric = row[ricColumnIndex];
        return ric != null && ric.indexOf('^') >= 0;
    }
}
