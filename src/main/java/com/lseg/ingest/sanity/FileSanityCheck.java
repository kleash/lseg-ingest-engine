package com.lseg.ingest.sanity;

import com.lseg.ingest.io.PipeFileParser;
import com.lseg.ingest.io.ZipLineReader;
import com.lseg.ingest.plan.IngestFile;
import com.lseg.ingest.plan.Kind;
import org.springframework.stereotype.Component;

/**
 * Cheap pre-ingest header check: opens the zip, locates header, verifies
 *   - metadata kind matches filename kind
 *   - business date matches the configured date
 *   - the natural-key column for the target is present in the header.
 * Reads only the first ~50 lines.
 */
@Component
public class FileSanityCheck {

    public record Result(boolean ok, String reason, PipeFileParser.Metadata metadata, java.util.List<String> headerColumns) {}

    public Result check(IngestFile file, String expectedBusinessDate) {
        try (ZipLineReader z = new ZipLineReader(file.path())) {
            PipeFileParser p = new PipeFileParser(z.reader());
            p.initialize(50);
            PipeFileParser.Metadata md = p.metadata();
            if (md == null) return new Result(false, "metadata row missing", null, p.headerColumns());

            String expectedKind = (file.kind() == Kind.INT) ? "INT" : "REF";
            if (!expectedKind.equals(md.kind()))
                return new Result(false, "metadata kind=" + md.kind() + " expected=" + expectedKind, md, p.headerColumns());

            if (expectedBusinessDate != null && !expectedBusinessDate.equals(md.businessDate()))
                return new Result(false, "business date=" + md.businessDate() + " expected=" + expectedBusinessDate, md, p.headerColumns());

            if (!p.headerIndex().containsKey(file.target().permIdSourceHeader))
                return new Result(false, "missing key column " + file.target().permIdSourceHeader, md, p.headerColumns());

            return new Result(true, "ok", md, p.headerColumns());
        } catch (Exception e) {
            return new Result(false, "exception: " + e.getClass().getSimpleName() + ": " + e.getMessage(), null, null);
        }
    }
}
