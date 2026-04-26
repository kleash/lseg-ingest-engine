package com.lseg.ingest.plan;

import com.lseg.ingest.config.IngestProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.lseg.ingest.Constants.*;

/**
 * Scans the input directory and classifies each file as an {@link IngestFile}.
 * Applies skip patterns from config; the audit-based skip is applied by the orchestrator.
 */
@Component
public class FileScanner {

    private static final Logger log = LoggerFactory.getLogger(FileScanner.class);

    // Filename grammar (after stripping .txt.zip): <dataset>.<INT|REF>.<feedId>.<yyyymmdd>.<seq>.<a>.<b>
    private static final Pattern FILE_PATTERN = Pattern.compile(
            "^(?<dataset>[A-Za-z0-9_\\-]+)\\.(?<kind>INT|REF)\\.(?<feed>[A-Za-z0-9]+)\\.(?<date>\\d{8})\\.(?<seq>\\d+)\\.\\d+\\.\\d+\\.txt\\.zip$");

    private static final Pattern BONDS_CSV_PATTERN = Pattern.compile(
            "^SG_HK_Bonds_(?<date>\\d{8})[^.]*\\.csv$");

    private final IngestProperties props;

    public FileScanner(IngestProperties props) {
        this.props = props;
    }

    public List<IngestFile> scan() throws IOException {
        return scan(props.getInputDir());
    }

    public List<IngestFile> scan(String inputDir) throws IOException {
        Path dir = Paths.get(inputDir);
        if (!Files.isDirectory(dir)) {
            throw new IOException("Input directory not found: " + dir);
        }
        List<PathMatcher> skipMatchers = compileSkipMatchers();
        List<IngestFile> out = new ArrayList<>();
        try (Stream<Path> stream = Files.list(dir)) {
            stream.filter(Files::isRegularFile).forEach(p -> {
                String name = p.getFileName().toString();
                if (name.endsWith(".ric.csv") || name.endsWith("notes.txt")) return;
                if (matchesAny(name, skipMatchers)) return;
                IngestFile f = classify(p, name);
                if (f != null) out.add(f);
                else log.debug("Unclassified file ignored: {}", name);
            });
        }
        log.info("Scanned {} ingestible files in {}", out.size(), dir);
        return out;
    }

    private List<PathMatcher> compileSkipMatchers() {
        FileSystem fs = FileSystems.getDefault();
        List<PathMatcher> list = new ArrayList<>();
        if (props.getSkipPatterns() != null) {
            for (String glob : props.getSkipPatterns()) {
                list.add(fs.getPathMatcher("glob:" + glob));
            }
        }
        return list;
    }

    private boolean matchesAny(String name, List<PathMatcher> matchers) {
        Path filename = Paths.get(name);
        for (PathMatcher m : matchers) if (m.matches(filename)) return true;
        return false;
    }

    IngestFile classify(Path path, String name) {
        Matcher m = FILE_PATTERN.matcher(name);
        if (m.matches()) {
            String dataset = m.group("dataset");
            String kindToken = m.group("kind");
            int seq = Integer.parseInt(m.group("seq"));
            Target target = mapTarget(dataset);
            if (target == null) return null;
            Kind kind = KIND_INT.equals(kindToken) ? Kind.INT : Kind.DELTA;
            return new IngestFile(path, name, dataset, target, kind, seq);
        }

        Matcher mb = BONDS_CSV_PATTERN.matcher(name);
        if (mb.matches()) {
            String date = mb.group("date");
            // For CSV bonds, we synthesize a constant dataset and seq=0
            return new IngestFile(path, name, "SG_HK_Bonds", Target.DSS_BONDS, Kind.INT, 0);
        }

        return null;
    }

    static Target mapTarget(String dataset) {
        if (dataset.equals("SG_HK_Bonds")) return Target.DSS_BONDS;
        // ORGS
        if (dataset.equals("Organization")) return Target.ORGS;
        if (dataset.contains("GLOBAL_ORGN") || dataset.contains("GLOABL_ORGN")) return Target.ORGS;
        // ASSETS
        if (dataset.startsWith("EIS_INT_") && dataset.endsWith("_ASSETS")) return Target.ASSETS;
        if (dataset.equals("EIS_DELTA_GLOBAL_ASSETS")) return Target.ASSETS;
        // QUOTES
        if (dataset.startsWith("EIS_INT_") && (dataset.endsWith("_QUOTE") || dataset.endsWith("_QUOTES"))) return Target.QUOTES;
        if (dataset.startsWith("EIS_DELTA_") && dataset.endsWith("_QUOTE")) return Target.QUOTES;
        return null;
    }
}
