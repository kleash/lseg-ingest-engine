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

    private final IngestProperties props;

    public FileScanner(IngestProperties props) {
        this.props = props;
    }

    public List<IngestFile> scan() throws IOException {
        Path dir = Paths.get(props.getInputDir());
        if (!Files.isDirectory(dir)) {
            throw new IOException("Input directory not found: " + dir);
        }
        List<PathMatcher> skipMatchers = compileSkipMatchers();
        List<IngestFile> out = new ArrayList<>();
        try (Stream<Path> stream = Files.list(dir)) {
            stream.filter(Files::isRegularFile).forEach(p -> {
                String name = p.getFileName().toString();
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
        if (!m.matches()) return null;
        String dataset = m.group("dataset");
        String kindToken = m.group("kind");
        int seq = Integer.parseInt(m.group("seq"));
        Target target = mapTarget(dataset);
        if (target == null) return null;
        Kind kind = "INT".equals(kindToken) ? Kind.INT : Kind.DELTA;
        return new IngestFile(path, name, dataset, target, kind, seq);
    }

    static Target mapTarget(String dataset) {
        // ORGS
        if (dataset.equals("Organization")) return Target.ORGS;
        if (dataset.equals("EIS_DELTA_GLOABL_ORGN")) return Target.ORGS;
        // ASSETS
        if (dataset.startsWith("EIS_INT_") && dataset.endsWith("_ASSETS")) return Target.ASSETS;
        if (dataset.equals("EIS_DELTA_GLOBAL_ASSETS")) return Target.ASSETS;
        // QUOTES
        if (dataset.startsWith("EIS_INT_") && (dataset.endsWith("_QUOTE") || dataset.endsWith("_QUOTES"))) return Target.QUOTES;
        if (dataset.startsWith("EIS_DELTA_") && dataset.endsWith("_QUOTE")) return Target.QUOTES;
        return null;
    }
}
