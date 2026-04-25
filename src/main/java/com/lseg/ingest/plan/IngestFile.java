package com.lseg.ingest.plan;

import java.nio.file.Path;

public record IngestFile(Path path, String fileName, String dataset, Target target, Kind kind, int seq) {}
