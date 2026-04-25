package com.lseg.ingest.plan;

import java.util.*;
import java.util.stream.Collectors;

public class IngestPlan {

    private final Map<Target, List<IngestFile>> intByTarget = new EnumMap<>(Target.class);
    private final Map<Target, List<IngestFile>> deltaByTarget = new EnumMap<>(Target.class);

    public IngestPlan(List<IngestFile> files) {
        for (Target t : Target.values()) {
            intByTarget.put(t, new ArrayList<>());
            deltaByTarget.put(t, new ArrayList<>());
        }
        for (IngestFile f : files) {
            (f.kind() == Kind.INT ? intByTarget : deltaByTarget).get(f.target()).add(f);
        }
        // DELTA must apply in seq order; INT order doesn't matter.
        for (Target t : Target.values()) {
            deltaByTarget.get(t).sort(Comparator.comparingInt(IngestFile::seq));
        }
    }

    public List<IngestFile> intFor(Target t) { return intByTarget.get(t); }
    public List<IngestFile> deltaFor(Target t) { return deltaByTarget.get(t); }

    public List<IngestFile> all() {
        List<IngestFile> all = new ArrayList<>();
        intByTarget.values().forEach(all::addAll);
        deltaByTarget.values().forEach(all::addAll);
        return all;
    }

    public String summary() {
        return Arrays.stream(Target.values())
                .map(t -> t + ": INT=" + intByTarget.get(t).size() + " DELTA=" + deltaByTarget.get(t).size())
                .collect(Collectors.joining(", "));
    }
}
