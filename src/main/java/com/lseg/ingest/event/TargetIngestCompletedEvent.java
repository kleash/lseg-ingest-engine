package com.lseg.ingest.event;

import com.lseg.ingest.plan.Target;

/**
 * Published by IngestOrchestrator after all files for a Target finish processing
 * (INT phase + DELTA phase). Fired in the finally block so it always fires,
 * even on failure. Listeners use {@code @EventListener}.
 *
 * <pre>
 * {@code
 * @EventListener
 * public void onTargetComplete(TargetIngestCompletedEvent e) {
 *     if (e.target() == Target.PRICING && e.success()) { ... }
 * }
 * }
 * </pre>
 *
 * fileCount is 0 when no files were present for the target on this business date.
 */
public record TargetIngestCompletedEvent(
        Target target,
        String businessDate,
        long jobId,
        boolean success,
        int fileCount
) {}
