package com.github.h3yow.durablekvcachejava.model;

import java.util.List;

/** 本地可观测状态快照（/admin/status）。 */
public record StatusSnapshot(
        LifecycleState state,
        int entries,
        int maxEntries,
        long currentWalBytes,
        long lastCheckpointSequence,
        long totalPuts,
        long totalDeletes,
        long totalEvictions,
        long totalExpiredPurges,
        List<String> recentEvictions,
        String recoveryReport,
        String lastError
) {
}
