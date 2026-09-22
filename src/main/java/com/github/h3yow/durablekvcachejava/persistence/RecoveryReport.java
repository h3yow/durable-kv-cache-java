package com.github.h3yow.durablekvcachejava.persistence;

/** 一次恢复的可解释结果，用于日志与 /admin/status。 */
public record RecoveryReport(
        boolean success,
        long snapshotRecords,
        long walRecords,
        long appliedRecords,
        long skippedDuplicates,
        long recoveredEntries,
        long recoveredExpired,
        long lastSequence,
        String failureReason,
        String detail
) {
}
