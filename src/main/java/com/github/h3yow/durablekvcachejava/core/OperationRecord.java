package com.github.h3yow.durablekvcachejava.core;

/**
 * WAL 单条操作记录。
 *
 * @param seq         单调递增序号
 * @param type        操作类型
 * @param key         键（TXBEG 时承载 groupId）
 * @param value       值（DELETE/EVICT/TX* 时为 null）
 * @param ttlMillis   TTL（DELETE/EVICT 时为 0；TXBEG 时承载成员数；TXEND 时承载淘汰键数）
 * @param timestampMs 操作时间戳
 * @param groupId     所属整组提交的组标识；历史单键记录（旧 7 字段 WAL 行）为 null
 */
public record OperationRecord(long seq, OperationType type, String key, String value,
                              long ttlMillis, long timestampMs, String groupId) {

    /** 单键操作记录（无组标识）。 */
    public OperationRecord(long seq, OperationType type, String key, String value,
                           long ttlMillis, long timestampMs) {
        this(seq, type, key, value, ttlMillis, timestampMs, null);
    }

    public boolean grouped() {
        return groupId != null;
    }
}
