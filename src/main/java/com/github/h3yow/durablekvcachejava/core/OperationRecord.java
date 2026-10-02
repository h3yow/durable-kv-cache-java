package com.github.h3yow.durablekvcachejava.core;

/**
 * WAL 单条操作记录。
 *
 * @param seq         单调递增序号
 * @param type        操作类型
 * @param key         键；{@link OperationType#GBEGIN}/{@link OperationType#GCOMMIT} 帧中为 groupId
 * @param value       值（DELETE/EVICT/帧记录时为 null）
 * @param ttlMillis   TTL（DELETE/EVICT 时为 0；GBEGIN 帧中为组成员操作数，GCOMMIT 帧中为 0）
 * @param timestampMs 操作时间戳
 * @param groupId     所属整组的 ID；不属于整组的历史/单键记录为 null
 */
public record OperationRecord(long seq, OperationType type, String key, String value,
                              long ttlMillis, long timestampMs, String groupId) {

    /** 不含组信息的构造器（单键 PUT/DELETE/EVICT，groupId 为 null）。 */
    public OperationRecord(long seq, OperationType type, String key, String value,
                           long ttlMillis, long timestampMs) {
        this(seq, type, key, value, ttlMillis, timestampMs, null);
    }

    /**
     * 构造一条整组帧记录（GBEGIN/GCOMMIT），帧的 key 字段承载 groupId。
     *
     * @param frameTtl GBEGIN 时为"GBEGIN 与 GCOMMIT 之间的记录总数（含 EVICT 与成员操作）"；
     *                 GCOMMIT 时必须为 0
     */
    public static OperationRecord frame(long seq, OperationType frameType, String groupId,
                                        long frameTtl, long timestampMs) {
        return new OperationRecord(seq, frameType, groupId, null, frameTtl, timestampMs, groupId);
    }
}
