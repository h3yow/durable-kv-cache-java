package com.github.h3yow.durablekvcachejava.core;

/**
 * WAL 单条操作记录。
 *
 * @param seq        单调递增序号
 * @param type       操作类型
 * @param key        键
 * @param value      值（DELETE 时为 null）
 * @param ttlMillis  TTL（DELETE 时为 0）
 * @param timestampMs 操作时间戳
 */
public record OperationRecord(long seq, OperationType type, String key, String value,
                              long ttlMillis, long timestampMs) {
    // TODO 骨架
}
