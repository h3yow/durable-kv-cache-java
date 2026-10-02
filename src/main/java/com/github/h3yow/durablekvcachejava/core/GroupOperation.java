package com.github.h3yow.durablekvcachejava.core;

/**
 * 整组操作中的单个成员操作（PUT 或 DELETE）。
 *
 * @param op         操作类型，仅允许 {@link OperationType#PUT} / {@link OperationType#DELETE}
 * @param key        目标键
 * @param value      PUT 的值（DELETE 时为 null）
 * @param ttlMillis  PUT 的 TTL（DELETE 时为 0）
 */
public record GroupOperation(OperationType op, String key, String value, Long ttlMillis) {

    /** 构造一个组内 PUT 成员。 */
    public static GroupOperation put(String key, String value, long ttlMillis) {
        return new GroupOperation(OperationType.PUT, key, value, ttlMillis);
    }

    /** 构造一个组内 DELETE 成员。 */
    public static GroupOperation delete(String key) {
        return new GroupOperation(OperationType.DELETE, key, null, 0L);
    }
}
