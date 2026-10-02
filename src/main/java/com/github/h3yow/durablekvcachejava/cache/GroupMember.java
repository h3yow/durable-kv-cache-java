package com.github.h3yow.durablekvcachejava.cache;

/**
 * 整组提交中的单个成员操作。
 *
 * @param key       成员键（非空）
 * @param action    INSERT / UPDATE / DELETE
 * @param value     写入值（仅 INSERT/UPDATE 需要，DELETE 时为 null）
 * @param ttlMillis TTL（仅 INSERT/UPDATE 需要正数，DELETE 时为 0）
 */
public record GroupMember(String key, GroupAction action, String value, Long ttlMillis) {

    /** 构造一个 INSERT 成员（要求键当前不存在）。 */
    public static GroupMember insert(String key, String value, long ttlMillis) {
        return new GroupMember(key, GroupAction.INSERT, value, ttlMillis);
    }

    /** 构造一个 UPDATE 成员（要求键当前存活）。 */
    public static GroupMember update(String key, String value, long ttlMillis) {
        return new GroupMember(key, GroupAction.UPDATE, value, ttlMillis);
    }

    /** 构造一个 DELETE 成员（要求键当前存活）。 */
    public static GroupMember delete(String key) {
        return new GroupMember(key, GroupAction.DELETE, null, 0L);
    }
}
