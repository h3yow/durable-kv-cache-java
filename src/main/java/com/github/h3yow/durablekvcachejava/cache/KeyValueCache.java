package com.github.h3yow.durablekvcachejava.cache;

import java.util.List;
import java.util.Map;

/**
 * 对外键值缓存服务接口。
 */
public interface KeyValueCache {

    /** 写入并返回带过期时间的最终状态描述。 */
    PutResult put(String key, String value, Long ttlMillis);

    /** 读取；不存在或已过期返回 null。 */
    String get(String key);

    /** 删除，返回是否删除了一个现存（未过期）键。 */
    boolean delete(String key);

    /**
     * 整组原子提交：组内全部成员要么全部生效，要么全部不生效。
     * 提交期间任何其它入口只能看到提交前或提交后的完整状态。
     *
     * @param groupId 调用方提供的非空组标识；相同 groupId + 相同成员的重复提交幂等
     * @param members 非空成员列表（组内键不得重复）
     */
    GroupResult commitGroup(String groupId, List<GroupMember> members);

    /** 查询一个 groupId 的已确认提交结论；从未确认提交返回 null。 */
    GroupResult groupStatus(String groupId);

    /** 当前存活键的一致性快照（键 -> 值），供整组校验/本地观测使用。 */
    Map<String, String> snapshot();

    /** 当前可观测指标。 */
    Metrics metrics();

    /** PUT 结果。 */
    record PutResult(String key, long expireAtMs, boolean evicted, String evictedKey, int liveEntries) {
    }

    /** 容量与淘汰可观测指标。 */
    record Metrics(int maxEntries, int liveEntries, long totalPutCount, long totalDeleteCount,
                   long evictionCount, long expiredScanCount, long lastExpireAtMs, String state) {
    }
}
