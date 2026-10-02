package com.github.h3yow.durablekvcachejava.cache;

import com.github.h3yow.durablekvcachejava.core.GroupCommitResult;
import com.github.h3yow.durablekvcachejava.core.GroupOperation;

import java.util.List;

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
     * 整组原子提交：组内成员操作要么全部生效，要么全部不生效。
     *
     * @param groupId 调用方提供的组 ID（用于重试去重）；null 时由服务端生成
     * @param members 组内成员操作（PUT/DELETE），同一组内键不得重复
     */
    GroupCommitResult commitGroup(String groupId, List<GroupOperation> members);

    /** 当前可观测指标。 */
    Metrics metrics();

    /** PUT 结果。 */
    record PutResult(String key, long expireAtMs, boolean evicted, String evictedKey, int liveEntries) {
    }

    /** 容量、淘汰与整组提交可观测指标。 */
    record Metrics(int maxEntries, int liveEntries, long totalPutCount, long totalDeleteCount,
                   long evictionCount, long expiredScanCount, long lastExpireAtMs, String state,
                   long groupCommitCount, long groupSuccessCount, long groupDuplicateCount,
                   long groupRejectedStaleCount, long groupRejectedCapacityCount) {
    }
}
