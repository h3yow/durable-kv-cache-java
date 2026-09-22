package com.github.h3yow.durablekvcachejava.model;

/**
 * 内存中不可变的键值条目。
 *
 * <p>顺序约定（全局唯一确定顺序的基础）：
 * <ul>
 *   <li>{@code sequence}：服务实例内单调递增的全局序号，严格等于 WAL 追加顺序；</li>
 *   <li>{@code version}：每个键各自单调递增的版本号，重写/删除均以 CAS 保证无旧值覆盖；</li>
 *   <li>{@code expireAtEpochMs}：由服务端时钟在写入时确定，读取/重写/删除都以此为唯一过期依据。</li>
 * </ul>
 */
public record CacheEntry(
        String key,
        String value,
        long version,
        long sequence,
        long expireAtEpochMs,
        long lastAccessNano
) {
    /** expireAtEpochMs 使用该哨兵值表示永不过期。 */
    public static final long NO_EXPIRY = Long.MAX_VALUE;

    /**
     * 唯一过期判定：当前时间（epoch 毫秒）达到过期点即为过期，且结论对所有入口一致。
     * 返回 false 的含义包括“永不过期”和“尚未到期”，调用方不需要区分。
     */
    public boolean isExpired(long nowEpochMs) {
        return expireAtEpochMs != NO_EXPIRY && nowEpochMs >= expireAtEpochMs;
    }

    public boolean expires() {
        return expireAtEpochMs != NO_EXPIRY;
    }

    /** 返回更新了最近访问时间的新实例（LRU 依据），其余字段不变。 */
    public CacheEntry withAccess(long nowNano) {
        return new CacheEntry(key, value, version, sequence, expireAtEpochMs, nowNano);
    }

    /** 返回新版本条目（同一次重写使用），lastAccess 同时刷新。 */
    public CacheEntry withRevision(String newValue, long newVersion, long newSequence,
                                  long newExpireAtEpochMs, long nowNano) {
        return new CacheEntry(key, newValue, newVersion, newSequence, newExpireAtEpochMs, nowNano);
    }
}
