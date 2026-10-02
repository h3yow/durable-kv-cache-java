package com.github.h3yow.durablekvcachejava.eviction;

/**
 * 淘汰顺序跟踪器：确定的 LRU 顺序。
 */
public interface EvictionTracker {

    /** 记录一次访问（PUT 或命中的 GET）。 */
    void touch(String key);

    /** 移除键的跟踪（删除/淘汰后）。 */
    void remove(String key);

    /** 把键恢复为最久未使用位置（WAL 失败回滚淘汰用）。 */
    void reinsertOldest(String key);

    /** 返回并移除最久未使用的键；为空时返回 null。 */
    String evictNext();

    /** 当前跟踪的键数量。 */
    int size();

    /**
     * 按 LRU 从旧到新返回键的快照（不移除任何键）。
     * 用于整组提交在"不产生任何副作用"的前提下预演淘汰顺序，判断能否为整组腾出足够空间。
     */
    java.util.List<String> peekOldestFirst();

    /** 清空全部跟踪状态。 */
    void clear();
}
