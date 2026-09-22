package com.github.h3yow.durablekvcachejava.eviction;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 基于 access-order {@link LinkedHashMap} 的确定 LRU 顺序跟踪器。
 *
 * <p>顺序语义：{@link #touch(String)} 一次就把键标记为"最近使用"并移到链表尾；
 * 链表头始终是最久未被 touch 的键。相同键重复 touch 不产生重复占位。
 * 所有方法由外部状态机的同一把锁串行调用，自身不再加锁。</p>
 */
public final class LruEvictionTracker implements EvictionTracker {

    private final LinkedHashMap<String, Boolean> order;

    public LruEvictionTracker() {
        this.order = new LinkedHashMap<>(16, 0.75f, true);
    }

    @Override
    public void touch(String key) {
        order.put(key, Boolean.TRUE);
    }

    @Override
    public void remove(String key) {
        order.remove(key);
    }

    @Override
    public String evictNext() {
        if (order.isEmpty()) {
            return null;
        }
        Iterator<Map.Entry<String, Boolean>> it = order.entrySet().iterator();
        String oldest = it.next().getKey();
        it.remove();
        return oldest;
    }

    @Override
    public void reinsertOldest(String key) {
        // 重建链表：先放被恢复键，其余按键当前从旧到新顺序追加。
        LinkedHashMap<String, Boolean> rebuilt = new LinkedHashMap<>(order.size() + 1, 0.75f, true);
        rebuilt.put(key, Boolean.TRUE);
        rebuilt.putAll(order);
        order.clear();
        order.putAll(rebuilt);
    }

    @Override
    public int size() {
        return order.size();
    }

    @Override
    public void clear() {
        order.clear();
    }

    /** 按 LRU 从旧到新返回键的快照（可观测/日志用）。 */
    public java.util.List<String> snapshotOldestFirst() {
        return Collections.unmodifiableList(new java.util.ArrayList<>(order.keySet()));
    }
}
