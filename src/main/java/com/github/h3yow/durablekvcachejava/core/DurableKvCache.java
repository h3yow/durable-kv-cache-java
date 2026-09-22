package com.github.h3yow.durablekvcachejava.core;

import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.CapacityExceededException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.InvalidRequestException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.StateConflictException;
import com.github.h3yow.durablekvcachejava.cache.KeyValueCache;
import com.github.h3yow.durablekvcachejava.config.CacheProperties;
import com.github.h3yow.durablekvcachejava.eviction.EvictionTracker;
import com.github.h3yow.durablekvcachejava.persistence.PersistenceLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * 单一锁串行化的持久化键值缓存状态机。
 *
 * <p>一致性模型：所有读写删均在同一把监视器锁内完成"判定 → 变更内存 → 追加 WAL"，
 * 因此同键的过期、重写、删除无论从哪个入口并发进入，都只有一个确定的全序结果，
 * 绝不返回已过期或被覆盖的旧值。</p>
 *
 * <p>持久化次序：内存可见的 PUT/DELETE 必先 {@code wal.append + force} 成功；
 * 若 WAL 追加失败，内存回滚并抛出 PersistenceException，调用方得到可区分的失败。</p>
 */
public final class DurableKvCache implements KeyValueCache {

    private static final Logger log = LoggerFactory.getLogger(DurableKvCache.class);

    private final PersistenceLog wal;
    private final EvictionTracker tracker;
    private final CacheProperties properties;

    private final Map<String, CacheEntry> store = new HashMap<>();
    private long nextSeq;
    private boolean ready = false;

    private long totalPutCount;
    private long totalDeleteCount;
    private long evictionCount;
    private long expiredScanCount;

    public DurableKvCache(PersistenceLog wal, EvictionTracker tracker, CacheProperties properties) {
        this.wal = wal;
        this.tracker = tracker;
        this.properties = properties;
        this.nextSeq = wal.nextSeq();
        log.info("缓存状态机初始化: nextSeq={}, maxEntries={}", nextSeq, properties.getMaxEntries());
    }

    // ------------------------------------------------------------------ 对外操作

    @Override
    public synchronized PutResult put(String key, String value, Long ttlMillis) {
        ensureReady();
        validatePut(key, value, ttlMillis);
        long now = System.currentTimeMillis();
        purgeExpired(now);

        boolean overwrite = store.containsKey(key);
        String evictedKey = null;
        CacheEntry evictedEntry = null;
        long evictSeq = -1L;
        if (!overwrite && properties.getMaxEntries() > 0 && store.size() >= properties.getMaxEntries()) {
            // 容量已满：按确定 LRU 淘汰最旧的一个键。
            evictedKey = tracker.evictNext();
            if (evictedKey == null || !store.containsKey(evictedKey)) {
                throw new CapacityExceededException(
                        "容量超限且无可淘汰键: maxEntries=" + properties.getMaxEntries());
            }
            evictedEntry = store.remove(evictedKey);
            // 淘汰先于新 PUT 获得序号并落盘：淘汰结论是状态机全序的一部分，
            // 重启重放时被淘汰键不得因历史 PUT 而复活。
            evictSeq = nextSeq++;
            try {
                wal.append(new OperationRecord(evictSeq, OperationType.EVICT,
                        evictedKey, null, 0L, now));
            } catch (RuntimeException e) {
                store.put(evictedKey, evictedEntry);
                tracker.reinsertOldest(evictedKey);
                nextSeq--;
                throw e;
            }
            evictionCount++;
            log.info("淘汰判定 key={} seq={} 依据=LRU最久未使用 liveBefore={} maxEntries={}",
                    evictedKey, evictSeq, store.size() + 1, properties.getMaxEntries());
        }

        long expireAt = now + ttlMillis;
        CacheEntry entry = new CacheEntry(value, expireAt);
        long seq = nextSeq++;

        // 先持久化；失败则回滚所有内存改动（含被淘汰键），调用方看不到半成品。
        try {
            wal.append(new OperationRecord(seq, OperationType.PUT, key, value, ttlMillis, now));
        } catch (RuntimeException e) {
            rollbackPut(key, overwrite, store.get(key), evictedKey, evictedEntry);
            nextSeq--;
            if (evictedKey != null) {
                // EVICT 已 force 而内存回滚恢复了被淘汰键：追加一条 EVICT 补偿记录，
                // 使 WAL 与内存重新一致（重放为幂等淘汰）。补偿也失败则直接抛出，
                // 重启重放会收敛到"该键被淘汰"，绝不暴露脏值。
                long compensateSeq = nextSeq++;
                wal.append(new OperationRecord(compensateSeq, OperationType.EVICT,
                        evictedKey, null, 0L, now));
            }
            throw e;
        }

        store.put(key, entry);
        tracker.touch(key);
        totalPutCount++;
        log.info("写入成功 key={} seq={} ttlMillis={} expireAt={} 判定={} evictedKey={} live={}",
                key, seq, ttlMillis, expireAt, overwrite ? "重写覆盖旧值" : "新增",
                evictedKey == null ? "无" : evictedKey, store.size());
        return new PutResult(key, expireAt, evictedKey != null, evictedKey, store.size());
    }

    @Override
    public synchronized String get(String key) {
        ensureReady();
        validateKey(key);
        long now = System.currentTimeMillis();
        CacheEntry e = store.get(key);
        if (e == null) {
            log.info("读取判定 key={} 依据=键不存在 结果=miss", key);
            return null;
        }
        if (now >= e.expireAtMs()) {
            // 惰性过期：判定与删除在同一把锁内原子完成。
            store.remove(key);
            tracker.remove(key);
            expiredScanCount++;
            log.info("读取判定 key={} 依据=已过期 now={} >= expireAt={} 结果=expired并移除",
                    key, now, e.expireAtMs());
            return null;
        }
        tracker.touch(key);
        log.info("读取判定 key={} 依据=未过期 now={} < expireAt={} 结果=hit", key, now, e.expireAtMs());
        return e.value();
    }

    @Override
    public synchronized boolean delete(String key) {
        ensureReady();
        validateKey(key);
        long now = System.currentTimeMillis();
        purgeExpired(now);
        CacheEntry existed = store.remove(key);
        boolean removed = existed != null;
        if (removed) {
            tracker.remove(key);
        }
        long seq = nextSeq++;
        try {
            wal.append(new OperationRecord(seq, OperationType.DELETE, key, null, 0L, now));
        } catch (RuntimeException e) {
            // WAL 失败：回滚内存，删除对外不可见；键恢复到最旧位置（保守的 LRU 顺序）。
            if (removed) {
                store.put(key, existed);
                tracker.reinsertOldest(key);
            }
            nextSeq--;
            throw e;
        }
        totalDeleteCount++;
        log.info("删除成功 key={} seq={} 判定={} live={}", key, seq,
                removed ? "删除了现存键" : "键不存在(幂等删除)", store.size());
        return removed;
    }

    @Override
    public synchronized Metrics metrics() {
        ensureReady();
        long now = System.currentTimeMillis();
        int live = 0;
        long nearest = Long.MAX_VALUE;
        for (CacheEntry e : store.values()) {
            if (now < e.expireAtMs()) {
                live++;
                nearest = Math.min(nearest, e.expireAtMs());
            }
        }
        return new Metrics(properties.getMaxEntries(), live, totalPutCount, totalDeleteCount,
                evictionCount, expiredScanCount, nearest == Long.MAX_VALUE ? -1L : nearest,
                ready ? "READY" : "RECOVERING");
    }

    // ------------------------------------------------------------------ 恢复支持

    /**
     * 恢复重放：不写 WAL、不改序号、不做容量淘汰，直接按日志全序重建内存。
     * 重复恢复由 Spring 只装配一次保证；对同一缓存二次调用会抛状态冲突，
     * 防止数据叠加。
     */
    public synchronized RecoverySummary replayFrom(OperationRecord r, long now) {
        if (ready) {
            throw new StateConflictException("缓存已就绪，禁止重复恢复以避免数据叠加");
        }
        switch (r.type()) {
            case PUT -> {
                long expireAt = r.timestampMs() + r.ttlMillis();
                if (now >= expireAt) {
                    // 重放时已过期：不进入存活表，但记录"跳过"，恢复结论可解释。
                    store.remove(r.key());
                    tracker.remove(r.key());
                    return RecoverySummary.expired();
                }
                boolean overwrite = store.containsKey(r.key());
                store.put(r.key(), new CacheEntry(r.value(), expireAt));
                tracker.touch(r.key());
                if (r.seq() + 1 > nextSeq) {
                    nextSeq = r.seq() + 1;
                }
                return overwrite ? RecoverySummary.putOverwrite() : RecoverySummary.putNew();
            }
            case DELETE -> {
                boolean had = store.remove(r.key()) != null;
                tracker.remove(r.key());
                if (r.seq() + 1 > nextSeq) {
                    nextSeq = r.seq() + 1;
                }
                return had ? RecoverySummary.deleteHit() : RecoverySummary.deleteMiss();
            }
            case EVICT -> {
                // 淘汰结论在恢复全序中生效：即使此前该键有 PUT 记录，也必须被移除。
                boolean had = store.remove(r.key()) != null;
                tracker.remove(r.key());
                if (had) {
                    // 恢复历史淘汰计数，使重启后 /admin/metrics 的淘汰结论连续可观测。
                    evictionCount++;
                }
                if (r.seq() + 1 > nextSeq) {
                    nextSeq = r.seq() + 1;
                }
                return had ? RecoverySummary.evictHit() : RecoverySummary.deleteMiss();
            }
            default -> throw new IllegalArgumentException("未知操作类型: " + r.type());
        }
    }

    /** 恢复期间也可安全调用的存活条目数（基于当前时间实时判过期）。 */
    public synchronized int synchronizedLiveEntries() {
        long now = System.currentTimeMillis();
        int live = 0;
        for (CacheEntry e : store.values()) {
            if (now < e.expireAtMs()) {
                live++;
            }
        }
        return live;
    }

    /** 恢复完成后调用：此后才允许对外服务。 */
    public synchronized void markReady() {
        if (ready) {
            throw new StateConflictException("缓存已标记就绪，重复就绪调用被拒绝");
        }
        ready = true;
        log.info("缓存就绪 liveEntries={} nextSeq={}", store.size(), nextSeq);
    }

    // ------------------------------------------------------------------ 内部逻辑

    private void ensureReady() {
        if (!ready) {
            throw new StateConflictException("缓存尚处于恢复阶段(RECOVERING)，请求被拒绝");
        }
    }

    private void validatePut(String key, String value, Long ttlMillis) {
        validateKey(key);
        if (value == null) {
            throw new InvalidRequestException("value 不得为 null（删除请用 DELETE）");
        }
        int bytes = value.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > properties.getMaxValueBytes()) {
            throw new InvalidRequestException(
                    "value 超长: " + bytes + " > maxValueBytes=" + properties.getMaxValueBytes());
        }
        if (ttlMillis == null) {
            throw new InvalidRequestException("ttlMillis 为必填参数");
        }
        if (ttlMillis <= 0) {
            throw new InvalidRequestException("ttlMillis 必须为正数，实际: " + ttlMillis);
        }
    }

    private void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new InvalidRequestException("key 不得为空");
        }
        if (key.length() > properties.getMaxKeyLength()) {
            throw new InvalidRequestException(
                    "key 超长: " + key.length() + " > maxKeyLength=" + properties.getMaxKeyLength());
        }
    }

    /** 顺手扫描移除当前已过期键（过期判定依据：now &gt;= expireAt）。 */
    private void purgeExpired(long now) {
        var it = store.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, CacheEntry> en = it.next();
            if (now >= en.getValue().expireAtMs()) {
                it.remove();
                tracker.remove(en.getKey());
                expiredScanCount++;
                log.info("过期清理 key={} 依据=now {} >= expireAt {}",
                        en.getKey(), now, en.getValue().expireAtMs());
            }
        }
    }

    private void rollbackPut(String key, boolean overwrite, CacheEntry previousEntry,
                             String evictedKey, CacheEntry evictedEntry) {
        if (overwrite) {
            // 重写失败：旧值在 WAL 成功前始终保留在 store/tracker 中，原状态即正确，仅显式还原。
            if (previousEntry != null) {
                store.put(key, previousEntry);
            }
        } else {
            store.remove(key);
            tracker.remove(key);
        }
        if (evictedKey != null && evictedEntry != null) {
            store.put(evictedKey, evictedEntry);
            tracker.reinsertOldest(evictedKey);
            evictionCount--;
        }
        log.error("WAL 追加失败回滚 key={} overwrite={} restoredEvicted={}",
                key, overwrite, evictedKey == null ? "无" : evictedKey);
    }

    /** 重放单条的分类计数。 */
    public record RecoverySummary(Kind kind) {
        public enum Kind { PUT_NEW, PUT_OVERWRITE, DELETE_HIT, DELETE_MISS, EXPIRED_SKIP, EVICT_HIT }

        static RecoverySummary putNew() {
            return new RecoverySummary(Kind.PUT_NEW);
        }

        static RecoverySummary putOverwrite() {
            return new RecoverySummary(Kind.PUT_OVERWRITE);
        }

        static RecoverySummary deleteHit() {
            return new RecoverySummary(Kind.DELETE_HIT);
        }

        static RecoverySummary deleteMiss() {
            return new RecoverySummary(Kind.DELETE_MISS);
        }

        static RecoverySummary expired() {
            return new RecoverySummary(Kind.EXPIRED_SKIP);
        }

        static RecoverySummary evictHit() {
            return new RecoverySummary(Kind.EVICT_HIT);
        }
    }
}
