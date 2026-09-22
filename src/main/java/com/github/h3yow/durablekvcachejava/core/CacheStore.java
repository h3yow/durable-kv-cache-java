package com.github.h3yow.durablekvcachejava.core;

import com.github.h3yow.durablekvcachejava.config.CacheProperties;
import com.github.h3yow.durablekvcachejava.exception.CacheException;
import com.github.h3yow.durablekvcachejava.exception.CapacityExceededException;
import com.github.h3yow.durablekvcachejava.exception.InvalidRequestException;
import com.github.h3yow.durablekvcachejava.exception.ServiceUnavailableException;
import com.github.h3yow.durablekvcachejava.exception.VersionConflictException;
import com.github.h3yow.durablekvcachejava.model.CacheEntry;
import com.github.h3yow.durablekvcachejava.model.DeleteResult;
import com.github.h3yow.durablekvcachejava.model.EntryView;
import com.github.h3yow.durablekvcachejava.model.LifecycleState;
import com.github.h3yow.durablekvcachejava.model.StatusSnapshot;
import com.github.h3yow.durablekvcachejava.model.WalRecord;
import com.github.h3yow.durablekvcachejava.persistence.RecoveryReport;
import com.github.h3yow.durablekvcachejava.persistence.RecoveryService;
import com.github.h3yow.durablekvcachejava.persistence.SnapshotStore;
import com.github.h3yow.durablekvcachejava.persistence.WalWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;

/**
 * 缓存核心。
 *
 * <h3>顺序与一致性</h3>
 * <ul>
 *   <li>所有写操作（PUT/DELETE/淘汰/过期清理/快照压缩）在同一把写锁下串行执行；
 *       全局 {@code sequence} 在持锁期间分配并立即按同样顺序追加 WAL，因此全局提交顺序、
 *       WAL 物理顺序、每键 version 单调三者完全一致。同一键的过期、重写、删除并发时，
 *       后到者一定看到先到者的结果，旧值不可能覆盖新值。</li>
 *   <li>读操作无锁；读到的 {@link CacheEntry} 不可变。GET 与写操作对过期的判定共用
 *       {@link CacheEntry#isExpired(long)}，过期只认写入时由服务端确定的
 *       {@code expireAtEpochMs}，各入口结论一致、可解释。</li>
 * </ul>
 *
 * <h3>持久化与恢复</h3>
 * <ul>
 *   <li>写入在 WAL append 成功（默认含 fsync）后才返回成功确认；崩溃只丢失未确认尾部。</li>
 *   <li>启动 {@link #recover()} 期间状态为 RECOVERING，所有数据入口拒绝服务，
 *       不暴露半初始化数据；恢复失败进入 FAILED 并保留可区分原因，绝不静默继续。</li>
 *   <li>checkpoint 用“快照原子发布 + WAL 轮转/归档”压缩日志，任意崩溃点都保持可重放。</li>
 * </ul>
 */
@Component
public class CacheStore {

    private static final Logger log = LoggerFactory.getLogger(CacheStore.class);
    private static final int RECENT_EVICTION_LIMIT = 50;

    private final CacheProperties props;
    private final Path dataDir;
    private final SnapshotStore snapshotStore;
    private final RecoveryService recoveryService;
    private final Clock clock;

    private final ConcurrentHashMap<String, CacheEntry> entries = new ConcurrentHashMap<>();
    /** 全局提交序号：严格单调，等于 WAL 顺序。 */
    private final AtomicLong sequenceGen = new AtomicLong(0L);

    /** 写锁：串行所有变更操作；读侧不持锁。 */
    private final StampedLock mutationLock = new StampedLock();
    private volatile WalWriter wal;
    private final AtomicReference<LifecycleState> state = new AtomicReference<>(LifecycleState.CREATED);
    private volatile String lastError;
    private volatile RecoveryReport recoveryReport;
    private volatile long lastCheckpointSequence = -1L;

    private final AtomicLong totalPuts = new AtomicLong();
    private final AtomicLong totalDeletes = new AtomicLong();
    private final AtomicLong totalEvictions = new AtomicLong();
    private final AtomicLong totalExpiredPurges = new AtomicLong();

    /** 淘汰决策环形日志（容量占用与淘汰结果的本地观测依据）。 */
    private final java.util.concurrent.ArrayBlockingQueue<String> recentEvictions =
            new java.util.concurrent.ArrayBlockingQueue<>(RECENT_EVICTION_LIMIT);

    /** 可注入时钟（测试可控制时间）。 */
    public interface Clock {
        long epochMs();
        /** 单调纳秒，用于 LRU 的“最近访问”比较。 */
        long nano();
    }

    @org.springframework.beans.factory.annotation.Autowired
    public CacheStore(CacheProperties props) {
        this(props, new Clock() {
            @Override public long epochMs() { return System.currentTimeMillis(); }
            @Override public long nano() { return System.nanoTime(); }
        });
    }

    public CacheStore(CacheProperties props, Clock clock) {
        this.props = props;
        this.clock = clock;
        this.dataDir = props.getDataDir();
        this.snapshotStore = new SnapshotStore(dataDir);
        this.recoveryService = new RecoveryService(dataDir, snapshotStore, clock::epochMs);
    }

    // ------------------------------------------------------------------ 生命周期

    /**
     * 启动恢复。只允许在 CREATED 状态调用一次；恢复期间拒绝所有数据请求。
     * 失败时状态置为 FAILED 并保留原因码，绝不静默继续。
     */
    public void recover() {
        if (!state.compareAndSet(LifecycleState.CREATED, LifecycleState.RECOVERING)) {
            throw new ServiceUnavailableException(
                    "recover() only allowed once from CREATED, current state=" + state.get());
        }
        try {
            props.validate();
            RecoveryService.RecoveredState recovered = recoveryService.recover();
            installRecovered(recovered.liveRecords(), recovered.lastSequence());
            this.recoveryReport = recovered.report();
            Path walPath = WalWriter.newWalPath(dataDir, recovered.lastSequence() + 1, clock.epochMs());
            this.wal = new WalWriter(walPath, props.isFsyncEachWrite());
            this.wal.open();
            state.set(LifecycleState.ACTIVE);
            log.info("[recovery] {}", recovered.report().detail());
            for (WalRecord r : recovered.liveRecords()) {
                log.info("[recovery-load] key={} version={} seq={} expireAt={} expires={}",
                        r.key(), r.version(), r.sequence(), r.expireAtEpochMs(),
                        r.expireAtEpochMs() == CacheEntry.NO_EXPIRY ? "no" : "yes");
            }
        } catch (RuntimeException e) {
            lastError = e.getClass().getSimpleName() + ": " + e.getMessage();
            state.set(LifecycleState.FAILED);
            log.error("[recovery-failed] {}", lastError, e);
            throw e;
        }
    }

    /** 恢复产物装载：在 RECOVERING 状态下执行，内存此时不对外可见。 */
    void installRecovered(List<WalRecord> liveRecords, long lastSequence) {
        entries.clear();
        long nowNano = clock.nano();
        for (WalRecord r : liveRecords) {
            entries.put(r.key(), new CacheEntry(r.key(), r.value(), r.version(), r.sequence(),
                    r.expireAtEpochMs(), nowNano));
        }
        sequenceGen.set(Math.max(0L, lastSequence));
        lastCheckpointSequence = snapshotStore.lastSnapshotSequence();
    }

    public LifecycleState state() {
        return state.get();
    }

    public void shutdown() {
        long stamp = mutationLock.writeLock();
        try {
            if (wal != null) {
                wal.close();
            }
        } finally {
            mutationLock.unlockWrite(stamp);
        }
    }

    private void requireActive() {
        LifecycleState s = state.get();
        if (s != LifecycleState.ACTIVE) {
            throw new ServiceUnavailableException("cache is not ACTIVE (state=" + s
                    + (lastError != null ? ", lastError=" + lastError : "") + ")");
        }
    }

    // ------------------------------------------------------------------ 写入

    /**
     * 写入。
     *
     * @param ttlMillis 相对服务端的存活毫秒；null 表示永不过期；非 null 时必须 &gt; 0
     * @param expectedVersion 非空时要求当前有效版本等于该值（CAS），-1 表示要求键不存在/已失效
     */
    public CacheEntry put(String key, String value, Long ttlMillis, Long expectedVersion) {
        requireActive();
        validateKey(key);
        validateValue(value);
        long expireAt = resolveExpireAt(ttlMillis);

        long stamp = mutationLock.writeLock();
        try {
            long now = clock.epochMs();
            CacheEntry existing = entries.get(key);
            boolean existingExpired = existing != null && existing.isExpired(now);
            CacheEntry current = existingExpired ? null : existing;

            if (expectedVersion != null) {
                long actual = current == null ? -1L : current.version();
                if (actual != expectedVersion) {
                    throw new VersionConflictException(
                            "CAS conflict key=" + key + " expectedVersion=" + expectedVersion
                                    + " actualVersion=" + actual
                                    + " basis=server-side current live version");
                }
            }

            boolean newKey = current == null;
            if (newKey) {
                // 容量判定前必须移除所有过期残条目，否则它们既不被计入 liveSize
                // 又占据 entries，会让容量结论与真实占用不一致
                sweepExpiredHoldingLock(now, true);
                if (liveSize(now) >= props.getMaxEntries()) {
                    if (props.getEviction() == CacheProperties.EvictionMode.REJECT) {
                        throw new CapacityExceededException(
                                "capacity exceeded key=" + key + " liveSize=" + liveSize(now)
                                        + " maxEntries=" + props.getMaxEntries()
                                        + " mode=REJECT basis=eviction policy");
                    }
                    evictOne(now);
                    if (liveSize(now) >= props.getMaxEntries()) {
                        throw new CapacityExceededException(
                                "capacity exceeded and no evictable entry key=" + key
                                        + " maxEntries=" + props.getMaxEntries());
                    }
                }
            }

            long seq = sequenceGen.incrementAndGet();
            long version = current == null ? 1L : current.version() + 1L;
            CacheEntry next = new CacheEntry(key, value, version, seq, expireAt, clock.nano());
            wal.append(WalRecord.put(seq, key, version, expireAt, value));
            entries.put(key, next);
            totalPuts.incrementAndGet();
            log.info("[put-ok] key={} version={} seq={} expireAt={} ttl={} basis=accepted write",
                    key, version, seq,
                    expireAt == CacheEntry.NO_EXPIRY ? "never" : expireAt,
                    ttlMillis == null ? "none" : ttlMillis);
            maybeAutoCheckpoint();
            return next;
        } finally {
            mutationLock.unlockWrite(stamp);
        }
    }

    // ------------------------------------------------------------------ 读取

    public EntryView get(String key) {
        requireActive();
        validateKey(key);
        long now = clock.epochMs();
        CacheEntry entry = entries.get(key);
        if (entry == null) {
            log.info("[get-miss] key={} basis=no entry", key);
            return new EntryView(key, null, -1L, -1L, null, null);
        }
        if (entry.isExpired(now)) {
            log.info("[get-expired] key={} version={} expireAt={} now={} basis=server-side expiry",
                    key, entry.version(), entry.expireAtEpochMs(), now);
            removeIfExpired(key, entry, now);
            return new EntryView(key, null, entry.version(), entry.sequence(),
                    entry.expireAtEpochMs(), 0L);
        }
        touchAccess(key, entry);
        Long expireAt = entry.expires() ? entry.expireAtEpochMs() : null;
        Long ttl = entry.expires() ? Math.max(0L, entry.expireAtEpochMs() - now) : null;
        log.info("[get-hit] key={} version={} seq={} expireAt={} basis=live entry",
                key, entry.version(), entry.sequence(), expireAt);
        return new EntryView(key, entry.value(), entry.version(), entry.sequence(), expireAt, ttl);
    }

    // ------------------------------------------------------------------ 删除

    public DeleteResult delete(String key, Long expectedVersion) {
        requireActive();
        validateKey(key);
        long stamp = mutationLock.writeLock();
        try {
            long now = clock.epochMs();
            CacheEntry existing = entries.get(key);
            boolean expired = existing != null && existing.isExpired(now);
            CacheEntry current = expired ? null : existing;
            if (current == null) {
                if (expired) {
                    entries.remove(key, existing);
                    log.info("[delete-expired] key={} version={} expireAt={} basis=already expired",
                            key, existing.version(), existing.expireAtEpochMs());
                    return new DeleteResult(key, false, existing.version(),
                            "already expired basis=server-side expiry; removed stale entry");
                }
                log.info("[delete-miss] key={} basis=no live entry", key);
                return new DeleteResult(key, false, -1L, "no live entry basis=state");
            }
            if (expectedVersion != null && current.version() != expectedVersion) {
                throw new VersionConflictException(
                        "CAS conflict key=" + key + " expectedVersion=" + expectedVersion
                                + " actualVersion=" + current.version());
            }
            long seq = sequenceGen.incrementAndGet();
            // 墓碑版本 = 被删除版本 + 1，保证“删除后重写”版本继续单调
            long tombVersion = current.version() + 1L;
            wal.append(WalRecord.delete(seq, key, tombVersion, CacheEntry.NO_EXPIRY));
            entries.remove(key, current);
            totalDeletes.incrementAndGet();
            log.info("[delete-ok] key={} tombVersion={} seq={} basis=explicit delete",
                    key, tombVersion, seq);
            return new DeleteResult(key, true, tombVersion,
                    "deleted live entry version=" + current.version());
        } finally {
            mutationLock.unlockWrite(stamp);
        }
    }

    // ------------------------------------------------------------------ 过期清理

    /** 后台清理：移除所有已过期条目。返回移除数。幂等、可重复调用。 */
    public int purgeExpired() {
        LifecycleState s = state.get();
        if (s != LifecycleState.ACTIVE) {
            return 0;
        }
        long now = clock.epochMs();
        long stamp = mutationLock.writeLock();
        int removed = 0;
        try {
            var it = entries.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, CacheEntry> e = it.next();
                if (e.getValue().isExpired(now)) {
                    it.remove();
                    removed++;
                    log.info("[purge-expired] key={} version={} expireAt={} now={} basis=ttl reached",
                            e.getKey(), e.getValue().version(), e.getValue().expireAtEpochMs(), now);
                }
            }
        } finally {
            mutationLock.unlockWrite(stamp);
        }
        if (removed > 0) {
            totalExpiredPurges.addAndGet(removed);
        }
        return removed;
    }

    private void removeIfExpired(String key, CacheEntry observed, long now) {
        entries.computeIfPresent(key, (k, cur) ->
                (cur == observed && cur.isExpired(now)) ? null : cur);
    }

    private void touchAccess(String key, CacheEntry observed) {
        long nowNano = clock.nano();
        entries.computeIfPresent(key, (k, cur) -> (cur == observed) ? cur.withAccess(nowNano) : cur);
    }

    /**
     * 持锁（或在写锁内）清扫全部已过期残条目。
     *
     * @param countPurges 是否计入 totalExpiredPurges（写路径内的惰性清扫不重复计数）
     * @return 移除数
     */
    private int sweepExpiredHoldingLock(long now, boolean countPurges) {
        int removed = 0;
        var it = entries.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, CacheEntry> e = it.next();
            if (e.getValue().isExpired(now)) {
                it.remove();
                removed++;
                log.info("[sweep-expired] key={} version={} expireAt={} now={} basis=ttl reached",
                        e.getKey(), e.getValue().version(), e.getValue().expireAtEpochMs(), now);
            }
        }
        if (countPurges && removed > 0) {
            totalExpiredPurges.addAndGet(removed);
        }
        return removed;
    }

    /** 当前未过期条目数（容量判定唯一口径）。 */
    private int liveSize(long now) {        int n = 0;
        for (CacheEntry e : entries.values()) {
            if (!e.isExpired(now)) {
                n++;
            }
        }
        return n;
    }

    // ------------------------------------------------------------------ 淘汰

    /**
     * 在写锁内调用：按“过期优先 → 最久未访问 → 键名字典序”选出唯一确定的淘汰目标。
     * 淘汰同样追加 DELETE 记录，重启重放时结论不会复活、顺序可解释。
     */
    private void evictOne(long now) {
        CacheEntry victim = null;
        for (CacheEntry e : entries.values()) {
            if (victim == null || evictionOrder(e, victim, now) < 0) {
                victim = e;
            }
        }
        if (victim == null) {
            return;
        }
        boolean expired = victim.isExpired(now);
        long seq = sequenceGen.incrementAndGet();
        long tombVersion = victim.version() + 1L;
        wal.append(WalRecord.delete(seq, victim.key(), tombVersion, CacheEntry.NO_EXPIRY));
        entries.remove(victim.key(), victim);
        totalEvictions.incrementAndGet();
        String reason = expired ? "expired-first" : "lru-then-key-asc";
        String detail = String.format(
                "evict key=%s version=%d lastAccessNano=%d expireAt=%d reason=%s at=%d liveSize=%d/%d",
                victim.key(), victim.version(), victim.lastAccessNano(), victim.expireAtEpochMs(),
                reason, now, liveSize(now), props.getMaxEntries());
        log.info("[eviction] {}", detail);
        while (!recentEvictions.offer(detail)) {
            recentEvictions.poll();
        }
    }

    /**
     * 淘汰排序（升序，最先被淘汰）：
     * 1) 已过期优先（expireAt 升序，相同按键名）；
     * 2) 未过期按最近访问纳秒升序（LRU）；
     * 3) 完全相同则按键名字典序，保证顺序唯一确定。
     */
    private int evictionOrder(CacheEntry a, CacheEntry b, long now) {
        boolean ax = a.isExpired(now);
        boolean bx = b.isExpired(now);
        if (ax != bx) {
            return ax ? -1 : 1;
        }
        if (ax) {
            int c = Long.compare(a.expireAtEpochMs(), b.expireAtEpochMs());
            if (c != 0) {
                return c;
            }
            return a.key().compareTo(b.key());
        }
        int c = Long.compare(a.lastAccessNano(), b.lastAccessNano());
        if (c != 0) {
            return c;
        }
        return a.key().compareTo(b.key());
    }

    // ------------------------------------------------------------------ 快照压缩

    private void maybeAutoCheckpoint() {
        long threshold = props.getWalCompactionBytes();
        WalWriter w = wal;
        if (threshold <= 0 || w == null || w.bytesWritten() < threshold) {
            return;
        }
        try {
            checkpoint();
        } catch (RuntimeException e) {
            // 自动压缩失败不影响已确认写入（WAL 完好），记录可观测错误；手动 checkpoint 会再次抛出
            lastError = "auto-checkpoint failed: " + e.getMessage();
            log.warn("[checkpoint-auto-failed] {}", lastError, e);
        }
    }

    /**
     * 同步快照压缩：
     * <ol>
     *   <li>持写锁得到一致性视图（此刻无任何写操作在途）；</li>
     *   <li>原子发布包含全部活动 PUT 的 snapshot.db（临时文件 + force + ATOMIC_MOVE）；</li>
     *   <li>关闭旧 WAL，打开从下一 sequence 起步的新 WAL 并 force；</li>
     *   <li>旧 WAL 归档为 .archived；崩溃后仍与快照按 sequence 去重重放，不叠加不丢失。</li>
     * </ol>
     */
    public long checkpoint() {
        requireActive();
        long stamp = mutationLock.writeLock();
        try {
            long now = clock.epochMs();
            List<WalRecord> live = new ArrayList<>(entries.size());
            for (CacheEntry e : entries.values()) {
                if (!e.isExpired(now)) {
                    live.add(WalRecord.put(e.sequence(), e.key(), e.version(),
                            e.expireAtEpochMs(), e.value()));
                }
            }
            live.sort(Comparator.comparingLong(WalRecord::sequence));

            SnapshotStore.SnapshotResult snap = snapshotStore.writeSnapshot(live);

            WalWriter old = wal;
            Path oldPath = old.file();
            old.close();
            Path newPath = WalWriter.newWalPath(dataDir, sequenceGen.get() + 1, clock.epochMs());
            WalWriter next = new WalWriter(newPath, props.isFsyncEachWrite());
            next.open();
            wal = next;
            lastCheckpointSequence = snap.lastSequence();

            try {
                Path archived = oldPath.resolveSibling(oldPath.getFileName() + ".archived");
                java.nio.file.Files.move(oldPath, archived,
                        java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                SnapshotStore.forceDirectory(dataDir);
            } catch (java.io.IOException e) {
                // 归档失败不影响正确性（旧 WAL 保留，恢复按 seq 去重），但必须可观测
                lastError = "archive old WAL failed: " + e.getMessage();
                log.warn("[checkpoint-archive-failed] {}", lastError);
            }
            log.info("[checkpoint] records={} bytes={} lastSeq={} newWal={}",
                    snap.records(), snap.bytes(), snap.lastSequence(), newPath.getFileName());
            return snap.lastSequence();
        } finally {
            mutationLock.unlockWrite(stamp);
        }
    }

    // ------------------------------------------------------------------ 校验/工具

    private void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new InvalidRequestException("key must not be null/empty basis=request validation");
        }
        if (key.length() > 4096) {
            throw new InvalidRequestException(
                    "key too long (" + key.length() + " > 4096 chars) basis=request validation");
        }
    }

    private void validateValue(String value) {
        if (value == null) {
            throw new InvalidRequestException("value must not be null basis=request validation");
        }
        long bytes = value.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > props.getMaxValueBytes()) {
            throw new InvalidRequestException("value too large: " + bytes + " bytes > maxValueBytes="
                    + props.getMaxValueBytes() + " basis=request validation");
        }
    }

    private long resolveExpireAt(Long ttlMillis) {
        if (ttlMillis == null) {
            return CacheEntry.NO_EXPIRY;
        }
        if (ttlMillis <= 0) {
            throw new InvalidRequestException(
                    "ttlMillis must be > 0 or omitted (null = never expire), got " + ttlMillis
                            + " basis=request validation");
        }
        return Math.addExact(clock.epochMs(), ttlMillis);
    }

    public int size() {
        return entries.size();
    }

    public StatusSnapshot status() {
        LifecycleState s = state.get();
        WalWriter w = wal;
        return new StatusSnapshot(
                s,
                entries.size(),
                props.getMaxEntries(),
                w == null ? 0L : w.bytesWritten(),
                lastCheckpointSequence,
                totalPuts.get(),
                totalDeletes.get(),
                totalEvictions.get(),
                totalExpiredPurges.get(),
                List.copyOf(recentEvictions),
                recoveryReport == null ? null : recoveryReport.detail(),
                lastError);
    }
}
