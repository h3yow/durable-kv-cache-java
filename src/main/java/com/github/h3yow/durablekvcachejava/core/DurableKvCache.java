package com.github.h3yow.durablekvcachejava.core;

import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.CapacityExceededException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.GroupCapacityException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.GroupStaleException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.InvalidRequestException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.RecoveryException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.StateConflictException;
import com.github.h3yow.durablekvcachejava.cache.KeyValueCache;
import com.github.h3yow.durablekvcachejava.config.CacheProperties;
import com.github.h3yow.durablekvcachejava.eviction.EvictionTracker;
import com.github.h3yow.durablekvcachejava.persistence.PersistenceLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
    /** 已确认提交的整组：groupId -> 提交时的成员操作（用于重试去重与恢复后判定）。 */
    private final Map<String, CommittedGroup> committedGroups = new HashMap<>();
    private long nextSeq;
    private boolean ready = false;

    private long totalPutCount;
    private long totalDeleteCount;
    private long evictionCount;
    private long expiredScanCount;
    private long groupCommitCount;
    private long groupSuccessCount;
    private long groupRejectedStaleCount;
    private long groupRejectedCapacityCount;
    private long groupDuplicateCount;

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
    public synchronized GroupCommitResult commitGroup(String groupId, List<GroupOperation> members) {
        ensureReady();
        long now = System.currentTimeMillis();

        // ------------------------------------------------ 1. 参数与幂等校验（无副作用）
        validateGroupRequest(members);
        String gid = groupId == null ? "g-" + java.util.UUID.randomUUID() : groupId;
        validateGroupId(gid);
        for (GroupOperation m : members) {
            validateKey(m.key());
            if (m.op() == OperationType.PUT) {
                validatePut(m.key(), m.value(), m.ttlMillis());
            } else if (m.op() != OperationType.DELETE) {
                throw new InvalidRequestException("不支持的整组成员操作类型: " + m.op());
            }
        }
        // 同一组内键不允许重复：否则组内自身顺序就会产生歧义。
        java.util.Set<String> duplicateGuard = new java.util.HashSet<>();
        for (GroupOperation m : members) {
            if (!duplicateGuard.add(m.key())) {
                throw new InvalidRequestException(
                        "整组内键重复: key=" + m.key() + "（同一组中每个键只能出现一次）");
            }
        }

        CommittedGroup previous = committedGroups.get(gid);
        if (previous != null) {
            // 重试去重：同 groupId 同内容直接返回历史结论，绝不叠加执行；
            // 同 groupId 不同内容则按参数错误明确拒绝。
            if (!previous.spec().equals(members)) {
                groupCommitCount++;
                throw new InvalidRequestException(
                        "groupId=" + gid + " 已被其它整组使用，成员内容与本次提交不一致（拒绝复用）");
            }
            groupCommitCount++;
            groupDuplicateCount++;
            log.info("整组重复提交判定 groupId={} 依据=相同成员已提交 结果=DUPLICATE beginSeq={} commitSeq={}",
                    gid, previous.beginSeq(), previous.commitSeq());
            return new GroupCommitResult(gid, GroupCommitResult.Outcome.DUPLICATE, members.size(),
                    previous.evictedKeys(), previous.beginSeq(), previous.commitSeq(), liveCountLocked());
        }

        // ------------------------------------------------ 2. 前置状态判定（先于任何变更）
        // 组内 DELETE 的键必须在提交时刻仍然存活；已过期/已删除都要能区分，且整组拒绝、零副作用。
        List<GroupStaleException.StaleKey> stale = new ArrayList<>();
        for (GroupOperation m : members) {
            if (m.op() != OperationType.DELETE) {
                continue;
            }
            CacheEntry e = store.get(m.key());
            if (e == null) {
                stale.add(GroupStaleException.StaleKey.deleted(m.key()));
            } else if (now >= e.expireAtMs()) {
                stale.add(GroupStaleException.StaleKey.expired(m.key()));
            }
        }
        if (!stale.isEmpty()) {
            groupCommitCount++;
            groupRejectedStaleCount++;
            log.info("整组拒绝 groupId={} 依据=成员前置状态不满足 stale={} 结果=GROUP_STALE(零副作用)",
                    gid, stale);
            throw new GroupStaleException(gid, List.copyOf(stale));
        }

        // 复用单键入口的过期规则：顺手清理与本组无关的已过期键。
        purgeExpired(now);

        java.util.Set<String> deleteKeys = new java.util.HashSet<>();
        java.util.Set<String> putKeys = new java.util.HashSet<>();
        for (GroupOperation m : members) {
            (m.op() == OperationType.PUT ? putKeys : deleteKeys).add(m.key());
        }
        // 新增加的键 = PUT 成员中当前不存活的键（过期键已在上面被清理，同样算新增）。
        int newKeyCount = 0;
        for (String k : putKeys) {
            if (!store.containsKey(k)) {
                newKeyCount++;
            }
        }
        int deleteHitCount = 0;
        for (String k : deleteKeys) {
            if (store.containsKey(k)) {
                deleteHitCount++;
            }
        }
        int projectedLive = store.size() - deleteHitCount + newKeyCount;

        // ------------------------------------------------ 3. 容量预演（只预演，不动内存）
        List<String> evictionPlan = new ArrayList<>();
        if (properties.getMaxEntries() > 0) {
            if (putKeys.size() > properties.getMaxEntries()) {
                // 整组自身最终占用就超过上限：即使清空缓存也放不下，直接拒绝。
                groupCommitCount++;
                groupRejectedCapacityCount++;
                String msg = "整组超过容量上限: groupId=" + gid + " 整组最终键数=" + putKeys.size()
                        + " > maxEntries=" + properties.getMaxEntries()
                        + "（整组拒绝，未淘汰任何键）";
                log.info("整组容量判定 groupId={} 依据=GROUP_TOO_LARGE 结果=拒绝 {}", gid, msg);
                throw new GroupCapacityException(gid, "GROUP_TOO_LARGE", msg);
            }
            int need = projectedLive - properties.getMaxEntries();
            if (need > 0) {
                // 严格复用现有 LRU 顺序：从旧到新挑选"不会被本组删除/重写"的存活键。
                for (String candidate : tracker.peekOldestFirst()) {
                    if (evictionPlan.size() >= need) {
                        break;
                    }
                    if (deleteKeys.contains(candidate) || putKeys.contains(candidate)) {
                        continue;
                    }
                    evictionPlan.add(candidate);
                }
                if (evictionPlan.size() < need) {
                    // 腾不出足够空间：整组拒绝。此前只做了只读预演，没有任何键被真正淘汰。
                    groupCommitCount++;
                    groupRejectedCapacityCount++;
                    String msg = "整组容量不足且无法腾出足够空间: groupId=" + gid
                            + " 需要淘汰=" + need + " 可按LRU淘汰=" + evictionPlan.size()
                            + "（整组拒绝，未淘汰任何键）";
                    log.info("整组容量判定 groupId={} 依据=GROUP_INSUFFICIENT_SPACE 结果=拒绝 {}", gid, msg);
                    throw new GroupCapacityException(gid, "GROUP_INSUFFICIENT_SPACE", msg);
                }
            }
        }

        // ------------------------------------------------ 4. 先落盘整组（GBEGIN/成员/EVICT/GCOMMIT）
        // 记录提交点字节偏移：中途任何失败都把 WAL 截回此处，磁盘上绝不残留半成品组。
        long commitPoint = wal.sizeBytes();
        long beginSeq = nextSeq;
        long seqCursor = nextSeq;
        try {
            int framedRecords = evictionPlan.size() + members.size();
            wal.append(OperationRecord.frame(seqCursor, OperationType.GBEGIN,
                    gid, framedRecords, now));
            seqCursor++;
            // 淘汰记录先于成员操作，且同属一个原子组：淘汰结论随整组一起生效或一起消失。
            for (String evictedKey : evictionPlan) {
                wal.append(new OperationRecord(seqCursor, OperationType.EVICT,
                        evictedKey, null, 0L, now, gid));
                seqCursor++;
            }
            for (GroupOperation m : members) {
                if (m.op() == OperationType.PUT) {
                    wal.append(new OperationRecord(seqCursor, OperationType.PUT,
                            m.key(), m.value(), m.ttlMillis(), now, gid));
                } else {
                    wal.append(new OperationRecord(seqCursor, OperationType.DELETE,
                            m.key(), null, 0L, now, gid));
                }
                seqCursor++;
            }
            long commitSeq = seqCursor;
            wal.append(OperationRecord.frame(seqCursor, OperationType.GCOMMIT, gid, 0L, now));
            seqCursor++;
            nextSeq = seqCursor;

            // ------------------------------------------------ 5. GCOMMIT 落盘成功后才变更内存
            for (String evictedKey : evictionPlan) {
                CacheEntry removed = store.remove(evictedKey);
                tracker.remove(evictedKey);
                if (removed != null) {
                    evictionCount++;
                }
            }
            for (GroupOperation m : members) {
                if (m.op() == OperationType.PUT) {
                    store.put(m.key(), new CacheEntry(m.value(), now + m.ttlMillis()));
                    tracker.touch(m.key());
                    totalPutCount++;
                } else {
                    store.remove(m.key());
                    tracker.remove(m.key());
                    totalDeleteCount++;
                }
            }
            groupCommitCount++;
            groupSuccessCount++;
            committedGroups.put(gid, new CommittedGroup(List.copyOf(members), beginSeq, commitSeq,
                    List.copyOf(evictionPlan)));
            int live = liveCountLocked();
            log.info("整组提交成功 groupId={} 成员={} keys={} 淘汰={} beginSeq={} commitSeq={} live={}",
                    gid, members.size(), memberKeys(members), evictionPlan, beginSeq, commitSeq, live);
            return new GroupCommitResult(gid, GroupCommitResult.Outcome.COMMITTED, members.size(),
                    List.copyOf(evictionPlan), beginSeq, commitSeq, live);
        } catch (RuntimeException e) {
            // WAL 中途失败：内存尚未改动；把文件截回本组开始前的提交点并归还序号。
            nextSeq = beginSeq;
            try {
                wal.truncateTo(commitPoint);
            } catch (RuntimeException truncateFailure) {
                // 即使在线截断失败，重启时 readAll 也会把未闭合组整体截除；这里保留原始异常链路。
                e.addSuppressed(truncateFailure);
            }
            log.error("整组持久化失败 groupId={} 已回滚WAL至提交点={} 内存无改动", gid, commitPoint, e);
            throw e;
        }
    }

    private static List<String> memberKeys(List<GroupOperation> members) {
        List<String> keys = new ArrayList<>(members.size());
        for (GroupOperation m : members) {
            keys.add(m.op().name() + ":" + m.key());
        }
        return keys;
    }

    private int liveCountLocked() {
        long now = System.currentTimeMillis();
        int live = 0;
        for (CacheEntry e : store.values()) {
            if (now < e.expireAtMs()) {
                live++;
            }
        }
        return live;
    }

    private void validateGroupRequest(List<GroupOperation> members) {
        if (members == null || members.isEmpty()) {
            throw new InvalidRequestException("整组提交的成员列表不得为空");
        }
        if (members.size() > 10_000) {
            throw new InvalidRequestException("整组成员数超过上限 10000: " + members.size());
        }
        for (GroupOperation m : members) {
            if (m == null) {
                throw new InvalidRequestException("整组成员不得为 null");
            }
        }
    }

    private void validateGroupId(String groupId) {
        if (groupId == null || groupId.isEmpty()) {
            throw new InvalidRequestException("groupId 不得为空");
        }
        if (groupId.length() > properties.getMaxKeyLength()) {
            throw new InvalidRequestException(
                    "groupId 超长: " + groupId.length() + " > " + properties.getMaxKeyLength());
        }
    }

    /** 已确认整组的持久化元数据（重试去重与结果可观测）。 */
    private record CommittedGroup(List<GroupOperation> spec, long beginSeq, long commitSeq,
                                  List<String> evictedKeys) {
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
                ready ? "READY" : "RECOVERING", groupCommitCount, groupSuccessCount,
                groupDuplicateCount, groupRejectedStaleCount, groupRejectedCapacityCount);
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
        RecoverySummary summary = applyReplayRecord(r, now);
        advanceSeq(r.seq());
        return summary;
    }

    /**
     * 整组重放：仅在恢复收到匹配的 GBEGIN/GCOMMIT 后调用一次。
     * 成员按日志顺序应用到内存（不写 WAL、不改容量），整组要么随 GCOMMIT 一起出现，
     * 要么被 WAL 读取层整体截除——恢复路径上不存在"恢复一半"的组。
     *
     * @param memberSeqs 与 members 一一对应的成员序号，用于推进 nextSeq
     */
    public synchronized List<RecoverySummary> replayGroup(String groupId, List<OperationRecord> members,
                                                          List<Long> memberSeqs, long beginSeq,
                                                          long commitSeq, long now) {
        if (ready) {
            throw new StateConflictException("缓存已就绪，禁止重复恢复以避免数据叠加");
        }
        if (committedGroups.containsKey(groupId)) {
            // WAL 中出现第二个已闭合的同 groupId 组：日志级幂等键冲突，恢复必须失败，拒绝带半套数据服务。
            throw new RecoveryException("WAL 中整组 groupId=" + groupId + " 重复出现，恢复中止以避免结果叠加");
        }
        List<RecoverySummary> summaries = new ArrayList<>(members.size());
        for (OperationRecord r : members) {
            summaries.add(applyReplayRecord(r, now));
        }
        advanceSeq(beginSeq);
        for (Long s : memberSeqs) {
            advanceSeq(s);
        }
        advanceSeq(commitSeq);
        // 去重比较只面向调用方提交的 PUT/DELETE 成员；帧内 EVICT 是容量副产物，不进入规格。
        // DELETE 保留 ttlMillis=0L，与在线提交时 GroupOperation.delete(...) 构造的规格完全一致。
        List<GroupOperation> spec = members.stream()
                .filter(r -> r.type() == OperationType.PUT || r.type() == OperationType.DELETE)
                .map(r -> new GroupOperation(r.type(), r.key(),
                        r.type() == OperationType.PUT ? r.value() : null,
                        r.type() == OperationType.PUT ? r.ttlMillis() : 0L))
                .toList();
        List<String> evicted = members.stream()
                .filter(r -> r.type() == OperationType.EVICT)
                .map(OperationRecord::key)
                .toList();
        committedGroups.put(groupId, new CommittedGroup(spec, beginSeq, commitSeq, evicted));
        groupCommitCount++;
        groupSuccessCount++;
        return summaries;
    }

    /** GBEGIN/GCOMMIT 帧记录只推进序号，不改变内存。 */
    public synchronized void replayFrame(OperationRecord r) {
        if (ready) {
            throw new StateConflictException("缓存已就绪，禁止重复恢复以避免数据叠加");
        }
        advanceSeq(r.seq());
    }

    private void advanceSeq(long seq) {
        if (seq + 1 > nextSeq) {
            nextSeq = seq + 1;
        }
    }

    /** 恢复全序中单条记录的内存应用（PUT/DELETE/EVICT），不触碰 WAL 与序号。 */
    private RecoverySummary applyReplayRecord(OperationRecord r, long now) {
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
                return overwrite ? RecoverySummary.putOverwrite() : RecoverySummary.putNew();
            }
            case DELETE -> {
                boolean had = store.remove(r.key()) != null;
                tracker.remove(r.key());
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
