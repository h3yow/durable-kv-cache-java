package com.github.h3yow.durablekvcachejava.core;

import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.CapacityExceededException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.InvalidRequestException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.PersistenceException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.StateConflictException;
import com.github.h3yow.durablekvcachejava.cache.GroupAction;
import com.github.h3yow.durablekvcachejava.cache.GroupIdConflictException;
import com.github.h3yow.durablekvcachejava.cache.GroupMember;
import com.github.h3yow.durablekvcachejava.cache.GroupResult;
import com.github.h3yow.durablekvcachejava.cache.GroupStaleException;
import com.github.h3yow.durablekvcachejava.cache.KeyValueCache;
import com.github.h3yow.durablekvcachejava.config.CacheProperties;
import com.github.h3yow.durablekvcachejava.eviction.EvictionTracker;
import com.github.h3yow.durablekvcachejava.persistence.PersistenceLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    /** groupId -> 已确认提交的整组结论（用于重复提交幂等与冲突识别，恢复时从 WAL 重建）。 */
    private final Map<String, StoredGroup> committedGroups = new HashMap<>();
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
        // 序号的严格连续性由 RecoveryService 的帧感知重放负责校验（未闭合整组帧会先被截除）。
        // 构造时只取最大序号 + 1 作为初始基线，随后由恢复流程按需重置，
        // 不能在此用严格递增校验（崩溃截断后历史最大序号可能大于重放后的序号）。
        this.nextSeq = walMaxSeq(wal);
        log.info("缓存状态机初始化: nextSeq={}, maxEntries={}", nextSeq, properties.getMaxEntries());
    }

    private static long walMaxSeq(PersistenceLog wal) {
        long max = 0L;
        for (var r : wal.readAll()) {
            max = Math.max(max, r.seq());
        }
        return max + 1;
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
    public synchronized GroupResult commitGroup(String groupIdRaw, List<GroupMember> membersRaw) {
        ensureReady();
        String groupId = validateGroupRequest(groupIdRaw, membersRaw);
        List<GroupMember> members = List.copyOf(membersRaw);
        String signature = groupSignature(members);

        // 1) 幂等/冲突：相同 groupId + 相同成员签名 -> 幂等返回；签名不同 -> 明确拒绝。
        //    该判定先于任何状态检查，保证重复提交绝不叠加/覆盖。
        StoredGroup existing = committedGroups.get(groupId);
        if (existing != null) {
            if (!existing.signature().equals(signature)) {
                throw new GroupIdConflictException(groupId,
                        "groupId=" + groupId + " 已成功提交过内容不同的整组操作，拒绝串组覆盖");
            }
            log.info("整组重复提交 group={} keys={} 依据=groupId与成员签名均一致 结果=幂等返回, 零副作用",
                    groupId, memberKeys(members));
            return existing.toDuplicateResult();
        }

        long now = System.currentTimeMillis();
        // 2) 与单键 PUT/DELETE 完全一致：先按统一规则清理已过期键（过期键自然释放占用）。
        purgeExpired(now);

        // 3) 前置条件（读判定，零副作用）：INSERT 要求键不存在；UPDATE/DELETE 要求键存活。
        List<String> staleKeys = new ArrayList<>();
        List<String> staleReasons = new ArrayList<>();
        for (GroupMember m : members) {
            CacheEntry e = store.get(m.key());
            if (m.action() == GroupAction.INSERT) {
                if (e != null) {
                    staleKeys.add(m.key());
                    staleReasons.add(m.key() + "(INSERT目标已存在)");
                }
            } else if (e == null) {
                staleKeys.add(m.key());
                staleReasons.add(m.key() + "(" + (m.action() == GroupAction.UPDATE ? "UPDATE" : "DELETE")
                        + "目标已过期或已删除)");
            }
        }
        if (!staleKeys.isEmpty()) {
            log.info("整组前置失败 group={} staleKeys={} 依据=提交时键状态不满足动作要求 结果=整组拒绝, 无WAL写入",
                    groupId, staleReasons);
            throw new GroupStaleException(groupId, staleKeys,
                    "整组前置条件不满足，整组拒绝: " + String.join(", ", staleReasons));
        }

        // 4) 容量规划（只计算不落盘、不淘汰）：
        //    本组最终必须存活的写入键 puts = INSERT+UPDATE；最终存活数 = live - 删除数 + 新增数。
        int max = properties.getMaxEntries();
        int deletes = 0;
        int inserts = 0;
        Set<String> memberKeys = new LinkedHashSet<>();
        for (GroupMember m : members) {
            memberKeys.add(m.key());
            if (m.action() == GroupAction.DELETE) {
                deletes++;
            } else if (m.action() == GroupAction.INSERT) {
                inserts++;
            }
        }
        int puts = members.size() - deletes;
        int liveAfterGroup = store.size() - deletes + inserts;
        List<String> toEvict = new ArrayList<>();
        if (max > 0) {
            // 即使把其它所有非成员键全部淘汰，本组最终存活键也只有组内写入键 puts 个，
            // 因此 puts > max 在数学上等价于"腾出全部位置也放不下"——下面分两种叙述拒绝：
            // 没有任何可腾键（组自身即超限）与淘汰全部后仍差若干个位置。
            if (puts > max) {
                int nonMemberLive = store.size() - (members.size() - inserts); // 现存且非组成员的键数
                int shortage = puts - max;
                String reason = nonMemberLive == 0
                        ? "整组写入键数 " + puts + " 超过容量上限 " + max
                                + "，且没有任何其它键可供淘汰（整组自身即超限，至少还差 " + shortage + " 个位置）"
                        : "整组提交后即使按 LRU 淘汰全部 " + nonMemberLive
                                + " 个非成员键，仍差 " + shortage + " 个位置（容量上限 " + max + "）";
                log.info("整组容量失败 group={} 依据=组内写入{}键, 非成员存活{}键, 仍差{}个 结果=整组拒绝, 无副作用",
                        groupId, puts, nonMemberLive, shortage);
                throw new CapacityExceededException("GROUP_TOO_LARGE", reason + "，整组拒绝");
            }
            int need = liveAfterGroup - max;
            if (need > 0) {
                // 候选项 = 当前存活且不属于本组的键（组内键无论写入/删除都不允许被淘汰），
                // 严格按现有 LRU 从旧到新取 need 个；只规划，先不动内存。
                for (String k : tracker.snapshotOldestFirst()) {
                    if (toEvict.size() >= need) {
                        break;
                    }
                    if (memberKeys.contains(k)) {
                        continue;
                    }
                    CacheEntry e = store.get(k);
                    if (e != null && now < e.expireAtMs()) {
                        toEvict.add(k);
                    }
                }
                if (toEvict.size() < need) {
                    // 防御性分支：正常校验下 puts<=max 时该条件不成立；
                    // 一旦出现（如未来引入受保护键），仍保证"不允许先淘汰一部分再失败"。
                    log.info("整组容量失败 group={} 依据=需腾{}个位置但确定可淘汰非成员键仅{}个 结果=整组拒绝, 无副作用",
                            groupId, need, toEvict.size());
                    throw new CapacityExceededException("GROUP_TOO_LARGE",
                            "整组提交后需 " + need + " 个空位，但按 LRU 仅能确定淘汰 "
                                    + toEvict.size() + " 个非成员键，整组拒绝");
                }
            }
        }

        // 5) 持久化阶段：TXBEG,(EVICT*,成员*),TXEND 连续帧，每条 force；
        //    记录文件起始长度，任一条失败就把本帧全部截断，序号回退，内存零改动。
        long walLenBefore = wal.size();
        long beginSeq = nextSeq;
        try {
            wal.append(new OperationRecord(nextSeq, OperationType.TXBEG, groupId,
                    actionManifest(members), members.size(), now, groupId));
            nextSeq++;
            for (String k : toEvict) {
                wal.append(new OperationRecord(nextSeq, OperationType.EVICT, k, null, 0L, now, groupId));
                nextSeq++;
            }
            for (GroupMember m : members) {
                OperationType t = m.action() == GroupAction.DELETE ? OperationType.DELETE : OperationType.PUT;
                wal.append(new OperationRecord(nextSeq, t, m.key(),
                        m.action() == GroupAction.DELETE ? null : m.value(),
                        m.action() == GroupAction.DELETE ? 0L : m.ttlMillis(), now, groupId));
                nextSeq++;
            }
            long endSeq = nextSeq;
            wal.append(new OperationRecord(nextSeq, OperationType.TXEND, groupId, null,
                    toEvict.size(), now, groupId));
            nextSeq++;
            // 6) 仅当整帧全部 force 成功后才提交内存：外部任何入口都不可能读到半成品。
            applyGroupToMemory(members, toEvict, now);
            GroupResult result = new GroupResult(groupId, true, false,
                    classifyKeys(members, GroupAction.INSERT),
                    classifyKeys(members, GroupAction.UPDATE),
                    classifyKeys(members, GroupAction.DELETE),
                    List.copyOf(toEvict), liveCount(), beginSeq, endSeq);
            committedGroups.put(groupId, new StoredGroup(signature, result));
            log.info("整组提交成功 group={} keys={} evicted={} seq={}..{} live={} 判定=整帧force成功后原子生效",
                    groupId, memberKeys, toEvict, beginSeq, endSeq, result.liveEntries());
            return result;
        } catch (RuntimeException e) {
            nextSeq = beginSeq; // 内存尚未改动，仅回退序号并截除本帧，保证"失败不留任何副作用"。
            try {
                wal.truncateTo(walLenBefore);
            } catch (RuntimeException truncErr) {
                log.error("整组失败后WAL截断也失败 group={} 截断至={}（重启恢复将丢弃未闭合帧）: {}",
                        groupId, walLenBefore, truncErr.getMessage());
            }
            log.error("整组提交失败 group={} 依据=WAL追加异常 结果=整组拒绝, 内存与WAL均回滚: {}",
                    groupId, e.getMessage());
            if (e instanceof PersistenceException) {
                throw e;
            }
            throw new PersistenceException("整组提交持久化失败 group=" + groupId, e);
        }
    }

    @Override
    public synchronized GroupResult groupStatus(String groupId) {
        ensureReady();
        validateGroupId(groupId);
        StoredGroup g = committedGroups.get(groupId);
        return g == null ? null : g.toResult();
    }

    @Override
    public synchronized Map<String, String> snapshot() {
        ensureReady();
        long now = System.currentTimeMillis();
        Map<String, String> snap = new HashMap<>();
        for (Map.Entry<String, CacheEntry> en : store.entrySet()) {
            if (now < en.getValue().expireAtMs()) {
                snap.put(en.getKey(), en.getValue().value());
            }
        }
        return snap;
    }

    /** 整帧落盘成功后：先淘汰、再按请求顺序应用成员，与单键规则复用同一套计数与 LRU 语义。 */
    private void applyGroupToMemory(List<GroupMember> members, List<String> toEvict, long now) {
        for (String k : toEvict) {
            store.remove(k);
            tracker.remove(k);
            evictionCount++;
        }
        for (GroupMember m : members) {
            if (m.action() == GroupAction.DELETE) {
                if (store.remove(m.key()) != null) {
                    tracker.remove(m.key());
                    totalDeleteCount++;
                }
            } else {
                store.put(m.key(), new CacheEntry(m.value(), now + m.ttlMillis()));
                tracker.touch(m.key());
                totalPutCount++;
            }
        }
    }

    private static List<String> classifyKeys(List<GroupMember> members, GroupAction action) {
        List<String> keys = new ArrayList<>();
        for (GroupMember m : members) {
            if (m.action() == action) {
                keys.add(m.key());
            }
        }
        return List.copyOf(keys);
    }

    private static List<String> memberKeys(List<GroupMember> members) {
        List<String> keys = new ArrayList<>();
        for (GroupMember m : members) {
            keys.add(m.key());
        }
        return List.copyOf(keys);
    }

    /** 成员签名：动作+键+TTL+值全部一致才视为同一提交（用于幂等与串组冲突识别）。 */
    public static String groupSignature(List<GroupMember> members) {
        StringBuilder sb = new StringBuilder();
        for (GroupMember m : members) {
            sb.append(m.action().name()).append(';')
                    .append(m.key().length()).append(':').append(m.key()).append(';')
                    .append(m.action() == GroupAction.DELETE ? "-" : m.ttlMillis()).append(';');
            String v = m.action() == GroupAction.DELETE ? "" : m.value();
            sb.append(v == null ? 0 : v.getBytes(StandardCharsets.UTF_8).length).append(':').append(v).append('\n');
        }
        return sb.toString();
    }

    /**
     * 组成员动作清单（持久化在 TXBEG 中）：恢复时无需依赖"键当时是否存在"推断动作，
     * 即使该组所有键恢复时都已过期，也能准确重建 INSERT/UPDATE/DELETE 签名。
     * 每个成员一段，段内以 {@code :} 分隔：{@code 动作首字母:键}:
     * （I/U/D 分别代表 INSERT/UPDATE/DELETE）。
     */
    static String actionManifest(List<GroupMember> members) {
        StringBuilder sb = new StringBuilder();
        for (GroupMember m : members) {
            sb.append(switch (m.action()) {
                case INSERT -> 'I';
                case UPDATE -> 'U';
                case DELETE -> 'D';
            }).append(':').append(m.key()).append('\n');
        }
        return sb.toString();
    }

    /** 解析动作清单为与成员顺序对应的动作列表。 */
    public static List<GroupAction> parseActionManifest(String manifest, int expected) {
        List<GroupAction> actions = new ArrayList<>();
        if (manifest != null) {
            for (String line : manifest.split("\n", -1)) {
                if (line.isEmpty()) {
                    continue;
                }
                actions.add(switch (line.charAt(0)) {
                    case 'I' -> GroupAction.INSERT;
                    case 'U' -> GroupAction.UPDATE;
                    case 'D' -> GroupAction.DELETE;
                    default -> throw new IllegalStateException("未知动作标记: " + line.charAt(0));
                });
            }
        }
        if (actions.size() != expected) {
            throw new com.github.h3yow.durablekvcachejava.cache.CacheExceptions.RecoveryException(
                    "TXBEG 动作清单数量(" + actions.size() + ")与成员数(" + expected + ")不一致");
        }
        return actions;
    }

    private String validateGroupRequest(String groupId, List<GroupMember> members) {
        validateGroupId(groupId);
        if (members == null || members.isEmpty()) {
            throw new InvalidRequestException("整组提交的 members 不得为空");
        }
        Set<String> seen = new LinkedHashSet<>();
        for (GroupMember m : members) {
            if (m == null || m.action() == null) {
                throw new InvalidRequestException("整组成员及其 action 不得为 null");
            }
            validateKey(m.key());
            if (!seen.add(m.key())) {
                throw new InvalidRequestException("整组提交内键重复出现: " + m.key());
            }
            if (m.action() == GroupAction.DELETE) {
                continue;
            }
            if (m.value() == null) {
                throw new InvalidRequestException("组成员 value 不得为 null（key=" + m.key() + "）");
            }
            int bytes = m.value().getBytes(StandardCharsets.UTF_8).length;
            if (bytes > properties.getMaxValueBytes()) {
                throw new InvalidRequestException(
                        "组成员 value 超长: " + bytes + " > maxValueBytes=" + properties.getMaxValueBytes()
                                + "（key=" + m.key() + "）");
            }
            if (m.ttlMillis() == null || m.ttlMillis() <= 0) {
                throw new InvalidRequestException(
                        "组成员 ttlMillis 必须为正数（key=" + m.key() + "），实际: " + m.ttlMillis());
            }
        }
        return groupId;
    }

    private void validateGroupId(String groupId) {
        if (groupId == null || groupId.isEmpty()) {
            throw new InvalidRequestException("groupId 不得为空");
        }
        if (groupId.length() > properties.getMaxKeyLength()) {
            throw new InvalidRequestException(
                    "groupId 超长: " + groupId.length() + " > maxKeyLength=" + properties.getMaxKeyLength());
        }
    }

    private int liveCount() {
        long now = System.currentTimeMillis();
        int live = 0;
        for (CacheEntry e : store.values()) {
            if (now < e.expireAtMs()) {
                live++;
            }
        }
        return live;
    }

    /** 恢复期登记一个已确认整组提交（由恢复服务按帧重放后调用）。 */
    public synchronized void registerRecoveredGroup(StoredGroupRecord rec) {
        if (ready) {
            throw new StateConflictException("缓存已就绪，禁止补登记恢复组");
        }
        if (committedGroups.containsKey(rec.groupId())) {
            throw new StateConflictException("WAL 中存在重复 groupId: " + rec.groupId());
        }
        GroupResult result = new GroupResult(rec.groupId(), true, false,
                List.copyOf(rec.inserted()), List.copyOf(rec.updated()),
                List.copyOf(rec.deleted()), List.copyOf(rec.evicted()),
                liveCount(), rec.beginSeq(), rec.endSeq());
        committedGroups.put(rec.groupId(), new StoredGroup(rec.signature(), result));
        log.info("恢复登记整组 group={} keys={} evicted={} seq={}..{}",
                rec.groupId(), result.insertedKeys(), rec.evicted(), rec.beginSeq(), rec.endSeq());
    }

    /** 恢复服务向状态机登记已确认整组提交所需的信息。 */
    public record StoredGroupRecord(String groupId, String signature,
                                    List<String> inserted, List<String> updated, List<String> deleted,
                                    List<String> evicted, long beginSeq, long endSeq) {
    }

    /** 已确认提交组的内部存档（签名 + 结论）。 */
    private record StoredGroup(String signature, GroupResult result) {
        GroupResult toResult() {
            return result;
        }

        GroupResult toDuplicateResult() {
            return new GroupResult(result.groupId(), true, true,
                    result.insertedKeys(), result.updatedKeys(), result.deletedKeys(),
                    result.evictedKeys(), result.liveEntries(), result.beginSeq(), result.endSeq());
        }
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

    /** 恢复重放前设置序号基线（未闭合整组帧被截除后，跳过其序号、永不复用）。 */
    public synchronized void resetNextSeqForRecovery(long next) {
        if (ready) {
            throw new StateConflictException("缓存已就绪，禁止重置恢复序号");
        }
        nextSeq = next;
    }

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
            case TXBEG, TXEND -> {
                return RecoverySummary.txMarker();
            }
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
        public enum Kind { PUT_NEW, PUT_OVERWRITE, DELETE_HIT, DELETE_MISS, EXPIRED_SKIP, EVICT_HIT, TX_MARKER }

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

        static RecoverySummary txMarker() {
            return new RecoverySummary(Kind.TX_MARKER);
        }
    }
}
