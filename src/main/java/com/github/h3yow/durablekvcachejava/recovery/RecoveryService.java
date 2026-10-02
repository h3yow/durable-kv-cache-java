package com.github.h3yow.durablekvcachejava.recovery;

import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.RecoveryException;
import com.github.h3yow.durablekvcachejava.cache.GroupAction;
import com.github.h3yow.durablekvcachejava.cache.GroupMember;
import com.github.h3yow.durablekvcachejava.core.DurableKvCache;
import com.github.h3yow.durablekvcachejava.core.OperationRecord;
import com.github.h3yow.durablekvcachejava.core.OperationType;
import com.github.h3yow.durablekvcachejava.persistence.PersistenceLog;
import com.github.h3yow.durablekvcachejava.persistence.WalEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 启动恢复：按 WAL 单一全序重放（帧感知，两遍扫描）。
 *
 * <p>保证：
 * <ul>
 *   <li>幂等：重放是纯应用（不回写 WAL），同一日志在空缓存上重放结果唯一；
 *       重复提交同一批记录不会叠加计数（以 key 为单位覆盖/删除）。</li>
 *   <li>整组原子：{@code TXBEG..TXEND} 帧要么整组应用，要么整组不应用。
 *       第一遍扫描只做结构校验与帧定位：帧末尾缺失 TXEND（提交中崩溃/kill -9）时，
 *       判定为未确认提交，从 TXBEG 偏移处整段截除 WAL 并从待重放列表剔除——
 *       崩溃后<b>第一次</b>重启即恢复到该组之前的完整状态，绝不出现半组。</li>
 *   <li>已闭合帧内任何结构异常（groupId 不一致、计数/动作清单不符、DELETE 未命中等）
 *       一律按 WAL 损坏中止启动，拒绝对外服务。</li>
 *   <li>已确认整组的 groupId 与成员签名在恢复后重新登记：相同 groupId+成员的
 *       重复提交仍然幂等，不会叠加或弄丢。</li>
 *   <li>重放时以日志时间戳判定 PUT 是否在当前时刻之前已过期，过期条目不进入存活集。</li>
 * </ul>
 */
public final class RecoveryService {

    private static final Logger log = LoggerFactory.getLogger(RecoveryService.class);

    private final PersistenceLog wal;
    private final DurableKvCache cache;

    public RecoveryService(PersistenceLog wal, DurableKvCache cache) {
        this.wal = wal;
        this.cache = cache;
    }

    public RecoveryResult recover() {
        long t0 = System.currentTimeMillis();
        long now = System.currentTimeMillis();

        List<WalEntry> all = new ArrayList<>();
        wal.readAllWithOffsets().forEach(all::add);

        // 未闭合帧尾部记录的序号属于崩溃现场，不做整体严格连续校验；
        // 连续性在截除后的 accepted 列表上校验（见下方），帧内一致性由帧遍历保证。

        // ------------------------------------------------ 第一遍：帧结构校验与未闭合帧截除
        List<WalEntry> accepted = new ArrayList<>(all.size());
        long groupsRecovered = 0;
        long discardedGroups = 0;

        int i = 0;
        long prevSeq = 0L;
        while (i < all.size()) {
            WalEntry e = all.get(i);
            OperationRecord r = e.record();
            // 序号必须单调递增；未闭合整组帧被截除后，新提交会跳过被丢弃的序号区间，
            // 因此历史上允许存在"永久空洞"，但绝不允许倒退或重复。
            if (r.seq() <= prevSeq) {
                throw new RecoveryException("WAL 序号非法(倒退/重复): 上一序号 " + prevSeq
                        + " 当前 " + r.seq());
            }
            long frameExpected = r.seq(); // 进入帧后要求帧内严格连续
            if (r.type() != OperationType.TXBEG) {
                if (r.type() == OperationType.TXEND) {
                    throw new RecoveryException(
                            "WAL seq=" + r.seq() + " 出现游离的 TXEND 记录");
                }
                accepted.add(e);
                i++;
                prevSeq = r.seq();
                continue;
            }
            // 找到一个 TXBEG：向后收集到匹配 TXEND。
            String gid = requireValidBeg(r);
            int memberCount = (int) r.ttlMillis();
            List<GroupAction> actions =
                    DurableKvCache.parseActionManifest(r.value(), memberCount);
            long frameStart = e.startOffset();
            frameExpected++; // TXBEG 自身
            int p = i + 1;
            final List<WalEntry> frameBody = new ArrayList<>();
            boolean closed = false;
            while (p < all.size()) {
                WalEntry be = all.get(p);
                OperationRecord br = be.record();
                if (br.seq() != frameExpected) {
                    throw new RecoveryException("WAL 组帧内序号不连续: 期望 " + frameExpected
                            + " 实际 " + br.seq() + " group=" + gid);
                }
                if (!gid.equals(br.groupId())) {
                    throw new RecoveryException("WAL seq=" + br.seq()
                            + " 组帧内 groupId 不一致: 期望 " + gid + " 实际 " + br.groupId());
                }
                if (br.type() == OperationType.TXBEG) {
                    throw new RecoveryException(
                            "WAL seq=" + br.seq() + " 组帧内出现嵌套 TXBEG group=" + gid);
                }
                if (br.type() == OperationType.TXEND) {
                    closed = true;
                    break;
                }
                frameBody.add(be);
                p++;
                frameExpected++;
            }
            if (!closed) {
                // 提交中崩溃：该组从未确认。截除整帧（含 TXBEG），本次重启直接恢复到提交前。
                wal.truncateTo(frameStart);
                discardedGroups++;
                log.error("恢复丢弃未闭合整组 group={} 判定=TXEND前崩溃 处理=从偏移{}整段截除, "
                        + "内存不重放该帧, 恢复到该组之前的完整状态", gid, frameStart);
                break; // 未闭合帧必在文件尾部（后续无更多记录）
            }
            prevSeq = frameExpected; // 含 TXEND
            // 帧体只允许 memberCount 个成员 + 若干 EVICT；EVICT 数与 TXEND 记录一致。
            WalEntry endEntry = all.get(p);
            validateClosedFrame(gid, memberCount, actions, frameBody, endEntry.record());
            groupsRecovered++;
            accepted.add(e);
            accepted.addAll(frameBody);
            accepted.add(endEntry);
            log.info("恢复识别闭合整组 group={} seq={}..{} 成员数={} 淘汰数={}",
                    gid, r.seq(), endEntry.record().seq(), memberCount, endEntry.record().ttlMillis());
            i = p + 1;
        }

        // accepted 序号必须单调递增（帧内严格连续；截除未闭合帧后允许永久空洞）。
        long maxAcceptedSeq = 0L;
        long lastAccepted = 0L;
        for (WalEntry e : accepted) {
            if (e.record().seq() <= lastAccepted) {
                throw new RecoveryException("已确认 WAL 记录序号非法(倒退/重复): 上一 "
                        + lastAccepted + " 当前 " + e.record().seq());
            }
            lastAccepted = e.record().seq();
            maxAcceptedSeq = Math.max(maxAcceptedSeq, e.record().seq());
        }
        long nextAfterTruncation = all.isEmpty() ? 1L : all.get(all.size() - 1).record().seq() + 1;
        cache.resetNextSeqForRecovery(Math.max(maxAcceptedSeq + 1, nextAfterTruncation));

        // ------------------------------------------------ 第二遍：全序重放
        long records = 0;
        long putsNew = 0, putsOverwrite = 0;
        long deleteHit = 0, deleteMiss = 0, expiredSkip = 0, evictApplied = 0;
        long maxSeq = maxAcceptedSeq;

        String gid = null;
        int memberIndex = 0;
        List<GroupAction> actions = null;
        final List<GroupMember> rebuiltMembers = new ArrayList<>();
        final List<String> inserted = new ArrayList<>();
        final List<String> updated = new ArrayList<>();
        final List<String> deleted = new ArrayList<>();
        final List<String> evicted = new ArrayList<>();
        long frameBeginSeq = -1L;

        for (WalEntry e : accepted) {
            OperationRecord r = e.record();
            if (r.type() == OperationType.TXBEG) {
                gid = r.groupId();
                memberIndex = 0;
                actions = DurableKvCache.parseActionManifest(r.value(), (int) r.ttlMillis());
                frameBeginSeq = r.seq();
                rebuiltMembers.clear();
                inserted.clear();
                updated.clear();
                deleted.clear();
                evicted.clear();
                continue;
            }
            if (r.type() == OperationType.TXEND) {
                cache.registerRecoveredGroup(new DurableKvCache.StoredGroupRecord(
                        gid, DurableKvCache.groupSignature(rebuiltMembers),
                        inserted, updated, deleted, evicted, frameBeginSeq, r.seq()));
                log.info("恢复重放 seq={} 组帧闭合 group={} 判定=整组原子恢复 keys={}/插入{}更新{}删除{} 淘汰{}",
                        r.seq(), gid, memberIndex, inserted.size(), updated.size(),
                        deleted.size(), evicted.size());
                gid = null;
                actions = null;
                continue;
            }

            var summary = cache.replayFrom(r, now);
            records++;
            if (gid == null) {
                switch (summary.kind()) {
                    case PUT_NEW -> putsNew++;
                    case PUT_OVERWRITE -> putsOverwrite++;
                    case DELETE_HIT -> deleteHit++;
                    case DELETE_MISS -> deleteMiss++;
                    case EXPIRED_SKIP -> expiredSkip++;
                    case EVICT_HIT -> evictApplied++;
                    case TX_MARKER -> throw new RecoveryException(
                            "WAL seq=" + r.seq() + " 出现游离的组标记记录");
                }
                log.info("恢复重放 seq={} key={} 操作={} 判定={}",
                        r.seq(), r.key(), r.type(), summary.kind());
            } else if (r.type() == OperationType.EVICT) {
                if (summary.kind() != DurableKvCache.RecoverySummary.Kind.EVICT_HIT
                        && summary.kind() != DurableKvCache.RecoverySummary.Kind.DELETE_MISS) {
                    throw new RecoveryException(
                            "组帧内 EVICT 重放分类非法 seq=" + r.seq());
                }
                evicted.add(r.key());
                evictApplied++;
                log.info("恢复重放 seq={} group={} key={} 操作=EVICT 判定={}",
                        r.seq(), gid, r.key(), summary.kind());
            } else {
                GroupAction action = actions.get(memberIndex++);
                GroupMember gm;
                switch (action) {
                    case INSERT -> {
                        if (summary.kind() != DurableKvCache.RecoverySummary.Kind.PUT_NEW
                                && summary.kind() != DurableKvCache.RecoverySummary.Kind.EXPIRED_SKIP) {
                            throw new RecoveryException("组帧 INSERT 成员重放矛盾 group=" + gid
                                    + " seq=" + r.seq() + " 判定=" + summary.kind());
                        }
                        gm = new GroupMember(r.key(), GroupAction.INSERT, r.value(), r.ttlMillis());
                        inserted.add(r.key());
                        putsNew++;
                        if (summary.kind() == DurableKvCache.RecoverySummary.Kind.EXPIRED_SKIP) {
                            expiredSkip++;
                            putsNew--;
                        }
                    }
                    case UPDATE -> {
                        if (summary.kind() != DurableKvCache.RecoverySummary.Kind.PUT_OVERWRITE
                                && summary.kind() != DurableKvCache.RecoverySummary.Kind.EXPIRED_SKIP) {
                            throw new RecoveryException("组帧 UPDATE 成员重放矛盾 group=" + gid
                                    + " seq=" + r.seq() + " 判定=" + summary.kind());
                        }
                        gm = new GroupMember(r.key(), GroupAction.UPDATE, r.value(), r.ttlMillis());
                        updated.add(r.key());
                        putsOverwrite++;
                        if (summary.kind() == DurableKvCache.RecoverySummary.Kind.EXPIRED_SKIP) {
                            expiredSkip++;
                            putsOverwrite--;
                        }
                    }
                    case DELETE -> {
                        if (summary.kind() != DurableKvCache.RecoverySummary.Kind.DELETE_HIT) {
                            throw new RecoveryException("组帧 DELETE 成员恢复时未命中存活键 group="
                                    + gid + " key=" + r.key()
                                    + "（与提交时前置条件矛盾，WAL 结论不可信）");
                        }
                        gm = GroupMember.delete(r.key());
                        deleted.add(r.key());
                        deleteHit++;
                    }
                    default -> throw new RecoveryException("未知组动作: " + action);
                }
                rebuiltMembers.add(gm);
                log.info("恢复重放 seq={} group={} key={} 操作={} 判定={}",
                        r.seq(), gid, r.key(), action, summary.kind());
            }
        }

        cache.markReady();
        RecoveryResult result = new RecoveryResult(records, putsNew + putsOverwrite,
                deleteHit + deleteMiss, expiredSkip, evictApplied, maxSeq,
                cache.synchronizedLiveEntries(), groupsRecovered, discardedGroups);
        log.info("恢复完成 结果={} 耗时Ms={}", result, System.currentTimeMillis() - t0);
        return result;
    }

    private static String requireValidBeg(OperationRecord r) {
        if (r.groupId() == null || !r.groupId().equals(r.key())) {
            throw new RecoveryException("WAL seq=" + r.seq() + " TXBEG 组标识非法");
        }
        if (r.ttlMillis() <= 0) {
            throw new RecoveryException(
                    "WAL seq=" + r.seq() + " TXBEG 成员数非法: " + r.ttlMillis());
        }
        return r.groupId();
    }

    private static void validateClosedFrame(String gid, int memberCount, List<GroupAction> actions,
                                            List<WalEntry> body, OperationRecord end) {
        int members = 0;
        int evicts = 0;
        for (WalEntry be : body) {
            OperationType t = be.record().type();
            if (t == OperationType.PUT || t == OperationType.DELETE) {
                members++;
            } else if (t == OperationType.EVICT) {
                evicts++;
            } else {
                throw new RecoveryException(
                        "组帧内出现非法记录类型 group=" + gid + " seq=" + be.record().seq());
            }
        }
        if (members != memberCount) {
            throw new RecoveryException("WAL group=" + gid + " 成员数不符: 清单声明 "
                    + memberCount + " 帧内实际 " + members);
        }
        if (actions.size() != memberCount) {
            throw new RecoveryException("WAL group=" + gid + " 动作清单数量与成员数不符");
        }
        if (end.ttlMillis() != evicts) {
            throw new RecoveryException("WAL group=" + gid + " TXEND 淘汰数不符: 记录 "
                    + end.ttlMillis() + " 帧内实际 " + evicts);
        }
    }

    /** 恢复结果。 */
    public record RecoveryResult(long recordsReplayed, long putsApplied, long deletesApplied,
                                 long expiredSkipped, long evictionsApplied, long finalSeq,
                                 int liveEntries, long groupsRecovered, long discardedGroups) {
    }
}
