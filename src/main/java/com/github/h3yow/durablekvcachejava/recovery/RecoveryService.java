package com.github.h3yow.durablekvcachejava.recovery;

import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.RecoveryException;
import com.github.h3yow.durablekvcachejava.core.DurableKvCache;
import com.github.h3yow.durablekvcachejava.core.OperationRecord;
import com.github.h3yow.durablekvcachejava.core.OperationType;
import com.github.h3yow.durablekvcachejava.persistence.PersistenceLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 启动恢复：按 WAL 单一全序重放。
 *
 * <p>保证：
 * <ul>
 *   <li>幂等：重放是纯应用（不回写 WAL），同一日志在空缓存上重放结果唯一；
 *       重复提交同一批记录不会叠加计数（以 key 为单位覆盖/删除）。</li>
 *   <li>恢复完成并 {@code markReady} 之前，业务入口全部抛 StateConflictException，
 *       不暴露半初始化数据。</li>
 *   <li>任一行损坏/序号异常抛 {@link RecoveryException}，调用方（Bean 装配/启动器）
 *       中止启动，绝不静默继续。</li>
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
        long records = 0;
        long putsNew = 0, putsOverwrite = 0;
        long deleteHit = 0, deleteMiss = 0, expiredSkip = 0, evictApplied = 0;
        long groupsApplied = 0;
        long maxSeq = 0L;
        long expectedSeq = 1L;

        // 整组缓冲：readAll 已保证返回的组一定带匹配的 GCOMMIT；GBEGIN/GCOMMIT 也各占一个序号。
        String openGroupId = null;
        long openBeginSeq = -1L;
        List<OperationRecord> groupMembers = null;
        List<Long> groupMemberSeqs = null;

        try {
            for (OperationRecord r : wal.readAll()) {
                if (r.seq() != expectedSeq) {
                    throw new RecoveryException(
                            "WAL 序号不连续: 期望 " + expectedSeq + " 实际 " + r.seq());
                }
                expectedSeq++;
                maxSeq = r.seq();

                if (r.type() == OperationType.GBEGIN) {
                    if (openGroupId != null) {
                        throw new RecoveryException("WAL 整组嵌套: groupId=" + openGroupId);
                    }
                    openGroupId = r.key();
                    openBeginSeq = r.seq();
                    groupMembers = new ArrayList<>();
                    groupMemberSeqs = new ArrayList<>();
                    cache.replayFrame(r);
                    log.info("恢复重放 seq={} 操作=GBEGIN groupId={} 成员数={}",
                            r.seq(), r.key(), r.ttlMillis());
                    records++;
                    continue;
                }
                if (r.type() == OperationType.GCOMMIT) {
                    if (openGroupId == null || !openGroupId.equals(r.key())) {
                        throw new RecoveryException(
                                "WAL GCOMMIT 无匹配 GBEGIN: groupId=" + r.key());
                    }
                    cache.replayFrame(r);
                    var summaries = cache.replayGroup(openGroupId, groupMembers, groupMemberSeqs,
                            openBeginSeq, r.seq(), now);
                    for (var s : summaries) {
                        switch (s.kind()) {
                            case PUT_NEW -> putsNew++;
                            case PUT_OVERWRITE -> putsOverwrite++;
                            case DELETE_HIT -> deleteHit++;
                            case DELETE_MISS -> deleteMiss++;
                            case EXPIRED_SKIP -> expiredSkip++;
                            case EVICT_HIT -> evictApplied++;
                        }
                    }
                    groupsApplied++;
                    log.info("恢复重放 seq={} 操作=GCOMMIT groupId={} 判定=整组{}条整体生效",
                            r.seq(), openGroupId, groupMembers.size());
                    openGroupId = null;
                    openBeginSeq = -1L;
                    groupMembers = null;
                    groupMemberSeqs = null;
                    records++;
                    continue;
                }

                if (openGroupId != null) {
                    groupMembers.add(r);
                    groupMemberSeqs.add(r.seq());
                    records++;
                    continue;
                }

                var summary = cache.replayFrom(r, now);
                records++;
                switch (summary.kind()) {
                    case PUT_NEW -> putsNew++;
                    case PUT_OVERWRITE -> putsOverwrite++;
                    case DELETE_HIT -> deleteHit++;
                    case DELETE_MISS -> deleteMiss++;
                    case EXPIRED_SKIP -> expiredSkip++;
                    case EVICT_HIT -> evictApplied++;
                }
                log.info("恢复重放 seq={} key={} 操作={} 判定={}",
                        r.seq(), r.key(), r.type(), summary.kind());
            }
            if (openGroupId != null) {
                // readAll 理应已截除未闭合组；走到这里说明帧结构处理有缺陷，明确失败而非半套服务。
                throw new RecoveryException("WAL 末尾整组缺少 GCOMMIT: groupId=" + openGroupId);
            }
        } catch (RecoveryException e) {
            log.error("恢复失败(可区分错误，启动中止): {}", e.getMessage());
            throw e;
        }

        cache.markReady();
        RecoveryResult result = new RecoveryResult(records, putsNew + putsOverwrite,
                deleteHit + deleteMiss, expiredSkip, evictApplied, groupsApplied, maxSeq,
                cache.synchronizedLiveEntries());
        log.info("恢复完成 结果={} 耗时Ms={}", result, System.currentTimeMillis() - t0);
        return result;
    }

    /** 恢复结果。 */
    public record RecoveryResult(long recordsReplayed, long putsApplied, long deletesApplied,
                                 long expiredSkipped, long evictionsApplied, long groupsApplied,
                                 long finalSeq, int liveEntries) {
    }
}
