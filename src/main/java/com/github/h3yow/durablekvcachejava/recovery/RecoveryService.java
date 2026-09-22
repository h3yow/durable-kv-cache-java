package com.github.h3yow.durablekvcachejava.recovery;

import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.RecoveryException;
import com.github.h3yow.durablekvcachejava.core.DurableKvCache;
import com.github.h3yow.durablekvcachejava.core.OperationRecord;
import com.github.h3yow.durablekvcachejava.persistence.PersistenceLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        long maxSeq = 0L;
        long expectedSeq = 1L;

        try {
            for (OperationRecord r : wal.readAll()) {
                if (r.seq() != expectedSeq) {
                    throw new RecoveryException(
                            "WAL 序号不连续: 期望 " + expectedSeq + " 实际 " + r.seq());
                }
                expectedSeq++;
                maxSeq = r.seq();
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
        } catch (RecoveryException e) {
            log.error("恢复失败(可区分错误，启动中止): {}", e.getMessage());
            throw e;
        }

        cache.markReady();
        RecoveryResult result = new RecoveryResult(records, putsNew + putsOverwrite,
                deleteHit + deleteMiss, expiredSkip, evictApplied, maxSeq,
                cache.synchronizedLiveEntries());
        log.info("恢复完成 结果={} 耗时Ms={}", result, System.currentTimeMillis() - t0);
        return result;
    }

    /** 恢复结果。 */
    public record RecoveryResult(long recordsReplayed, long putsApplied, long deletesApplied,
                                 long expiredSkipped, long evictionsApplied, long finalSeq,
                                 int liveEntries) {
    }
}
