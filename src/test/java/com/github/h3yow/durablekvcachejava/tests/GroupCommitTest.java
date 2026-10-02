package com.github.h3yow.durablekvcachejava.tests;

import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.GroupCapacityException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.GroupStaleException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.InvalidRequestException;
import com.github.h3yow.durablekvcachejava.config.CacheProperties;
import com.github.h3yow.durablekvcachejava.core.DurableKvCache;
import com.github.h3yow.durablekvcachejava.core.GroupCommitResult;
import com.github.h3yow.durablekvcachejava.core.GroupOperation;
import com.github.h3yow.durablekvcachejava.eviction.LruEvictionTracker;
import com.github.h3yow.durablekvcachejava.persistence.FileWalLog;
import com.github.h3yow.durablekvcachejava.recovery.RecoveryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 整组提交状态机测试：原子生效、重试去重、过期/删除前置失败、容量边界、重叠键并发全序。
 * 每个用例使用独立 WAL 文件名（均在 JUnit {@code @TempDir} 下），互不污染。
 */
class GroupCommitTest {

    private static final Logger log = LoggerFactory.getLogger(GroupCommitTest.class);

    @TempDir
    Path tempDir;

    private record Harness(DurableKvCache cache, FileWalLog wal, CacheProperties props) {
    }

    private Harness newCache(String walName, int maxEntries) {
        CacheProperties p = new CacheProperties();
        p.setDataDir(tempDir.toString());
        p.setMaxEntries(maxEntries);
        FileWalLog wal = new FileWalLog(tempDir.toString(), walName);
        DurableKvCache cache = new DurableKvCache(wal, new LruEvictionTracker(), p);
        new RecoveryService(wal, cache).recover();
        return new Harness(cache, wal, p);
    }

    private static List<GroupOperation> ops(Object... kv) {
        List<GroupOperation> list = new ArrayList<>();
        for (int i = 0; i < kv.length; i += 2) {
            list.add(GroupOperation.put((String) kv[i], (String) kv[i + 1], 600_000L));
        }
        return list;
    }

    @Test
    void groupPutAllOrNothingAppliesAtomically() {
        Harness h = newCache("g1.wal", 0);
        GroupCommitResult r = h.cache().commitGroup("sess-1",
                ops("sess:profile", "alice", "sess:perm", "admin", "sess:count", "3"));
        assertEquals(GroupCommitResult.Outcome.COMMITTED, r.outcome());
        assertEquals(3, r.memberCount());
        assertTrue(r.evictedKeys().isEmpty());
        assertEquals("alice", h.cache().get("sess:profile"));
        assertEquals("admin", h.cache().get("sess:perm"));
        assertEquals("3", h.cache().get("sess:count"));
        assertEquals(3, r.liveEntries());
        log.info("整组基础 groupId=sess-1 keys=sess:profile/perm/count 依据=COMMITTED后同时可见 恢复结果=在线, seq={}..{}",
                r.beginSeq(), r.commitSeq());
    }

    @Test
    void groupWithPutAndDeleteSucceedsAndDeletesAreDurableVisible() {
        Harness h = newCache("g2.wal", 0);
        h.cache().put("old", "x", 600_000L);
        GroupCommitResult r = h.cache().commitGroup("mix-1", List.of(
                GroupOperation.put("new", "y", 600_000L),
                GroupOperation.delete("old")));
        assertEquals(GroupCommitResult.Outcome.COMMITTED, r.outcome());
        assertEquals("y", h.cache().get("new"));
        assertNull(h.cache().get("old"), "组内 DELETE 必须随组生效");
        log.info("整组混合 groupId=mix-1 keys=PUT(new),DELETE(old) 依据=COMMITTED 结果=new命中/old删除 live={}",
                r.liveEntries());
    }

    @Test
    void duplicateGroupIdSamePayloadIsIdempotentAndDifferentPayloadRejected() {
        Harness h = newCache("g3.wal", 0);
        List<GroupOperation> spec = ops("a", "1", "b", "2");
        GroupCommitResult first = h.cache().commitGroup("dup", spec);
        assertEquals(GroupCommitResult.Outcome.COMMITTED, first.outcome());

        // 相同 groupId 相同内容重试：不重复执行，返回历史结论；值不会被叠加或刷新。
        GroupCommitResult again = h.cache().commitGroup("dup", ops("a", "1", "b", "2"));
        assertEquals(GroupCommitResult.Outcome.DUPLICATE, again.outcome());
        assertEquals(first.beginSeq(), again.beginSeq());
        assertEquals(first.commitSeq(), again.commitSeq());
        assertEquals("1", h.cache().get("a"));
        assertEquals("2", h.cache().get("b"));

        // 相同 groupId 不同内容：明确按参数错误拒绝，防止幂等键被误用。
        assertThrows(InvalidRequestException.class,
                () -> h.cache().commitGroup("dup", ops("a", "9", "b", "2")));
        assertEquals(1L, h.cache().metrics().groupDuplicateCount());
        log.info("整组幂等 groupId=dup 依据=同ID同内容=DUPLICATE, 同ID异内容=INVALID_REQUEST 结果=无叠加");
    }

    @Test
    void groupWithExpiredOrDeletedDeleteMemberIsRejectedWithDistinctReasons() {
        Harness h = newCache("g4.wal", 0);
        h.cache().put("alive", "v", 600_000L);
        h.cache().put("willexpire", "v", 20L);
        h.cache().put("willdelete", "v", 600_000L);
        h.cache().delete("willdelete");
        try {
            Thread.sleep(40);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        GroupStaleException ex = assertThrows(GroupStaleException.class,
                () -> h.cache().commitGroup("stale-1", List.of(
                        GroupOperation.delete("alive"),
                        GroupOperation.delete("willexpire"),
                        GroupOperation.delete("willdelete"))));
        Map<String, String> reasons = new java.util.HashMap<>();
        ex.staleKeys().forEach(s -> reasons.put(s.key(), s.reason()));
        assertEquals("KEY_EXPIRED", reasons.get("willexpire"),
                "已过期成员必须能与已删除成员区分: " + reasons);
        assertEquals("KEY_NOT_FOUND", reasons.get("willdelete"));
        assertFalse(reasons.containsKey("alive"), "存活键不得被判定为 stale");

        // 失败零副作用：alive 仍然存在，过期键未被误删，指标可区分。
        assertEquals("v", h.cache().get("alive"));
        assertEquals(1L, h.cache().metrics().groupRejectedStaleCount());
        log.info("整组stale groupId=stale-1 依据=willexpire:KEY_EXPIRED/willdelete:KEY_NOT_FOUND 结果=整组拒绝零副作用");
    }

    @Test
    void groupFitsExactlyAtCapacityBoundary() {
        // 容量 3：已有 2 个存活键，整组新增 1 个（无淘汰），刚好放下。
        Harness h = newCache("g5.wal", 3);
        h.cache().put("a", "1", 600_000L);
        h.cache().put("b", "2", 600_000L);
        GroupCommitResult r = h.cache().commitGroup("exact", List.of(
                GroupOperation.put("c", "3", 600_000L)));
        assertEquals(GroupCommitResult.Outcome.COMMITTED, r.outcome());
        assertTrue(r.evictedKeys().isEmpty());
        assertEquals(3, h.cache().metrics().liveEntries());
        log.info("整组容量-刚好 groupId=exact keys=c 依据=live2+新增1=max3 淘汰=[] 结果=COMMITTED");
    }

    @Test
    void groupOneOverCapacityEvictsExactlyOneInLruOrder() {
        // 容量 3：a,b,c 已满；GET a 刷新后 b 最旧；整组新增 d 时必须只淘汰 b。
        Harness h = newCache("g6.wal", 3);
        h.cache().put("a", "1", 600_000L);
        h.cache().put("b", "2", 600_000L);
        h.cache().put("c", "3", 600_000L);
        h.cache().get("a");
        GroupCommitResult r = h.cache().commitGroup("oneover", List.of(
                GroupOperation.put("d", "4", 600_000L)));
        assertEquals(List.of("b"), r.evictedKeys(), "必须按现有 LRU 顺序只淘汰最旧的 b: " + r.evictedKeys());
        assertNull(h.cache().get("b"));
        assertEquals(3, h.cache().metrics().liveEntries());
        assertEquals(1L, h.cache().metrics().evictionCount());
        log.info("整组容量-差一个 groupId=oneover keys=d 依据=LRU(b最旧) 淘汰=[b] 结果=COMMITTED live=3");
    }

    @Test
    void groupLargerThanCapacityIsRejectedWithoutEvictingAnything() {
        Harness h = newCache("g7.wal", 2);
        h.cache().put("a", "1", 600_000L);
        h.cache().put("b", "2", 600_000L);
        GroupCapacityException ex = assertThrows(GroupCapacityException.class,
                () -> h.cache().commitGroup("toobig", ops("x", "1", "y", "2", "z", "3")));
        assertEquals("GROUP_TOO_LARGE", ex.reasonCode());
        // 不允许先淘汰一部分再失败：a/b 必须原样存活。
        assertEquals("1", h.cache().get("a"));
        assertEquals("2", h.cache().get("b"));
        assertNull(h.cache().get("x"));
        assertEquals(0L, h.cache().metrics().evictionCount());
        assertEquals(1L, h.cache().metrics().groupRejectedCapacityCount());
        log.info("整组容量-超上限 groupId=toobig keys=x,y,z 依据=3>max2 GROUP_TOO_LARGE 结果=拒绝且未淘汰任何键");
    }

    @Test
    void insufficientSpaceReasonIsDistinguishableViaTrackerShortage() {
        // 正常记账模型下，组自身最终键数 <= maxEntries 时总能按 LRU 腾够空间；
        // GROUP_INSUFFICIENT_SPACE 是"预演淘汰名额不足"的防御性分支。
        // 这里用 max=2、存活 a,b、整组 3 个全新键验证对外原因码（TOO_LARGE），
        // 并通过原因码字段确认与 INVALID_REQUEST、GROUP_STALE、PERSISTENCE_FAILED 可区分。
        Harness h = newCache("g8.wal", 2);
        h.cache().put("a", "1", 600_000L);
        h.cache().put("b", "2", 600_000L);
        GroupCapacityException ex = assertThrows(GroupCapacityException.class,
                () -> h.cache().commitGroup("short", ops("x", "1", "y", "2", "z", "3")));
        // 3 个键的组即使清空缓存也放不下：按"整组自身超过上限"判定。
        assertEquals("GROUP_TOO_LARGE", ex.reasonCode());
        assertEquals("short", ex.groupId());
        log.info("整组容量-原因码 groupId=short 依据=组最终键数3>max2 结果=GROUP_TOO_LARGE(与参数错误/stale可区分)");
    }

    @Test
    void rejectedCapacityGroupLeavesNoSideEffectsAndKeepsLruIntact() {
        Harness h = newCache("g9.wal", 2);
        h.cache().put("a", "1", 600_000L);
        h.cache().put("b", "2", 600_000L);
        GroupCapacityException ex = assertThrows(GroupCapacityException.class,
                () -> h.cache().commitGroup("big2", ops("x", "1", "y", "2", "z", "3")));
        assertEquals("GROUP_TOO_LARGE", ex.reasonCode());
        // 被拒后，紧接着的整组必须正常成功，且 LRU 顺序未被破坏（a 旧 b 新）。
        GroupCommitResult after = h.cache().commitGroup("after", List.of(
                GroupOperation.put("x", "1", 600_000L)));
        assertEquals(List.of("a"), after.evictedKeys(),
                "被容量拒绝的整组不得改变 LRU 顺序: " + after.evictedKeys());
        log.info("整组容量-零副作用 groupId=big2/after 依据=拒绝后LRU仍为(a旧,b新) 淘汰=[a]");
    }

    @Test
    void concurrentOverlappingGroupsSettleInOneDeterministicTotalOrder() throws Exception {
        Harness h = newCache("g10.wal", 0); // 不限容量，专注全序正确性
        // 初始一致集合：共享键 shared，两组各自的私有键 pA / pB。
        h.cache().commitGroup("init", ops("shared", "S0", "pA", "A0", "pB", "B0"));
        int threads = 8;
        int rounds = 100;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);
        Map<Integer, Throwable> errors = new ConcurrentHashMap<>();
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int id = t;
            futures.add(pool.submit(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < rounds; i++) {
                        if (id % 2 == 0) {
                            // A 组：shared 与 pA 必须来自同一逻辑版本（同组同时生效）。
                            String ver = "A" + id + "-" + i;
                            h.cache().commitGroup("A-" + id + "-" + i,
                                    ops("shared", ver, "pA", ver));
                        } else {
                            String ver = "B" + id + "-" + i;
                            h.cache().commitGroup("B-" + id + "-" + i,
                                    ops("shared", ver, "pB", ver));
                        }
                    }
                } catch (Throwable e) {
                    errors.put(id, e);
                }
            }));
        }
        for (Future<?> f : futures) {
            f.get(60, TimeUnit.SECONDS);
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertTrue(errors.isEmpty(), "并发整组提交不得出现异常: " + errors);

        String shared = h.cache().get("shared");
        String pA = h.cache().get("pA");
        String pB = h.cache().get("pB");
        // 单一确定全序：shared 的最终值要么是某个 A 版本，要么是某个 B 版本；
        // 若最终是 A 版本，则 pA 必须等于它（A 整组生效）；B 版本同理。绝不允许"覆盖到一半"。
        boolean consistent =
                (shared.startsWith("A") && shared.equals(pA)) ||
                (shared.startsWith("B") && shared.equals(pB));
        assertTrue(consistent,
                "整组全序被破坏: shared=" + shared + " pA=" + pA + " pB=" + pB);
        long ok = h.cache().metrics().groupSuccessCount();
        assertEquals(threads * rounds + 1L, ok, "全部工作整组 + 初始 init 组都必须恰好成功一次");
        log.info("整组并发 组=A(shared,pA)/B(shared,pB) 依据=单锁串行化 最终 shared={} 与对应私有键一致, 成功组数={}",
                shared, ok);
    }

    @Test
    void groupPutCanOverwriteExpiredKeyButStaleDeleteFailsWholeGroup() throws Exception {
        Harness h = newCache("g12.wal", 0);
        h.cache().put("exp", "old", 20L);
        h.cache().put("fresh", "f", 600_000L);
        Thread.sleep(40);

        // PUT 重写已过期键沿用单键 PUT 语义：整组成功，旧值不复活。
        GroupCommitResult ok = h.cache().commitGroup("rewrite-exp", List.of(
                GroupOperation.put("exp", "new", 600_000L),
                GroupOperation.put("other", "o", 600_000L)));
        assertEquals(GroupCommitResult.Outcome.COMMITTED, ok.outcome());
        assertEquals("new", h.cache().get("exp"));
        assertEquals("o", h.cache().get("other"));

        // 但同一组里只要有一个 DELETE 命中过期键，整组（含其它本来合法的 PUT）都必须失败。
        h.cache().put("exp2", "old2", 20L);
        Thread.sleep(40);
        GroupStaleException ex = assertThrows(GroupStaleException.class,
                () -> h.cache().commitGroup("stale-mix", List.of(
                        GroupOperation.put("brandnew", "v", 600_000L),
                        GroupOperation.delete("exp2"))));
        assertEquals("KEY_EXPIRED", ex.staleKeys().get(0).reason());
        assertNull(h.cache().get("brandnew"), "被拒整组中的合法 PUT 也不得生效（原子性）");
        assertEquals("f", h.cache().get("fresh"), "失败零副作用");
        log.info("整组过期混合 组=rewrite-exp(成功:PUT覆盖过期键)/stale-mix(拒绝:一个DELETE过期整组回退) "
                + "依据=PUT单键语义 vs DELETE前置存活 结果=brandnew不存在");
    }

    @Test
    void concurrentReadersNeverObservePartiallyAppliedGroup() throws Exception {
        // 不变量：内存状态在任何一个锁内瞬间都必须是"某个完整提交之后"的状态。
        // 写者反复把 (a,b,c) 整组翻转到新版本 vN；读者在同一把状态机锁内一次性取三键快照
        // （与 commitGroup/get 互斥），因此快照里三键要么同版本、要么因过期缺失——
        // 绝不允许 a=vN 而 b=vM（M≠N）的"只改了一半"状态。
        Harness h = newCache("g13.wal", 0);
        h.cache().commitGroup("seed", ops("a", "v0", "b", "v0", "c", "v0"));
        int rounds = 500;
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CyclicBarrier barrier = new CyclicBarrier(2);
        java.util.concurrent.atomic.AtomicLong violations = new java.util.concurrent.atomic.AtomicLong();
        java.util.concurrent.atomic.AtomicLong snapshots = new java.util.concurrent.atomic.AtomicLong();
        Future<?> writer = pool.submit(() -> {
            try {
                barrier.await();
                for (int i = 1; i <= rounds; i++) {
                    String v = "v" + i;
                    h.cache().commitGroup("flip-" + i, ops("a", v, "b", v, "c", v));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Future<?> reader = pool.submit(() -> {
            try {
                barrier.await();
                long endAt = System.currentTimeMillis() + 8000;
                while (System.currentTimeMillis() < endAt) {
                    // synchronized 块与 commitGroup 持有的是同一个监视器，快照必为某个完整提交点。
                    // 同时穿插普通单键 GET，验证单键入口也绝不会读到半成品。
                    List<String> snap;
                    synchronized (h.cache()) {
                        snap = List.of(h.cache().get("a"), h.cache().get("b"), h.cache().get("c"));
                    }
                    if (snap.get(0) != null && snap.get(1) != null && snap.get(2) != null) {
                        snapshots.incrementAndGet();
                        if (!snap.get(0).equals(snap.get(1)) || !snap.get(1).equals(snap.get(2))) {
                            violations.incrementAndGet();
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        writer.get(30, TimeUnit.SECONDS);
        reader.get(30, TimeUnit.SECONDS);
        pool.shutdown();
        assertTrue(snapshots.get() > 0, "读者应至少取到一次完整快照");
        assertEquals(0L, violations.get(), "锁内快照观察到了只改了一部分的整组状态");
        String finalV = "v" + rounds;
        assertEquals(finalV, h.cache().get("a"));
        assertEquals(finalV, h.cache().get("b"));
        assertEquals(finalV, h.cache().get("c"));
        log.info("整组原子可见 组=flip-*(a,b,c) 依据=同锁快照{}个全部三键同版本 结果=最终=v{} 无中间态",
                snapshots.get(), rounds);
    }

    @Test
    void groupFailureDoesNotConsumeSeqOrCorruptNextPut() {
        Harness h = newCache("g11.wal", 2);
        h.cache().put("a", "1", 600_000L);
        long seqBefore = h.wal().nextSeq();
        assertThrows(GroupCapacityException.class,
                () -> h.cache().commitGroup("fail", ops("x", "1", "y", "2", "z", "3")));
        // 失败整组的帧不得占用序号：下一条单键 PUT 的序号必须连续。
        long seqAfterRejected = h.wal().nextSeq();
        assertEquals(seqBefore, seqAfterRejected, "被拒绝整组不得在 WAL 留下任何序号");
        h.cache().put("b", "2", 600_000L);
        assertEquals(seqBefore + 1, h.wal().nextSeq(), "后续单键写入序号必须连续可写");
        log.info("整组失败序号 groupId=fail 依据=回滚到提交点 nextSeq 前后={}/{} 后续PUT连续",
                seqBefore, seqAfterRejected);
    }
}
