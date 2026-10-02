package com.github.h3yow.durablekvcachejava.tests;

import com.github.h3yow.durablekvcachejava.cache.GroupMember;
import com.github.h3yow.durablekvcachejava.config.CacheProperties;
import com.github.h3yow.durablekvcachejava.core.DurableKvCache;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多组并发、键互相重叠时：不存在半组可见态，最终结果是单一确定顺序（某个组的整组结果）。
 */
class GroupConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(GroupConcurrencyTest.class);

    @TempDir
    Path tempDir;

    private DurableKvCache boot(int maxEntries) {
        CacheProperties p = new CacheProperties();
        p.setDataDir(tempDir.toString());
        p.setMaxEntries(maxEntries);
        FileWalLog wal = new FileWalLog(tempDir.toString(), "gconc.wal");
        DurableKvCache cache = new DurableKvCache(wal, new LruEvictionTracker(), p);
        new RecoveryService(wal, cache).recover();
        return cache;
    }

    /** 构造一个整组：组 A={a,b,c} 与组 B={c,d,e} 在 c 上重叠。 */
    private static List<GroupMember> group(char who, int round) {
        String tag = who + "-" + round;
        String[] keys = who == 'A' ? new String[]{"a", "b", "c"} : new String[]{"c", "d", "e"};
        List<GroupMember> ms = new ArrayList<>();
        for (String k : keys) {
            ms.add(GroupMember.update(k, tag, 600_000L));
        }
        return ms;
    }

    @Test
    void overlappingGroupsNeverExposeHalfGroupAndSettleToOneOrder() throws Exception {
        DurableKvCache c = boot(0);
        // 先让 5 个键都存在（整组 UPDATE 要求键存活）。
        c.commitGroup("seed", List.of(
                GroupMember.insert("a", "seed", 600_000L),
                GroupMember.insert("b", "seed", 600_000L),
                GroupMember.insert("c", "seed", 600_000L),
                GroupMember.insert("d", "seed", 600_000L),
                GroupMember.insert("e", "seed", 600_000L)));

        int writersPerSide = 6; // A/B 各 6 个写入线程
        int readers = 4;
        int threads = writersPerSide * 2 + readers;
        int rounds = 30;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);
        AtomicInteger groupSeq = new AtomicInteger();
        Map<Integer, Throwable> errors = new ConcurrentHashMap<>();
        AtomicInteger tornObservations = new AtomicInteger();
        List<String> tornSamples = new ArrayList<>();
        AtomicInteger okSnapshots = new AtomicInteger();

        for (int t = 0; t < writersPerSide * 2; t++) {
            final char who = t < writersPerSide ? 'A' : 'B';
            final int id = t;
            pool.submit(() -> {
                try {
                    barrier.await();
                    for (int r = 0; r < rounds; r++) {
                        int n = groupSeq.incrementAndGet();
                        // 每轮每组一个唯一 groupId，避免 409 串组冲突；并发顺序由状态机单锁决定。
                        c.commitGroup("grp-" + who + "-" + id + "-" + n, group(who, n));
                    }
                } catch (Throwable e) {
                    errors.put(id, e);
                }
            });
        }
        for (int t = 0; t < readers; t++) {
            final int id = 100 + t;
            pool.submit(() -> {
                try {
                    barrier.await();
                    for (int r = 0; r < rounds * 4; r++) {
                        Map<String, String> s = c.snapshot(); // 一致性快照
                        String va = s.get("a");
                        String vb = s.get("b");
                        String vc = s.get("c");
                        String vd = s.get("d");
                        String ve = s.get("e");
                        // 半组判定：一个组"自己的私有键"标签必须与它对共享键 c 的写入标签一致。
                        // 若 A 已提交则 a,b 标签为 X 时 c 要么也是 X（A 是 c 的最后写入者），
                        // 要么 c 是某个 B 标签（B 在 A 之后整体覆盖了 c）——
                        // 此时 a,b 仍是 X，说明 A 组自身完整，c 的变化是 B 整组生效造成的。
                        // 真正的撕裂只可能是：a/b 已经是新 A 标签而 c 还停留在更早的 A 标签
                        // （A 组只写了一半），或 d/e 与 c 出现类似错配。
                        boolean aPrivateIntact = va.equals(vb);
                        boolean bPrivateIntact = vd.equals(ve);
                        boolean aTagIsA = va.startsWith("A-");
                        boolean bTagIsB = vd.startsWith("B-");
                        // A 组完整条件：a,b 同标签，且该标签一旦出现，c 不能是"比它更旧的 A-*"
                        boolean aTorn = !aPrivateIntact
                                || (aTagIsA && vc.startsWith("A-")
                                        && !va.equals(vc)
                                        && groupNumber(vc) < groupNumber(va));
                        boolean bTorn = !bPrivateIntact
                                || (bTagIsB && vc.startsWith("B-")
                                        && !vd.equals(vc)
                                        && groupNumber(vc) < groupNumber(vd));
                        if (aTorn || bTorn) {
                            tornObservations.incrementAndGet();
                            synchronized (tornSamples) {
                                tornSamples.add("a=" + va + ",b=" + vb + ",c=" + vc
                                        + ",d=" + vd + ",e=" + ve);
                            }
                        } else {
                            okSnapshots.incrementAndGet();
                        }
                    }
                } catch (Throwable e) {
                    errors.put(id, e);
                }
            });
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "并发线程必须全部结束");
        assertTrue(errors.isEmpty(), "并发过程不得出现异常: " + errors);
        assertEquals(0, tornObservations.get(),
                "发现半组可见态: " + tornSamples.stream().limit(5).toList());

        // 最终状态必须是单一确定顺序：
        // a,b 同属最后一个生效的 A 组；d,e 同属最后一个生效的 B 组；
        // 共享键 c 属于二者中最后提交的那个组（c 的标签前缀即决定全序赢家）。
        Map<String, String> fin = c.snapshot();
        assertTrue(fin.get("a").startsWith("A-") && fin.get("a").equals(fin.get("b")),
                "A 组私有键必须完整属于同一个 A 组: " + fin);
        assertTrue(fin.get("d").startsWith("B-") && fin.get("d").equals(fin.get("e")),
                "B 组私有键必须完整属于同一个 B 组: " + fin);
        assertTrue(fin.get("c").startsWith("A-") || fin.get("c").startsWith("B-"),
                "共享键 c 必须属于某个完整生效的组: " + fin);
        String lastWriter = fin.get("c").startsWith("A-") ? "A" : "B";
        log.info("并发重叠判定 groups=A{{a,b,c}}/B{{c,d,e}}重叠键=c 依据=单锁全序+整帧后原子提交 "
                + "恢复结果={}个写入者x{}轮全部完成, 合格快照={} 半组观测=0 全序赢家={} A标签={} B标签={} c={}",
                writersPerSide * 2, rounds, okSnapshots.get(), lastWriter,
                fin.get("a"), fin.get("d"), fin.get("c"));
    }

    private static long groupNumber(String tag) {
        // tag 形如 "A-27" / "B-42" / "seed"
        int dash = tag.indexOf('-');
        if (dash < 0) {
            return -1L;
        }
        return Long.parseLong(tag.substring(dash + 1));
    }
}
