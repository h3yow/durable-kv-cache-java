package com.github.h3yow.durablekvcachejava.tests;

import com.github.h3yow.durablekvcachejava.cache.GroupMember;
import com.github.h3yow.durablekvcachejava.cache.GroupResult;
import com.github.h3yow.durablekvcachejava.config.CacheProperties;
import com.github.h3yow.durablekvcachejava.core.DurableKvCache;
import com.github.h3yow.durablekvcachejava.core.OperationRecord;
import com.github.h3yow.durablekvcachejava.core.OperationType;
import com.github.h3yow.durablekvcachejava.eviction.LruEvictionTracker;
import com.github.h3yow.durablekvcachejava.persistence.FileWalLog;
import com.github.h3yow.durablekvcachejava.persistence.PersistenceLog;
import com.github.h3yow.durablekvcachejava.persistence.WalEntry;
import com.github.h3yow.durablekvcachejava.recovery.RecoveryService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证「同一套用例在同一工作区连续多次执行、执行顺序变化」结论稳定：
 * <ul>
 *   <li>每个场景使用彼此独立的存储位置（{@link IsolatedTestDir}），
 *       每轮执行前清除上一轮残留，本轮第一次提交永远判定为全新提交；</li>
 *   <li>上一轮被强杀留下的不完整残留（未闭合组帧 + 半行撕裂）不影响下一轮；</li>
 *   <li>场景执行顺序变化不改变任何场景的结论。</li>
 * </ul>
 */
class RepeatableRunIsolationTest {

    private static final Logger log = LoggerFactory.getLogger(RepeatableRunIsolationTest.class);

    private DurableKvCache boot(Path dir, int maxEntries) {
        return boot(dir, maxEntries, UnaryOperator.identity());
    }

    private DurableKvCache boot(Path dir, int maxEntries, UnaryOperator<PersistenceLog> wrap) {
        CacheProperties p = new CacheProperties();
        p.setDataDir(dir.toString());
        p.setMaxEntries(maxEntries);
        PersistenceLog wal = wrap.apply(new FileWalLog(dir.toString(), "cache.wal"));
        DurableKvCache cache = new DurableKvCache(wal, new LruEvictionTracker(), p);
        new RecoveryService(wal, cache).recover();
        return cache;
    }

    // —— 三个彼此独立的场景（各自使用专属目录名，互不共享存储） ——

    /** 场景 A：整组提交。第一轮必须 OK（非重复），同组重放必须 OK_DUPLICATE。 */
    private String groupScenario(Path dir, String tag) {
        DurableKvCache c = boot(dir, 0);
        List<GroupMember> members = List.of(
                GroupMember.insert("http:profile", "alice", 600_000L),
                GroupMember.insert("http:perms", "admin", 600_000L));
        GroupResult first = c.commitGroup("http-grp-1", members);
        assertTrue(first.committed(), tag + ": 本轮第一次提交必须正常生效");
        assertFalse(first.duplicate(), tag + ": 全新一轮的第一次提交不得被判为重复提交");
        GroupResult replay = c.commitGroup("http-grp-1", members);
        assertTrue(replay.duplicate(), tag + ": 同一轮内同组重放必须幂等");
        assertEquals("alice", c.get("http:profile"));
        assertEquals("admin", c.get("http:perms"));
        String conclusion = "OK,seq=" + first.beginSeq() + ".." + first.endSeq()
                + ",replay=OK_DUPLICATE,live=" + first.liveEntries();
        log.info("重复执行判定[{}] 场景=整组提交 group=http-grp-1 keys=http:profile,http:perms "
                + "依据=本轮首次提交非重复/同轮重放幂等 恢复结果={}", tag, conclusion);
        return conclusion;
    }

    /** 场景 B：单键读写删。 */
    private String singleKeyScenario(Path dir, String tag) {
        DurableKvCache c = boot(dir, 0);
        c.put("alpha", "hello", 60_000L);
        String hit = c.get("alpha");
        assertEquals("hello", hit);
        assertTrue(c.delete("alpha"));
        assertNull(c.get("alpha"));
        assertFalse(c.delete("alpha"));
        String conclusion = "PUT->GET(" + hit + ")->DELETE(existed=true)->GET(miss)->DELETE(existed=false)";
        log.info("重复执行判定[{}] 场景=单键读写删 key=alpha 依据=PUT后命中/DELETE后miss 恢复结果={}",
                tag, conclusion);
        return conclusion;
    }

    /** 场景 C：容量淘汰。 */
    private String evictionScenario(Path dir, String tag) {
        DurableKvCache c = boot(dir, 2);
        c.put("a", "1", 60_000L);
        c.put("b", "2", 60_000L);
        assertEquals("1", c.get("a")); // 刷新 a，使 b 成为 LRU 最旧
        var r = c.put("c", "3", 60_000L);
        assertTrue(r.evicted());
        assertEquals("b", r.evictedKey());
        assertNull(c.get("b"));
        String conclusion = "evicted=b,live=" + c.metrics().liveEntries()
                + ",evictionCount=" + c.metrics().evictionCount();
        log.info("重复执行判定[{}] 场景=容量淘汰 keys=a,b,c 依据=LRU最旧b被淘汰 恢复结果={}", tag, conclusion);
        return conclusion;
    }

    @Test
    void sameSuiteRepeatedRoundsGiveIdenticalConclusions() {
        List<List<String>> rounds = new ArrayList<>();
        for (int round = 1; round <= 3; round++) {
            String tag = "第" + round + "轮";
            // 每一轮使用与上一轮相同的目录名：fresh() 先清除上一轮残留，再给出干净目录。
            Path groupDir = IsolatedTestDir.fresh("repeat-group");
            Path singleDir = IsolatedTestDir.fresh("repeat-single");
            Path evictDir = IsolatedTestDir.fresh("repeat-evict");
            rounds.add(List.of(
                    groupScenario(groupDir, tag),
                    singleKeyScenario(singleDir, tag),
                    evictionScenario(evictDir, tag)));
        }
        assertEquals(rounds.get(0), rounds.get(1), "第2轮结论必须与第1轮完全一致");
        assertEquals(rounds.get(0), rounds.get(2), "第3轮结论必须与第1轮完全一致");
        log.info("重复执行判定 依据=连续3轮同套场景 恢复结果=三轮结论逐字一致: {}", rounds.get(0));
    }

    @Test
    void killedPreviousRoundLeftoverIsWipedBeforeNextRound() {
        // —— 第 1 轮：确认一组后，在第二组提交中途"被强杀"（TXEND 永不落盘），
        //    并追加一条无换行的半行模拟页撕裂，留下不完整残留。 ——
        Path dir = IsolatedTestDir.fresh("repeat-leftover");
        PersistenceLog[] dropped = new PersistenceLog[1];
        DurableKvCache c1 = boot(dir, 0, real -> {
            dropped[0] = new PersistenceLog() {
                @Override
                public void append(OperationRecord r) {
                    if (r.type() != OperationType.TXEND) {
                        real.append(r); // TXEND 之前的记录正常落盘，TXEND 丢失 = 提交中途被 kill
                    }
                }

                @Override
                public Iterable<OperationRecord> readAll() {
                    return real.readAll();
                }

                @Override
                public Iterable<WalEntry> readAllWithOffsets() {
                    return real.readAllWithOffsets();
                }

                @Override
                public long size() {
                    return real.size();
                }

                @Override
                public void truncateTo(long size) {
                    real.truncateTo(size);
                }

                @Override
                public long nextSeq() {
                    return real.nextSeq();
                }

                @Override
                public void close() {
                    real.close();
                }
            };
            return dropped[0];
        });
        c1.commitGroup("grp-done", List.of(GroupMember.insert("k-done", "v", 600_000L)));
        c1.commitGroup("grp-killed", List.of(GroupMember.insert("k-half", "v", 600_000L)));
        dropped[0].close(); // 强杀：不做任何清理，WAL 上留下未闭合组帧
        try {
            Files.writeString(dir.resolve("cache.wal"), "99|PUT|aGFsZg", // 半行撕裂（无换行）
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        long leftoverBytes;
        try {
            leftoverBytes = Files.size(dir.resolve("cache.wal"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        assertTrue(leftoverBytes > 0);
        log.info("残留构造 group=grp-done(已确认)/grp-killed(TXEND前被强杀) keys=k-done,k-half "
                + "依据=WAL残留{}字节(未闭合组帧+半行撕裂) 恢复结果=留给下一轮处理", leftoverBytes);

        // —— 第 2 轮：同名目录先被整体清除，本轮从干净状态开始，结论确定。 ——
        Path dir2 = IsolatedTestDir.fresh("repeat-leftover");
        try (var listing = Files.list(dir2)) {
            assertEquals(0, listing.count(), "新一轮开始前，上一轮残留必须被整体清除");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        DurableKvCache c2 = boot(dir2, 0);
        assertNull(c2.get("k-done"), "上一轮的已确认提交不得算进本轮");
        assertNull(c2.get("k-half"), "上一轮的半组残留不得算进本轮");
        assertNull(c2.groupStatus("grp-done"));
        assertNull(c2.groupStatus("grp-killed"));
        GroupResult r = c2.commitGroup("grp-done",
                List.of(GroupMember.insert("k-done", "v", 600_000L)));
        assertTrue(r.committed());
        assertFalse(r.duplicate(), "本轮第一次提交必须判定为全新提交，不得被历史残留误判为重复");
        assertEquals("v", c2.get("k-done"));
        log.info("残留恢复判定 group=grp-done keys=k-done 依据=同名目录先清残留再启动 "
                + "恢复结果=本轮从干净状态开始, 首次提交OK(非重复), live={}", r.liveEntries());
    }

    @Test
    void scenarioOrderDoesNotChangeConclusions() {
        // 每种顺序下，各场景始终使用自己专属的目录名（每轮 fresh），结论只取决于场景本身。
        List<String> order1 = new ArrayList<>();
        order1.add(groupScenario(IsolatedTestDir.fresh("order-group"), "顺序1"));
        order1.add(singleKeyScenario(IsolatedTestDir.fresh("order-single"), "顺序1"));
        order1.add(evictionScenario(IsolatedTestDir.fresh("order-evict"), "顺序1"));

        List<String> order2 = new ArrayList<>(3);
        String evict = evictionScenario(IsolatedTestDir.fresh("order-evict"), "顺序2");
        String single = singleKeyScenario(IsolatedTestDir.fresh("order-single"), "顺序2");
        String group = groupScenario(IsolatedTestDir.fresh("order-group"), "顺序2");
        order2.add(group);
        order2.add(single);
        order2.add(evict);

        assertEquals(order1, order2, "执行顺序变化不得改变任何场景的结论");
        log.info("顺序无关判定 依据=[整组,单键,淘汰]与[淘汰,单键,整组]两种顺序 "
                + "恢复结果=各场景结论逐字一致: {}", order1);
    }
}
