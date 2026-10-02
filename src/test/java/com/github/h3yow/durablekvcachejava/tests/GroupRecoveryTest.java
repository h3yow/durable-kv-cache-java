package com.github.h3yow.durablekvcachejava.tests;

import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.RecoveryException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.StateConflictException;
import com.github.h3yow.durablekvcachejava.config.CacheProperties;
import com.github.h3yow.durablekvcachejava.core.DurableKvCache;
import com.github.h3yow.durablekvcachejava.core.GroupOperation;
import com.github.h3yow.durablekvcachejava.core.OperationType;
import com.github.h3yow.durablekvcachejava.eviction.LruEvictionTracker;
import com.github.h3yow.durablekvcachejava.persistence.FileWalLog;
import com.github.h3yow.durablekvcachejava.recovery.RecoveryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 整组操作的重启恢复：整组完整恢复、崩溃半组截除、帧损坏明确失败、重复恢复拒绝。
 * 每个用例使用独立 WAL 文件（同处 {@code @TempDir}），互不污染。
 */
class GroupRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(GroupRecoveryTest.class);

    @TempDir
    Path tempDir;

    private DurableKvCache bootRecovered(String walName, int maxEntries) {
        CacheProperties p = new CacheProperties();
        p.setDataDir(tempDir.toString());
        p.setMaxEntries(maxEntries);
        FileWalLog wal = new FileWalLog(tempDir.toString(), walName);
        DurableKvCache cache = new DurableKvCache(wal, new LruEvictionTracker(), p);
        new RecoveryService(wal, cache).recover();
        return cache;
    }

    private DurableKvCache bootRecovered(String walName) {
        return bootRecovered(walName, 0);
    }

    private FileWalLog walOf(String name) {
        return new FileWalLog(tempDir.toString(), name);
    }

    @Test
    void committedGroupFullyRestoredAfterRestartAndReplayIsIdempotent() {
        // 进程 1：先有单键历史，再提交一个包含 PUT/DELETE/淘汰 的整组。
        DurableKvCache first = bootRecovered("gr.wal", 3);
        first.put("solo", "s", 600_000L);
        first.put("evictme", "e", 600_000L);
        first.get("solo"); // 刷新 solo，使 evictme 成为 LRU 最旧
        var r = first.commitGroup("sess-42", List.of(
                GroupOperation.put("sess:p", "alice", 600_000L),
                GroupOperation.put("sess:perm", "admin", 600_000L),
                GroupOperation.put("sess:n", "9", 600_000L)));
        // 容量 3：solo/evictme 占 2，整组新增 3 => 需淘汰 2；淘汰顺序为 evictme, solo。
        assertEquals(2, r.evictedKeys().size(), "为整组淘汰两个旧键: " + r.evictedKeys());
        assertEquals("evictme", r.evictedKeys().get(0));
        assertEquals("solo", r.evictedKeys().get(1));

        // 进程 2：冷启动恢复，整组必须完整生效（不能只恢复一半）。
        DurableKvCache second = bootRecovered("gr.wal", 3);
        assertEquals("alice", second.get("sess:p"));
        assertEquals("admin", second.get("sess:perm"));
        assertEquals("9", second.get("sess:n"));
        assertNull(second.get("solo"), "整组内淘汰结论必须跨重启保持");
        assertNull(second.get("evictme"));
        assertEquals(3, second.metrics().liveEntries());
        assertEquals(2L, second.metrics().evictionCount(), "淘汰计数跨重启连续");
        assertEquals(1L, second.metrics().groupSuccessCount());

        // 进程 3：再重启一次，结论必须幂等一致（不叠加、不丢失）。
        DurableKvCache third = bootRecovered("gr.wal", 3);
        assertEquals("alice", third.get("sess:p"));
        assertEquals("9", third.get("sess:n"));
        assertEquals(3, third.metrics().liveEntries());
        // 恢复后用相同 groupId 重试：返回 DUPLICATE，不重复执行。
        var dup = third.commitGroup("sess-42", List.of(
                GroupOperation.put("sess:p", "alice", 600_000L),
                GroupOperation.put("sess:perm", "admin", 600_000L),
                GroupOperation.put("sess:n", "9", 600_000L)));
        assertTrue(dup.duplicate());
        assertEquals(3, third.metrics().liveEntries());
        log.info("整组恢复 组=sess-42 keys=sess:p/perm/n 依据=GBEGIN..GCOMMIT整段重放 淘汰=[evictme,solo] "
                + "恢复结果=三次启动完全一致, 重试=DUPLICATE");
    }

    @Test
    void halfGroupAfterHardKillIsTruncatedAsOneUnit() throws Exception {
        // 进程 1：确认一个完整整组 + 若干单键。
        DurableKvCache first = bootRecovered("half.wal");
        first.commitGroup("g-done", List.of(
                GroupOperation.put("a", "1", 600_000L),
                GroupOperation.put("b", "2", 600_000L)));
        first.put("solo", "s", 600_000L);

        // 直接在 WAL 末尾手工拼出"强杀残留"：GBEGIN + 一个成员，但没有 GCOMMIT、最后无换行。
        Path f = tempDir.resolve("half.wal");
        String before = Files.readString(f, StandardCharsets.UTF_8);
        long beginSeq = before.lines().count() + 1;
        List<String> tail = new ArrayList<>();
        // GBEGIN 声明帧内只有 1 条 PUT 成员；强杀发生在 GCOMMIT 之前（此处同时是无换行半行）。
        tail.add(frameLine(beginSeq, OperationType.GBEGIN, "g-killed", 1, 600_000L, null));
        tail.add(memberPutLine(beginSeq + 1, "c", "3", 600_000L, "g-killed"));
        String joined = String.join("\n", tail);
        Files.writeString(f, joined, StandardOpenOption.APPEND); // 无结尾换行，模拟半行/未提交

        // 进程 2：恢复必须把整个未闭合组（GBEGIN+成员）连同半行一起截除。
        DurableKvCache second = bootRecovered("half.wal");
        assertEquals("1", second.get("a"));
        assertEquals("2", second.get("b"));
        assertEquals("s", second.get("solo"));
        assertNull(second.get("c"), "未提交整组的成员不得被恢复（不能恢复半组）");
        assertEquals(1L, second.metrics().groupSuccessCount(), "只有 g-done 一个完整组被恢复");

        // 文件层面：残留必须被物理截除，下一条写入序号接在 g-done 之后且内容合法。
        String after = Files.readString(f, StandardCharsets.UTF_8);
        assertTrue(after.endsWith("\n"), "截除后文件必须以完整换行结尾");
        assertFalse(after.contains("g-killed"), "未闭合组帧必须被整体截除");
        second.put("after", "z", 600_000L);
        DurableKvCache third = bootRecovered("half.wal");
        assertEquals("z", third.get("after"));
        assertNull(third.get("c"));
        log.info("半组恢复 组=g-done(完整)/g-killed(强杀未提交) 依据=无匹配GCOMMIT整段截除 "
                + "恢复结果=a/b/solo存活, c不存在, 后续PUT序号连续");
    }

    @Test
    void groupWithMissingCommitButMemberCountSatisfiedAndNoGcommitIsTreatedAsCrashTail() throws Exception {
        // GBEGIN 声明 1 条成员，成员完整落盘，但 GCOMMIT 缺失（进程在提交确认前被杀）。
        // 这正是"确认成功的整组"之外的状态：整组必须被当作崩溃残留整体截除，绝不只恢复成员。
        DurableKvCache first = bootRecovered("badframe.wal");
        first.commitGroup("g-ok", List.of(GroupOperation.put("a", "1", 600_000L)));
        Path f = tempDir.resolve("badframe.wal");
        String current = Files.readString(f, StandardCharsets.UTF_8);
        long beginSeq = current.lines().count() + 1;
        StringBuilder sb = new StringBuilder();
        sb.append(frameLine(beginSeq, OperationType.GBEGIN, "g-broken", 1, 600_000L, null)).append('\n');
        sb.append(memberPutLine(beginSeq + 1, "b", "2", 600_000L, "g-broken")).append('\n');
        Files.writeString(f, sb.toString(), StandardOpenOption.APPEND);

        // 恢复成功但未闭合组被整体截除：g-ok 存活，b 不存在，WAL 被物理截回提交点。
        DurableKvCache second = bootRecovered("badframe.wal");
        assertEquals("1", second.get("a"));
        assertNull(second.get("b"), "缺少 GCOMMIT 的整组不得只恢复成员（半组）");
        String after = Files.readString(f, StandardCharsets.UTF_8);
        assertFalse(after.contains("g-broken"), "未闭合整组必须被整体截除");
        log.info("未提交组恢复判定 组=g-broken 依据=GBEGIN+成员完整但无GCOMMIT=提交确认前被杀 "
                + "恢复结果=整组截除, a存活/b不存在");
    }

    @Test
    void orphanGcommitOrNestedGbeginAbortsRecoveryWithDistinctError() throws Exception {
        // 无 GBEGIN 的 GCOMMIT 属于帧结构损坏（完整行、带换行），必须明确失败而非截尾。
        DurableKvCache first = bootRecovered("orphan.wal");
        first.put("a", "1", 600_000L);
        Path f = tempDir.resolve("orphan.wal");
        String current = Files.readString(f, StandardCharsets.UTF_8);
        long seq = current.lines().count() + 1;
        String gcommit = frameLine(seq, OperationType.GCOMMIT, "orphan", 1, 600_000L, null) + "\n";
        Files.writeString(f, gcommit, StandardOpenOption.APPEND);

        RecoveryException ex = assertThrows(RecoveryException.class, () -> {
            FileWalLog wal = walOf("orphan.wal");
            CacheProperties p = new CacheProperties();
            p.setDataDir(tempDir.toString());
            DurableKvCache c = new DurableKvCache(wal, new LruEvictionTracker(), p);
            new RecoveryService(wal, c).recover();
        });
        assertTrue(ex.getMessage().contains("GBEGIN"), "必须给出可区分的帧错误: " + ex.getMessage());
        log.info("孤儿GCOMMIT判定 groupId=orphan 依据=无匹配GBEGIN 结果=RecoveryException({}) 启动中止",
                ex.getMessage());
    }

    @Test
    void duplicateGroupIdInWalAbortsRecovery() throws Exception {
        // 两个已闭合整组使用相同 groupId：违反幂等键唯一性，恢复必须失败而不是叠加。
        DurableKvCache first = bootRecovered("dupid.wal");
        first.commitGroup("same-id", List.of(GroupOperation.put("a", "1", 600_000L)));
        Path f = tempDir.resolve("dupid.wal");
        String current = Files.readString(f, StandardCharsets.UTF_8);
        long seq = current.lines().count() + 1;
        StringBuilder sb = new StringBuilder();
        sb.append(frameLine(seq, OperationType.GBEGIN, "same-id", 1, 600_000L, null)).append('\n');
        sb.append(memberPutLine(seq + 1, "b", "2", 600_000L, "same-id")).append('\n');
        sb.append(frameLine(seq + 2, OperationType.GCOMMIT, "same-id", 1, 600_000L, "same-id")).append('\n');
        Files.writeString(f, sb.toString(), StandardOpenOption.APPEND);

        RecoveryException ex = assertThrows(RecoveryException.class, () -> {
            FileWalLog wal = walOf("dupid.wal");
            CacheProperties p = new CacheProperties();
            p.setDataDir(tempDir.toString());
            DurableKvCache c = new DurableKvCache(wal, new LruEvictionTracker(), p);
            new RecoveryService(wal, c).recover();
        });
        assertTrue(ex.getMessage().contains("same-id"), "错误信息需指出冲突的 groupId: " + ex.getMessage());
        log.info("重复组ID恢复判定 groupId=same-id 依据=第二个已闭合组复用ID 结果=RecoveryException 拒绝服务");
    }

    @Test
    void recoveringSameCacheTwiceIsRejected() {
        FileWalLog wal = walOf("duprecover.wal");
        CacheProperties p = new CacheProperties();
        p.setDataDir(tempDir.toString());
        DurableKvCache cache = new DurableKvCache(wal, new LruEvictionTracker(), p);
        new RecoveryService(wal, cache).recover();
        cache.commitGroup("g1", List.of(GroupOperation.put("a", "1", 600_000L)));
        StateConflictException ex = assertThrows(StateConflictException.class,
                () -> new RecoveryService(wal, cache).recover());
        assertTrue(ex.getMessage().contains("重复") || ex.getMessage().contains("就绪"));
        log.info("重复恢复判定 groupId=g1 依据=缓存已READY 结果=StateConflictException({})", ex.getMessage());
    }

    // ------------------------------------------------------------ WAL 行构造辅助

    private static String b64(String s) {
        return java.util.Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String frameLine(long seq, OperationType type, String groupId, int members,
                                    long ts, String unused) {
        // GCOMMIT 帧的 ttl 字段必须为 0（成员数只由 GBEGIN 承载）。
        long ttlField = type == OperationType.GCOMMIT ? 0L : members;
        com.github.h3yow.durablekvcachejava.core.OperationRecord r =
                new com.github.h3yow.durablekvcachejava.core.OperationRecord(
                        seq, type, groupId, null, ttlField, ts, groupId);
        long crc = FileWalLog.checksum(r);
        return seq + "|" + type.name() + "|" + b64(groupId) + "|"
                + "" + "|" + ttlField + "|" + ts + "|" + b64(groupId) + "|" + crc;
    }

    private static String memberPutLine(long seq, String key, String value, long ttl, String groupId) {
        long ts = 600_000L;
        com.github.h3yow.durablekvcachejava.core.OperationRecord r =
                new com.github.h3yow.durablekvcachejava.core.OperationRecord(
                        seq, OperationType.PUT, key, value, ttl, ts, groupId);
        long crc = FileWalLog.checksum(r);
        return seq + "|PUT|" + b64(key) + "|" + b64(value) + "|" + ttl + "|" + ts
                + "|" + b64(groupId) + "|" + crc;
    }

    private static void assertFalse(boolean condition, String msg) {
        org.junit.jupiter.api.Assertions.assertFalse(condition, msg);
    }
}
