package com.github.h3yow.durablekvcachejava.tests;

import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.RecoveryException;
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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 整组操作的强杀/重启恢复：
 * 已确认整组完整恢复、TXEND 前崩溃整组丢弃不出现半组、重复提交不叠加、
 * 闭合帧损坏恢复中止、容量/淘汰结论跨重启连续。每个用例独立 WAL 文件 + @TempDir。
 */
class GroupRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(GroupRecoveryTest.class);

    @TempDir
    Path tempDir;

    private DurableKvCache boot(String wal) {
        return boot(wal, 0);
    }

    private DurableKvCache boot(String wal, int maxEntries) {
        CacheProperties p = new CacheProperties();
        p.setDataDir(tempDir.toString());
        p.setMaxEntries(maxEntries);
        FileWalLog logWal = new FileWalLog(tempDir.toString(), wal);
        DurableKvCache cache = new DurableKvCache(logWal, new LruEvictionTracker(), p);
        new RecoveryService(logWal, cache).recover();
        return cache;
    }

    private FileWalLog rawWal(String name) {
        return new FileWalLog(tempDir.toString(), name);
    }

    @Test
    void confirmedGroupFullyRecoveredAcrossRestart() {
        DurableKvCache first = boot("gr.wal");
        first.commitGroup("grp-full", List.of(
                GroupMember.insert("s:profile", "alice", 600_000L),
                GroupMember.insert("s:perms", "admin", 600_000L),
                GroupMember.insert("s:count", "7", 600_000L)));
        first.commitGroup("grp-del", List.of(
                GroupMember.update("s:profile", "alice2", 600_000L),
                GroupMember.delete("s:count")));

        // 模拟 kill -9 后冷启动：不做任何优雅收尾。
        DurableKvCache second = boot("gr.wal");
        assertEquals("alice2", second.get("s:profile"), "UPDATE 结论必须完整恢复");
        assertEquals("admin", second.get("s:perms"));
        assertNull(second.get("s:count"), "组内 DELETE 结论必须完整恢复");
        assertNotNull(second.groupStatus("grp-full"), "已确认组必须能查到结论");
        assertNotNull(second.groupStatus("grp-del"));
        // 重启后相同 groupId + 相同成员仍是幂等，不叠加。
        var dup = second.commitGroup("grp-full", List.of(
                GroupMember.insert("s:profile", "alice", 600_000L),
                GroupMember.insert("s:perms", "admin", 600_000L),
                GroupMember.insert("s:count", "7", 600_000L)));
        assertTrue(dup.duplicate());
        assertEquals("alice2", second.get("s:profile"), "重复提交不得把后来的 UPDATE 覆盖回去");
        assertNull(second.get("s:count"), "重复提交不得把已删除键加回来");
        assertEquals(2, second.metrics().liveEntries());

        // 再重启一次：结论仍然一致（恢复本身幂等，无半组）。
        DurableKvCache third = boot("gr.wal");
        assertEquals("alice2", third.get("s:profile"));
        assertNull(third.get("s:count"));
        assertEquals(2, third.metrics().liveEntries());
        log.info("整组重启判定 group=grp-full/grp-del 依据=闭合帧全序重放+groupId登记 "
                + "恢复结果=三键组UPDATE/DELETE完整, 重复提交幂等, 两次重启live均=2");
    }

    @Test
    void crashBeforeTxEndDiscardsWholeGroupOnFirstRestart() throws IOException {
        DurableKvCache first = boot("half.wal");
        first.put("before", "keep", 600_000L);
        first.commitGroup("grp-ok", List.of(
                GroupMember.insert("g1", "1", 600_000L),
                GroupMember.insert("g2", "2", 600_000L)));
        long lenBeforeHalf;
        try (FileWalLog w = rawWal("half.wal")) {
            lenBeforeHalf = w.size();
        }
        // 手工制造"提交中被强杀"：写入 TXBEG + 两个成员的 PUT，但永远不写 TXEND。
        try (FileWalLog w = rawWal("half.wal")) {
            long seq = w.nextSeq();
            w.append(new com.github.h3yow.durablekvcachejava.core.OperationRecord(
                    seq, com.github.h3yow.durablekvcachejava.core.OperationType.TXBEG,
                    "grp-half", "I:h1\nI:h2\n", 2, System.currentTimeMillis(), "grp-half"));
            w.append(new com.github.h3yow.durablekvcachejava.core.OperationRecord(
                    seq + 1, com.github.h3yow.durablekvcachejava.core.OperationType.PUT,
                    "h1", "v1", 600_000L, System.currentTimeMillis(), "grp-half"));
            w.append(new com.github.h3yow.durablekvcachejava.core.OperationRecord(
                    seq + 2, com.github.h3yow.durablekvcachejava.core.OperationType.PUT,
                    "h2", "v2", 600_000L, System.currentTimeMillis(), "grp-half"));
            // 不写 TXEND，直接"强杀"。
        }

        // 第一次重启：整帧必须被截除丢弃，绝不能只恢复 h1 不恢复 h2。
        DurableKvCache second = boot("half.wal");
        assertEquals("keep", second.get("before"), "组之前的完整状态必须保留");
        assertEquals("1", second.get("g1"), "此前已确认组不受影响");
        assertEquals("2", second.get("g2"));
        assertNull(second.get("h1"), "未确认整组不得恢复（不能只恢复一半）");
        assertNull(second.get("h2"));
        assertNull(second.groupStatus("grp-half"), "未确认组不得登记");
        long lenAfterRecover;
        try (FileWalLog w = rawWal("half.wal")) {
            lenAfterRecover = w.size();
        }
        assertEquals(lenBeforeHalf, lenAfterRecover, "未闭合帧必须从 WAL 物理截除");

        // 崩溃后允许用新的 groupId 重新提交同一批键（旧 groupId 因整组丢弃从未登记，
        // 但为了语义清晰，这里用新 id；旧 id 同样可再次使用因为它未登记）。
        var retried = second.commitGroup("grp-half", List.of(
                GroupMember.insert("h1", "v1", 600_000L),
                GroupMember.insert("h2", "v2", 600_000L)));
        assertTrue(retried.committed() && !retried.duplicate(),
                "崩溃丢弃的组重新提交应作为新提交成功，而非误判重复");
        assertEquals("v1", second.get("h1"));
        assertEquals("v2", second.get("h2"));

        // 第二次重启：新提交完整保留，没有重复恢复叠加。
        DurableKvCache third = boot("half.wal");
        assertEquals("keep", third.get("before"));
        assertEquals("v1", third.get("h1"));
        assertEquals("v2", third.get("h2"));
        assertEquals(5, third.metrics().liveEntries(),
                "before+g1+g2(截除后恢复的3键)+h1+h2(重试整组) = 5，无半组、无叠加");
        log.info("强杀半组判定 group=grp-half 依据=无TXEND=未确认提交 恢复结果=整帧{}字节起截除, "
                + "before/g1/g2保留, h1/h2均不存在, 重新提交成功, 二次重启live=5", lenBeforeHalf);
    }

    @Test
    void crashWithTruncatedLastLineAlsoRecoversToPreGroupState() throws IOException {
        DurableKvCache first = boot("crush.wal");
        first.put("pre", "p", 600_000L);
        long len;
        try (FileWalLog w = rawWal("crush.wal")) {
            len = w.size();
        }
        // TXBEG 只写了半截（连换行都没有）——最极端的页撕裂，必须整段丢弃。
        Files.writeString(tempDir.resolve("crush.wal"),
                "2|TXBEG|grp-x|bW8K", java.nio.file.StandardOpenOption.APPEND);
        DurableKvCache second = boot("crush.wal");
        assertEquals("p", second.get("pre"));
        assertNull(second.groupStatus("grp-x"));
        try (FileWalLog w = rawWal("crush.wal")) {
            assertEquals(len, w.size(), "半行也必须截除干净");
        }
        log.info("撕裂半行判定 group=grp-x 依据=末行无换行=崩溃残留 恢复结果=截除至{}字节, pre保留", len);
    }

    @Test
    void corruptedClosedFrameAbortsRecoveryWithDistinctError() throws IOException {
        DurableKvCache first = boot("corrupt.wal");
        first.commitGroup("grp-c", List.of(
                GroupMember.insert("c1", "v1", 600_000L),
                GroupMember.insert("c2", "v2", 600_000L)));
        Path walFile = tempDir.resolve("corrupt.wal");
        List<String> lines = Files.readAllLines(walFile, StandardCharsets.UTF_8);
        // 帧布局：TXBEG, PUT c1, PUT c2, TXEND。翻转成员值 base64 首字符造成 CRC 失败。
        String memberLine = lines.get(2); // 第 3 行 = PUT c2
        char[] arr = memberLine.toCharArray();
        int sep = 0;
        int valueStart = 0;
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == '|') {
                sep++;
                if (sep == 3) {
                    valueStart = i + 1;
                    break;
                }
            }
        }
        char ch = arr[valueStart];
        arr[valueStart] = Character.isUpperCase(ch) ? Character.toLowerCase(ch) : Character.toUpperCase(ch);
        lines.set(2, new String(arr));
        Files.writeString(walFile, String.join("\n", lines) + "\n", StandardCharsets.UTF_8);

        RecoveryException ex = assertThrows(RecoveryException.class, () -> boot("corrupt.wal"));
        assertTrue(ex.getMessage().contains("校验"), "闭合帧内容损坏必须以可区分原因中止: "
                + ex.getMessage());
        log.info("闭合帧损坏判定 group=grp-c 依据=成员行CRC不匹配 恢复结果=RecoveryException({}), "
                + "拒绝对外服务", ex.getMessage());
    }

    @Test
    void groupEvictionDecisionSurvivesRestart() {
        // 容量 2：组提交 3 个 INSERT 必须先淘汰最旧的非成员键，淘汰结论跨重启保持。
        DurableKvCache first = boot("gev.wal", 2);
        first.put("old1", "1", 600_000L);
        first.put("old2", "2", 600_000L);
        first.get("old2"); // old1 成为 LRU 最旧
        var r = first.commitGroup("grp-ev", List.of(
                GroupMember.insert("n1", "a", 600_000L)));
        assertEquals(List.of("old1"), r.evictedKeys());
        assertEquals(1L, first.metrics().evictionCount());

        DurableKvCache second = boot("gev.wal", 2);
        assertNull(second.get("old1"), "组提交淘汰的键重启后不得复活");
        assertEquals("2", second.get("old2"));
        assertEquals("a", second.get("n1"));
        assertEquals(1L, second.metrics().evictionCount(), "淘汰计数必须跨重启连续");
        assertEquals(2, second.metrics().liveEntries(), "容量结论跨重启接得上");

        // 重启后再做一次会触发淘汰的组提交：序号、容量仍然连续。
        var r2 = second.commitGroup("grp-ev2", List.of(
                GroupMember.insert("n2", "b", 600_000L)));
        // 此时 LRU：old2 被 GET 刷新过（重启重放 touch），n1 后写入 => 最旧为 old2
        assertEquals(List.of("old2"), r2.evictedKeys());
        log.info("组淘汰重启判定 group=grp-ev/grp-ev2 依据=EVICT在闭合帧内参与重放 "
                + "恢复结果=old1/old2先后淘汰不复活, evictionCount连续, live始终=2");
    }

    @Test
    void repeatedRecoveryDoesNotDuplicateGroupState() {
        DurableKvCache first = boot("rep.wal");
        first.commitGroup("grp-rep", List.of(
                GroupMember.insert("r1", "1", 600_000L),
                GroupMember.insert("r2", "2", 600_000L)));
        // 连续三次冷启动：每次恢复结论必须完全相同。
        for (int attempt = 1; attempt <= 3; attempt++) {
            DurableKvCache c = boot("rep.wal");
            assertEquals("1", c.get("r1"), "第 " + attempt + " 次重启 r1");
            assertEquals("2", c.get("r2"), "第 " + attempt + " 次重启 r2");
            assertEquals(2, c.metrics().liveEntries());
            assertNotNull(c.groupStatus("grp-rep"));
        }
        log.info("重复恢复判定 group=grp-rep 依据=重放纯应用+groupId表重建 "
                + "恢复结果=连续3次冷启动 live=2 结论一致, 无叠加无丢失");
    }
}
