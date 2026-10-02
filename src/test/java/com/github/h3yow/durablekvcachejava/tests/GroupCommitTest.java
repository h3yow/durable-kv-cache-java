package com.github.h3yow.durablekvcachejava.tests;

import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.CapacityExceededException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.InvalidRequestException;
import com.github.h3yow.durablekvcachejava.cache.GroupIdConflictException;
import com.github.h3yow.durablekvcachejava.cache.GroupMember;
import com.github.h3yow.durablekvcachejava.cache.GroupResult;
import com.github.h3yow.durablekvcachejava.cache.GroupStaleException;
import com.github.h3yow.durablekvcachejava.cache.KeyValueCache;
import com.github.h3yow.durablekvcachejava.config.CacheProperties;
import com.github.h3yow.durablekvcachejava.core.DurableKvCache;
import com.github.h3yow.durablekvcachejava.eviction.LruEvictionTracker;
import com.github.h3yow.durablekvcachejava.persistence.FileWalLog;
import com.github.h3yow.durablekvcachejava.persistence.PersistenceLog;
import com.github.h3yow.durablekvcachejava.recovery.RecoveryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 整组操作测试：原子生效、前置失败零副作用、幂等/冲突、容量边界。
 * 每个用例使用独立 WAL 文件，JUnit @TempDir 保证数据目录互不污染。
 */
class GroupCommitTest {

    private static final Logger log = LoggerFactory.getLogger(GroupCommitTest.class);

    @TempDir
    Path tempDir;

    private DurableKvCache boot(String walName, int maxEntries) {
        CacheProperties p = new CacheProperties();
        p.setDataDir(tempDir.toString());
        p.setMaxEntries(maxEntries);
        FileWalLog wal = new FileWalLog(tempDir.toString(), walName);
        DurableKvCache cache = new DurableKvCache(wal, new LruEvictionTracker(), p);
        new RecoveryService(wal, cache).recover();
        return cache;
    }

    private static List<GroupMember> profilePermsCount(String prefix, String v) {
        return List.of(
                GroupMember.insert(prefix + ":profile", v, 600_000L),
                GroupMember.insert(prefix + ":perms", v + "-perms", 600_000L),
                GroupMember.insert(prefix + ":count", "1", 600_000L));
    }

    @Test
    void groupCommitAppliesAllMembersAtomically() {
        DurableKvCache c = boot("g-commit.wal", 0);
        GroupResult r = c.commitGroup("grp-session-1", profilePermsCount("s1", "alice"));
        assertTrue(r.committed());
        assertFalse(r.duplicate());
        assertEquals(List.of("s1:profile", "s1:perms", "s1:count"), r.insertedKeys());
        assertEquals("alice", c.get("s1:profile"));
        assertEquals("alice-perms", c.get("s1:perms"));
        assertEquals("1", c.get("s1:count"));
        Map<String, String> snap = c.snapshot();
        assertEquals("alice", snap.get("s1:profile"));
        log.info("整组成功判定 group=grp-session-1 keys=s1:profile,s1:perms,s1:count "
                + "依据=TXEND确认后整帧原子生效 恢复结果=在线提交 seq={}..{} live={}",
                r.beginSeq(), r.endSeq(), r.liveEntries());
    }

    @Test
    void groupMixesUpdateAndDeleteAndIsQueryable() {
        DurableKvCache c = boot("g-mix.wal", 0);
        c.commitGroup("grp-init", List.of(
                GroupMember.insert("a", "1", 600_000L),
                GroupMember.insert("b", "2", 600_000L),
                GroupMember.insert("d", "4", 600_000L)));
        GroupResult r = c.commitGroup("grp-mix", List.of(
                GroupMember.update("a", "11", 600_000L),
                GroupMember.update("b", "22", 600_000L),
                GroupMember.delete("d")));
        assertEquals(List.of(), r.insertedKeys());
        assertEquals(List.of("a", "b"), r.updatedKeys());
        assertEquals(List.of("d"), r.deletedKeys());
        assertEquals("11", c.get("a"));
        assertEquals("22", c.get("b"));
        assertNull(c.get("d"));
        GroupResult status = c.groupStatus("grp-mix");
        assertTrue(status.committed());
        assertEquals(3, status.updatedKeys().size() + status.insertedKeys().size()
                + status.deletedKeys().size());
        assertNull(c.groupStatus("not-exist-group"), "未确认提交的 groupId 必须查不到");
        log.info("混合整组判定 group=grp-mix keys=a,b更新,d删除 依据=UPDATE存活命中/DELETE存活命中 "
                + "恢复结果=在线提交, groupStatus可查, 未知groupId返回null");
    }

    @Test
    void staleMemberRejectsWholeGroupWithNoSideEffect() {
        DurableKvCache c = boot("g-stale.wal", 0);
        c.put("exists", "v", 600_000L);
        c.put("gone", "v", 1L);
        c.put("deleted", "v", 600_000L);
        c.delete("deleted");
        try {
            Thread.sleep(5);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        long seqBefore = peekNextSeq(c);
        // UPDATE 一个已过期键 + DELETE 一个已删除键：整组必须拒绝。
        GroupStaleException ex = assertThrows(GroupStaleException.class, () ->
                c.commitGroup("grp-stale", List.of(
                        GroupMember.update("exists", "new", 600_000L),
                        GroupMember.update("gone", "new", 600_000L),
                        GroupMember.delete("deleted"))));
        assertTrue(ex.staleKeys().contains("gone"));
        assertTrue(ex.staleKeys().contains("deleted"));
        assertEquals("v", c.get("exists"), "前置失败不得改动组内其它存活键");
        assertNull(c.groupStatus("grp-stale"), "失败组不得登记为已提交");
        assertEquals(seqBefore, peekNextSeq(c), "前置失败不得消耗 WAL 序号");
        log.info("整组前置失败判定 group=grp-stale staleKeys={} 依据=gone已过期/deleted已删除 "
                + "恢复结果=整组拒绝, exists不变, 无WAL序号消耗", ex.staleKeys());
    }

    @Test
    void insertOnExistingKeyIsStaleAndKeepsOldValue() {
        DurableKvCache c = boot("g-insert-stale.wal", 0);
        c.put("k", "old", 600_000L);
        GroupStaleException ex = assertThrows(GroupStaleException.class, () ->
                c.commitGroup("grp-ins", List.of(GroupMember.insert("k", "new", 600_000L))));
        assertEquals(List.of("k"), ex.staleKeys());
        assertEquals("old", c.get("k"), "INSERT 冲突不得覆盖旧值");
        log.info("INSERT冲突判定 group=grp-ins staleKeys=[k] 依据=目标键已存在 恢复结果=412拒绝, 旧值old保留");
    }

    @Test
    void duplicateGroupIdSameMembersIsIdempotentAcrossRestart() {
        DurableKvCache first = boot("g-dup.wal", 0);
        GroupResult r1 = first.commitGroup("grp-dup", profilePermsCount("u", "bob"));
        // 在线重复提交：幂等返回 duplicate=true，值不变。
        GroupResult r2 = first.commitGroup("grp-dup", profilePermsCount("u", "bob"));
        assertTrue(r2.duplicate());
        assertEquals(r1.beginSeq(), r2.beginSeq());
        assertEquals("bob", first.get("u:profile"));

        // 重启后重复提交同样幂等：结果不叠加、不丢失。
        DurableKvCache second = boot("g-dup.wal", 0);
        assertEquals("bob", second.get("u:profile"));
        GroupResult r3 = second.commitGroup("grp-dup", profilePermsCount("u", "bob"));
        assertTrue(r3.duplicate(), "重启后同 groupId 同成员必须仍识别为重复提交");
        assertEquals(3, second.metrics().liveEntries());
        log.info("幂等判定 group=grp-dup 依据=groupId+成员签名一致(含重启后登记) "
                + "恢复结果=三次提交只生效一次, live=3");
    }

    @Test
    void sameGroupIdDifferentMembersConflicts() {
        DurableKvCache c = boot("g-conflict.wal", 0);
        c.commitGroup("grp-x", List.of(GroupMember.insert("a", "1", 600_000L)));
        GroupIdConflictException ex = assertThrows(GroupIdConflictException.class, () ->
                c.commitGroup("grp-x", List.of(GroupMember.insert("b", "2", 600_000L))));
        assertEquals("grp-x", ex.groupId());
        assertEquals("1", c.get("a"), "串组冲突不得改动任何状态");
        assertNull(c.get("b"));
        log.info("串组冲突判定 group=grp-x 依据=同groupId成员签名不同 恢复结果=409 GROUP_ID_CONFLICT, 状态不变");
    }

    @Test
    void invalidGroupRequestsAreRejectedAsParameterErrors() {
        DurableKvCache c = boot("g-invalid.wal", 0);
        assertThrows(InvalidRequestException.class, () ->
                c.commitGroup("", List.of(GroupMember.insert("a", "1", 10L))));
        assertThrows(InvalidRequestException.class, () ->
                c.commitGroup("g", List.of()));
        assertThrows(InvalidRequestException.class, () ->
                c.commitGroup("g", List.of(
                        GroupMember.insert("a", "1", 10L),
                        GroupMember.update("a", "2", 10L))), "组内键重复");
        assertThrows(InvalidRequestException.class, () ->
                c.commitGroup("g", List.of(GroupMember.insert("a", null, 10L))));
        assertThrows(InvalidRequestException.class, () ->
                c.commitGroup("g", List.of(GroupMember.insert("a", "v", 0L))));
        log.info("整组参数错误判定 依据=空groupId/空组/键重复/null值/非正ttl "
                + "恢复结果=全部400 INVALID_REQUEST, 与412/507明确区分");
    }

    @Test
    void capacityExactlyFitsAfterEvictingOldestNonMembers() {
        DurableKvCache c = boot("g-fit.wal", 3);
        c.put("old1", "1", 600_000L);
        c.put("old2", "2", 600_000L);
        c.put("old3", "3", 600_000L);
        // old1 最旧；整组新增 1 键恰好放下：淘汰 old1。
        GroupResult r = c.commitGroup("grp-fit",
                List.of(GroupMember.insert("new1", "n", 600_000L)));
        assertEquals(List.of("old1"), r.evictedKeys(), "必须按现有 LRU 顺序淘汰最旧非成员键");
        assertEquals(3, r.liveEntries());
        assertNull(c.get("old1"));
        assertEquals("n", c.get("new1"));
        assertEquals(1L, c.metrics().evictionCount(), "淘汰计数跨整组操作连续");
        log.info("容量刚好放下判定 group=grp-fit keys=[new1] 依据=新增1需1空位,LRU最旧非成员old1被淘汰 "
                + "恢复结果=整体成功 live=3 evictedKeys=[old1]");
    }

    @Test
    void capacityOneShortEvenAfterEvictingAllNonMembers() {
        // 边界"差一个"：容量3，已满 old1/old2/old3；组内 UPDATE old1(占用不增) +
        // INSERT n1,n2,n3（3个新键）。puts=4 > max=3，即使把非成员 old2/old3 全部
        // 淘汰，组最终仍需 4 个位置、差 1 个 => 整组拒绝，且 old1/old2/old3 一个都不许先被淘汰。
        DurableKvCache c = boot("g-short.wal", 3);
        c.put("old1", "1", 600_000L);
        c.put("old2", "2", 600_000L);
        c.put("old3", "3", 600_000L);
        long seqBefore = peekNextSeq(c);
        CapacityExceededException ex = assertThrows(CapacityExceededException.class, () ->
                c.commitGroup("grp-short", List.of(
                        GroupMember.update("old1", "11", 600_000L),
                        GroupMember.insert("n1", "1", 600_000L),
                        GroupMember.insert("n2", "2", 600_000L),
                        GroupMember.insert("n3", "3", 600_000L))));
        assertEquals("GROUP_TOO_LARGE", ex.reason());
        assertTrue(ex.getMessage().contains("仍差 1 个位置"), ex.getMessage());
        assertEquals("1", c.get("old1"), "容量拒绝不得误淘汰/误改任何键");
        assertEquals("2", c.get("old2"));
        assertEquals("3", c.get("old3"));
        assertEquals(0L, c.metrics().evictionCount());
        assertEquals(seqBefore, peekNextSeq(c), "容量拒绝不得消耗序号");
        log.info("差一个判定 group=grp-short keys=old1更新+n1/n2/n3插入 依据=组写入4键>上限3, "
                + "淘汰全部2个非成员仍差1 恢复结果=507拒绝, old1/2/3均存活, 零副作用");
    }

    @Test
    void capacityGroupLargerThanLimitWithNoRoomAtAll() {
        // 边界"整组大于上限"：容量 2，缓存为空；组内 3 个 INSERT，
        // 没有任何其它键可供淘汰，整组自身即超限。
        DurableKvCache empty = boot("g-huge.wal", 2);
        CapacityExceededException ex = assertThrows(CapacityExceededException.class, () ->
                empty.commitGroup("grp-huge", List.of(
                        GroupMember.insert("h1", "1", 600_000L),
                        GroupMember.insert("h2", "2", 600_000L),
                        GroupMember.insert("h3", "3", 600_000L))));
        assertEquals("GROUP_TOO_LARGE", ex.reason());
        assertTrue(ex.getMessage().contains("没有任何其它键可供淘汰"), ex.getMessage());
        assertEquals(0, empty.metrics().liveEntries());
        log.info("整组大于上限判定 group=grp-huge keys=h1,h2,h3 依据=组写入键3>上限2且缓存空 "
                + "恢复结果=507拒绝, 零淘汰零副作用");
    }

    @Test
    void deletesFreeCapacityAndNeverEvictGroupMembers() {
        // 容量 3 满：old1(LRU最旧),keep,old3。整组 DELETE keep + INSERT n1,n2：
        // 最终 3-1+2=4, need=1；LRU 最旧是 old1（非成员），淘汰 old1 后刚好 live=3。
        DurableKvCache c = boot("g-delcap.wal", 3);
        c.put("old1", "1", 600_000L);
        c.put("keep", "k", 600_000L);
        c.put("old3", "3", 600_000L);
        c.get("keep"); // 刷新 keep，使 old1 保持最旧
        GroupResult r = c.commitGroup("grp-delcap", List.of(
                GroupMember.delete("keep"),
                GroupMember.insert("n1", "a", 600_000L),
                GroupMember.insert("n2", "b", 600_000L)));
        assertTrue(r.deletedKeys().contains("keep"));
        assertEquals(List.of("old1"), r.evictedKeys(), "成员键即使处于 LRU 最旧位置也不得被淘汰");
        assertEquals(3, r.liveEntries());
        assertNull(c.get("keep"));
        assertEquals("a", c.get("n1"));
        assertEquals("b", c.get("n2"));
        log.info("删除释放容量判定 group=grp-delcap keys=keep删除,n1/n2插入 依据=删除释放1+LRU淘汰非成员old1 "
                + "恢复结果=成功 live=3, 组内键未被误淘汰");
    }

    private static long peekNextSeq(DurableKvCache c) {
        // 通过指标不直接暴露 nextSeq；用一次失败操作前后的 groupStatus 无法取序号，
        // 故用反射读取内部字段仅用于断言（测试代码）。
        try {
            var f = DurableKvCache.class.getDeclaredField("nextSeq");
            f.setAccessible(true);
            return (long) f.get(c);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void walFailureMidFrameLeavesNoSideEffectAndNoDanglingFrame() {
        // 故障注入：第 3 次 append（TXBEG=1, 第一个成员 PUT=2, 第二个成员 PUT=3）抛异常，
        // 模拟组帧写到一半 force 失败。要求：整组拒绝、内存零改动、WAL 被截回提交前、
        // 重启恢复看不到任何未闭合帧（组查询为 null，旧键原样存活）。
        CacheProperties p = new CacheProperties();
        p.setDataDir(tempDir.toString());
        p.setMaxEntries(0);
        FileWalLog real = new FileWalLog(tempDir.toString(), "g-walfail.wal");
        PersistenceLog failing = new PersistenceLog() {
            private int appends;

            @Override
            public void append(com.github.h3yow.durablekvcachejava.core.OperationRecord record) {
                appends++;
                if (appends == 3) {
                    throw new com.github.h3yow.durablekvcachejava.cache.CacheExceptions
                            .PersistenceException("注入故障: 模拟组帧中途 force 失败", null);
                }
                real.append(record);
            }

            @Override
            public Iterable<com.github.h3yow.durablekvcachejava.core.OperationRecord> readAll() {
                return real.readAll();
            }

            @Override
            public Iterable<com.github.h3yow.durablekvcachejava.persistence.WalEntry>
                    readAllWithOffsets() {
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
        DurableKvCache c = new DurableKvCache(failing, new LruEvictionTracker(), p);
        new RecoveryService(failing, c).recover();
        c.put("pre", "keep", 600_000L);
        long seqBefore = peekNextSeq(c);

        assertThrows(com.github.h3yow.durablekvcachejava.cache.CacheExceptions
                .PersistenceException.class, () -> c.commitGroup("grp-walfail", List.of(
                        GroupMember.insert("x1", "1", 600_000L),
                        GroupMember.insert("x2", "2", 600_000L))));
        assertEquals("keep", c.get("pre"), "WAL 失败不得改动任何内存状态");
        assertNull(c.get("x1"));
        assertNull(c.get("x2"));
        assertNull(c.groupStatus("grp-walfail"), "失败组不得登记");
        assertEquals(seqBefore, peekNextSeq(c), "序号必须回退，不残留空洞");

        // 重启：WAL 已截回，恢复结果等同于该组从未存在；随后可重新提交成功。
        DurableKvCache c2 = boot("g-walfail.wal", 0);
        assertEquals("keep", c2.get("pre"));
        assertNull(c2.get("x1"));
        assertNull(c2.groupStatus("grp-walfail"));
        var retry = c2.commitGroup("grp-walfail", List.of(
                GroupMember.insert("x1", "1", 600_000L),
                GroupMember.insert("x2", "2", 600_000L)));
        assertTrue(retry.committed());
        assertEquals("1", c2.get("x1"));
        log.info("WAL中途失败判定 group=grp-walfail 依据=第3条append注入异常 "
                + "恢复结果=整组拒绝, pre保留, WAL截回, 重启无未闭合帧, 重试成功");
    }

    @Test
    void expiredGroupMembersArePurgedAndMixingWithSingleKeyApisIsConsistent() throws Exception {
        DurableKvCache c = boot("g-ttl.wal", 0);
        // 整组里混一个短 TTL 键。
        c.commitGroup("grp-ttl", List.of(
                GroupMember.insert("long1", "L1", 600_000L),
                GroupMember.insert("short", "S", 40L)));
        assertEquals("S", c.get("short"));
        Thread.sleep(60);
        // 短键过期后，对它的整组 UPDATE 必须以前置失败拒绝；长键不受影响。
        GroupStaleException ex = assertThrows(GroupStaleException.class, () ->
                c.commitGroup("grp-ttl2", List.of(
                        GroupMember.update("short", "S2", 600_000L),
                        GroupMember.update("long1", "L2", 600_000L))));
        assertTrue(ex.staleKeys().contains("short"));
        assertEquals("L1", c.get("long1"), "前置失败不得改动同组其它键");
        // 单键接口看到的状态与整组接口一致：short 已不存在，可以用 INSERT 重新建组。
        assertNull(c.get("short"));
        var rebuilt = c.commitGroup("grp-ttl3", List.of(
                GroupMember.insert("short", "S3", 600_000L),
                GroupMember.update("long1", "L3", 600_000L)));
        assertTrue(rebuilt.committed());
        assertEquals("S3", c.get("short"));
        assertEquals("L3", c.get("long1"));
        log.info("整组TTL判定 group=grp-ttl2/grp-ttl3 keys=short/long1 "
                + "依据=short到期后UPDATE前置失败(412), 单键GET同样miss, INSERT重建成功 "
                + "恢复结果=长键全程不受影响, 单键/整组结论一致");
    }

    @Test
    void singleKeyOperationsInterleaveWithGroupsAndShareTheSameTotalOrder() {
        DurableKvCache c = boot("g-mix-api.wal", 0);
        c.commitGroup("gm1", List.of(GroupMember.insert("k", "group-v1", 600_000L)));
        // 单键重写一个由整组创建的键。
        c.put("k", "single-v2", 600_000L);
        assertEquals("single-v2", c.get("k"));
        // 整组再整体更新：结论以最后一次操作（无论入口）为准。
        var r = c.commitGroup("gm2", List.of(GroupMember.update("k", "group-v3", 600_000L)));
        assertEquals(List.of("k"), r.updatedKeys());
        assertEquals("group-v3", c.get("k"));
        // 单键删除后，整组 DELETE/UPDATE 都会前置失败（键已不存在）。
        assertTrue(c.delete("k"));
        assertThrows(GroupStaleException.class, () ->
                c.commitGroup("gm3", List.of(GroupMember.delete("k"))));
        log.info("单键/整组混用判定 keys=k 依据=同一状态机单锁全序 恢复结果="
                + "group-v1->single-v2->group-v3->单键删除, 后续整组DELETE正确412");
    }
}
