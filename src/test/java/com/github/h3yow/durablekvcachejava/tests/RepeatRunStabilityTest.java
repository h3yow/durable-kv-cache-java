package com.github.h3yow.durablekvcachejava.tests;

import com.github.h3yow.durablekvcachejava.cache.GroupMember;
import com.github.h3yow.durablekvcachejava.cache.GroupResult;
import com.github.h3yow.durablekvcachejava.config.CacheProperties;
import com.github.h3yow.durablekvcachejava.core.DurableKvCache;
import com.github.h3yow.durablekvcachejava.core.OperationRecord;
import com.github.h3yow.durablekvcachejava.core.OperationType;
import com.github.h3yow.durablekvcachejava.eviction.LruEvictionTracker;
import com.github.h3yow.durablekvcachejava.persistence.FileWalLog;
import com.github.h3yow.durablekvcachejava.recovery.RecoveryService;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 反复执行稳定性：同一套用例连续多轮执行、执行顺序变化，结论必须一致。
 *
 * <p>每一"轮"都通过 {@link IsolatedTestStorage} 分配全新的独立数据目录，
 * 轮与轮之间、用例与用例之间不共享任何存储；上一轮被强杀留下的残留
 * （未闭合整组帧）既不会泄漏进下一轮，也不会让本轮的全新提交被误判为重复。</p>
 */
class RepeatRunStabilityTest {

    private static final Logger log = LoggerFactory.getLogger(RepeatRunStabilityTest.class);

    private DurableKvCache boot(Path dir) {
        CacheProperties p = new CacheProperties();
        p.setDataDir(dir.toString());
        p.setMaxEntries(0);
        FileWalLog wal = new FileWalLog(dir.toString(), "cache.wal");
        DurableKvCache cache = new DurableKvCache(wal, new LruEvictionTracker(), p);
        new RecoveryService(wal, cache).recover();
        return cache;
    }

    /** 固定场景：两组整组提交 + 各自的重复提交，返回每一步判定结论。 */
    private List<String> scenario(Path dir, String runTag) {
        DurableKvCache cache = boot(dir);
        List<String> verdicts = new ArrayList<>();

        GroupResult g1 = cache.commitGroup("rerun-grp-1", List.of(
                GroupMember.insert("rr:profile", "alice", 600_000L),
                GroupMember.insert("rr:perms", "admin", 600_000L)));
        verdicts.add("grp1=" + (g1.committed() && !g1.duplicate() ? "OK_FIRST" : "UNEXPECTED"));

        GroupResult g1dup = cache.commitGroup("rerun-grp-1", List.of(
                GroupMember.insert("rr:profile", "alice", 600_000L),
                GroupMember.insert("rr:perms", "admin", 600_000L)));
        verdicts.add("grp1dup=" + (g1dup.duplicate() ? "OK_DUPLICATE" : "UNEXPECTED"));

        GroupResult g2 = cache.commitGroup("rerun-grp-2", List.of(
                GroupMember.update("rr:perms", "admin2", 600_000L),
                GroupMember.insert("rr:count", "1", 600_000L)));
        verdicts.add("grp2=" + (g2.committed() && !g2.duplicate() ? "OK_FIRST" : "UNEXPECTED"));

        GroupResult g2dup = cache.commitGroup("rerun-grp-2", List.of(
                GroupMember.update("rr:perms", "admin2", 600_000L),
                GroupMember.insert("rr:count", "1", 600_000L)));
        verdicts.add("grp2dup=" + (g2dup.duplicate() ? "OK_DUPLICATE" : "UNEXPECTED"));

        verdicts.add("live=" + cache.metrics().liveEntries());
        verdicts.add("profile=" + cache.get("rr:profile"));
        verdicts.add("perms=" + cache.get("rr:perms"));
        verdicts.add("count=" + cache.get("rr:count"));

        log.info("反复执行[{}] dir={} group=rerun-grp-1 keys=rr:profile,rr:perms "
                        + "group=rerun-grp-2 keys=rr:perms,rr:count 判定依据=首提OK/重放OK_DUPLICATE "
                        + "结论={}",
                runTag, dir, verdicts);
        return verdicts;
    }

    @Test
    void consecutiveRunsOfSameScenarioGiveIdenticalVerdicts() {
        // 同一套用例连续执行 3 轮，每轮全新独立目录，结论必须逐位一致。
        List<String> run1 = scenario(IsolatedTestStorage.freshDir("rerun-1"), "第1轮");
        List<String> run2 = scenario(IsolatedTestStorage.freshDir("rerun-2"), "第2轮");
        List<String> run3 = scenario(IsolatedTestStorage.freshDir("rerun-3"), "第3轮");

        assertEquals(run1, run2, "第2轮结论必须与第1轮完全一致（不受上一轮落盘状态影响）");
        assertEquals(run2, run3, "第3轮结论必须与第2轮完全一致");
        assertTrue(run1.contains("grp1=OK_FIRST"),
                "每一轮的首个整组提交都必须被判定为本轮全新的第一次提交: " + run1);
        assertTrue(run1.contains("grp1dup=OK_DUPLICATE"),
                "只有调用方真的重复提交同一组内容才允许幂等结论: " + run1);
        log.info("反复执行判定 依据=3轮独立存储结论逐位比对 恢复结果=三轮完全一致 {}", run1);
    }

    @Test
    void residualFromKilledRunDoesNotLeakIntoNextRun() {
        // —— 第 1 轮：确认一组后，在第二组提交中途被"强杀"（TXBEG+成员已落盘，无 TXEND）——
        Path dirA = IsolatedTestStorage.freshDir("killed-run");
        DurableKvCache first = boot(dirA);
        GroupResult ok = first.commitGroup("killed-grp-ok", List.of(
                GroupMember.insert("ka", "1", 600_000L),
                GroupMember.insert("kb", "2", 600_000L)));
        assertTrue(ok.committed() && !ok.duplicate());
        try (FileWalLog w = new FileWalLog(dirA.toString(), "cache.wal")) {
            long seq = w.nextSeq();
            w.append(new OperationRecord(seq, OperationType.TXBEG,
                    "killed-grp-half", "I:h1\nI:h2\n", 2, System.currentTimeMillis(),
                    "killed-grp-half"));
            w.append(new OperationRecord(seq + 1, OperationType.PUT,
                    "h1", "v1", 600_000L, System.currentTimeMillis(), "killed-grp-half"));
            // 强杀：h2 与 TXEND 永远不落盘。
        }
        log.info("残留构造 dir={} group=killed-grp-half keys=h1,h2 依据=TXBEG+部分成员落盘,"
                + "无TXEND=未确认提交", dirA);

        // —— 第 2 轮：全新目录重跑同一套完整用例，必须从干净状态开始 ——
        Path dirB = IsolatedTestStorage.freshDir("next-run");
        List<String> clean = scenario(dirB, "残留后新一轮");
        assertTrue(clean.contains("grp1=OK_FIRST"),
                "新一轮的首提不得因上一轮残留被误判重复: " + clean);

        // —— 第 1 轮目录自身重启：残留半组被确定丢弃，全新提交正常生效 ——
        DurableKvCache recovered = boot(dirA);
        assertEquals("1", recovered.get("ka"), "已确认组必须完整恢复");
        assertEquals("2", recovered.get("kb"));
        assertNull(recovered.get("h1"), "未确认半组不得恢复");
        assertNull(recovered.groupStatus("killed-grp-half"), "未确认组不得登记");

        GroupResult retry = recovered.commitGroup("killed-grp-half", List.of(
                GroupMember.insert("h1", "v1", 600_000L),
                GroupMember.insert("h2", "v2", 600_000L)));
        assertTrue(retry.committed() && !retry.duplicate(),
                "磁盘上只有未确认残留时，全新提交必须生效并返回真实首提结果，不得误判重复");
        assertEquals("v1", recovered.get("h1"));
        assertEquals("v2", recovered.get("h2"));

        GroupResult dup = recovered.commitGroup("killed-grp-ok", List.of(
                GroupMember.insert("ka", "1", 600_000L),
                GroupMember.insert("kb", "2", 600_000L)));
        assertTrue(dup.duplicate(), "真正重复提交已确认组才允许幂等结论");
        assertEquals(4, recovered.metrics().liveEntries(),
                "ka,kb + h1,h2 = 4，残留不计入、重复不叠加");
        log.info("残留恢复判定 dir={} group=killed-grp-ok(已确认)/killed-grp-half(残留) "
                + "依据=无TXEND整帧丢弃 恢复结果=ka,kb保留; h1残留不计入; "
                + "同内容全新提交OK_FIRST; 真重复提交OK_DUPLICATE; live=4", dirA);
    }

    @Test
    void executionOrderDoesNotChangeVerdicts() {
        // 同一组操作、两种执行顺序，各自独立目录，对应步骤结论与最终状态必须一致。
        Path dirX = IsolatedTestStorage.freshDir("order-x");
        Path dirY = IsolatedTestStorage.freshDir("order-y");

        List<String> orderX = runInOrder(dirX, "顺序X[A,B,put,dupA]",
                List.of("A", "B", "PUT", "DUP_A"));
        List<String> orderY = runInOrder(dirY, "顺序Y[B,put,A,dupA]",
                List.of("B", "PUT", "A", "DUP_A"));

        // 按步骤名（而非执行先后）对齐比较。
        assertEquals(orderX, orderY, "执行顺序变化不得改变任一步骤的判定结论");

        DurableKvCache cx = boot(dirX);
        DurableKvCache cy = boot(dirY);
        Map<String, String> sx = cx.snapshot();
        Map<String, String> sy = cy.snapshot();
        assertEquals(sx, sy, "重启恢复后两种顺序的最终存活集必须一致");
        assertEquals(Map.of("oa1", "a1", "oa2", "a2", "ob1", "b1", "oc", "c"), sx);
        log.info("顺序无关判定 依据=步骤名对齐比对+恢复后快照比对 恢复结果=两目录最终状态一致 {}", sx);
    }

    /** 按给定顺序执行步骤，返回按步骤名排序的判定结论（便于跨顺序对齐）。 */
    private List<String> runInOrder(Path dir, String tag, List<String> steps) {
        DurableKvCache cache = boot(dir);
        List<String> verdicts = new ArrayList<>();
        for (String step : steps) {
            switch (step) {
                case "A" -> {
                    GroupResult r = cache.commitGroup("ord-a", List.of(
                            GroupMember.insert("oa1", "a1", 600_000L),
                            GroupMember.insert("oa2", "a2", 600_000L)));
                    verdicts.add("A=" + (r.committed() && !r.duplicate() ? "OK_FIRST" : "UNEXPECTED"));
                }
                case "B" -> {
                    GroupResult r = cache.commitGroup("ord-b", List.of(
                            GroupMember.insert("ob1", "b1", 600_000L)));
                    verdicts.add("B=" + (r.committed() && !r.duplicate() ? "OK_FIRST" : "UNEXPECTED"));
                }
                case "PUT" -> {
                    cache.put("oc", "c", 600_000L);
                    verdicts.add("PUT=OK");
                }
                case "DUP_A" -> {
                    GroupResult r = cache.commitGroup("ord-a", List.of(
                            GroupMember.insert("oa1", "a1", 600_000L),
                            GroupMember.insert("oa2", "a2", 600_000L)));
                    verdicts.add("DUP_A=" + (r.duplicate() ? "OK_DUPLICATE" : "UNEXPECTED"));
                }
                default -> throw new IllegalArgumentException("未知步骤: " + step);
            }
        }
        verdicts.sort(String::compareTo);
        log.info("顺序执行[{}] dir={} group=ord-a keys=oa1,oa2 group=ord-b keys=ob1 key=oc "
                + "判定依据=首提OK/重放OK_DUPLICATE 结论={}", tag, dir, verdicts);
        return verdicts;
    }
}
