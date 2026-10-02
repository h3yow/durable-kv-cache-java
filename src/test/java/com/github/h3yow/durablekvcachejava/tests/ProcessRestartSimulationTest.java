package com.github.h3yow.durablekvcachejava.tests;

import com.github.h3yow.durablekvcachejava.Application;
import com.github.h3yow.durablekvcachejava.core.GroupCommitResult;
import com.github.h3yow.durablekvcachejava.core.GroupOperation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 进程异常重启模拟：不做任何"优雅导出"，仅依赖 WAL，关闭并重建整个 Spring 上下文。
 *
 * <p>注意：data-dir 通过命令行参数 {@code --kv-cache.data-dir=...} 传入（最高优先级），
 * 不能用 builder 的 default properties——后者优先级低于 jar 内 application.properties，
 * 覆盖会被忽略。每个用例使用 @TempDir 下独立子目录，互不污染。</p>
 */
class ProcessRestartSimulationTest {

    private static final Logger log = LoggerFactory.getLogger(ProcessRestartSimulationTest.class);

    @TempDir
    Path tempDir;

    private ConfigurableApplicationContext boot(String dir) {
        return new SpringApplicationBuilder(Application.class)
                .run("--kv-cache.data-dir=" + dir,
                        "--spring.main.web-application-type=none",
                        "--logging.pattern.console=");
    }

    @Test
    void dataSurvivesFullContextRestart() {
        String dir = tempDir.resolve("single-key").toString();

        // —— 进程 1：启动、写入（返回成功即已 force 落盘）、直接关闭 ——
        try (ConfigurableApplicationContext ctx1 = boot(dir)) {
            var cache1 = ctx1.getBean(com.github.h3yow.durablekvcachejava.cache.KeyValueCache.class);
            cache1.put("durable", "i-survive", 600_000L);
            cache1.put("willdelete", "x", 600_000L);
            cache1.delete("willdelete");
            assertEquals("i-survive", cache1.get("durable"));
            log.info("重启模拟[进程1] key=durable 判定=PUT成功(WAL force) 恢复结果=即将直接关闭上下文");
        }

        // —— 进程 2：同目录冷启动，恢复期间不暴露服务；恢复后立即可读 ——
        try (ConfigurableApplicationContext ctx2 = boot(dir)) {
            var cache2 = ctx2.getBean(com.github.h3yow.durablekvcachejava.cache.KeyValueCache.class);
            assertEquals("i-survive", cache2.get("durable"),
                    "已确认写入在进程重启后必须可读");
            assertEquals(null, cache2.get("willdelete"), "删除结论也必须跨进程保持");
            assertNotNull(cache2.metrics());
            log.info("重启模拟[进程2] key=durable 依据=WAL重放恢复 恢复结果=durable=i-survive, "
                    + "willdelete=miss, state={}", cache2.metrics().state());
        }
    }

    @Test
    void committedGroupSurvivesFullContextRestartAsOneUnit() {
        String dir = tempDir.resolve("group").toString();
        String run = Long.toHexString(System.nanoTime());
        String gid = "ctx-sess-" + run;

        // 进程 1：提交一个包含 PUT 与 DELETE 的整组，返回成功即表示整帧已 force 落盘。
        try (ConfigurableApplicationContext ctx1 = boot(dir)) {
            var cache1 = ctx1.getBean(com.github.h3yow.durablekvcachejava.cache.KeyValueCache.class);
            cache1.put("grp:old:" + run, "x", 600_000L);
            var r = cache1.commitGroup(gid, List.of(
                    GroupOperation.put("grp:profile:" + run, "alice", 600_000L),
                    GroupOperation.put("grp:perm:" + run, "admin", 600_000L),
                    GroupOperation.delete("grp:old:" + run)));
            assertEquals(GroupCommitResult.Outcome.COMMITTED, r.outcome());
            log.info("整组重启模拟[进程1] 组={} keys=grp:profile/grp:perm/DELETE(grp:old) 判定=COMMITTED", gid);
        }

        // 进程 2：冷启动，整组必须完整恢复，不能只恢复其中一部分。
        try (ConfigurableApplicationContext ctx2 = boot(dir)) {
            var cache2 = ctx2.getBean(com.github.h3yow.durablekvcachejava.cache.KeyValueCache.class);
            assertEquals("alice", cache2.get("grp:profile:" + run), "整组成员 profile 必须随组恢复");
            assertEquals("admin", cache2.get("grp:perm:" + run), "整组成员 perm 必须随组恢复");
            assertNull(cache2.get("grp:old:" + run), "整组内删除结论必须跨进程保持");
            assertEquals(1L, cache2.metrics().groupSuccessCount(), "恢复后恰好存在一个已确认整组");

            // 同 groupId 同内容重试：DUPLICATE，绝不叠加。
            var dup = cache2.commitGroup(gid, List.of(
                    GroupOperation.put("grp:profile:" + run, "alice", 600_000L),
                    GroupOperation.put("grp:perm:" + run, "admin", 600_000L),
                    GroupOperation.delete("grp:old:" + run)));
            assertTrue(dup.duplicate());
            assertEquals("alice", cache2.get("grp:profile:" + run));
            log.info("整组重启模拟[进程2] 组={} 依据=GBEGIN..GCOMMIT整段重放 "
                    + "恢复结果=profile/perm存活, old已删, 重试=DUPLICATE", gid);
        }
    }
}
