package com.github.h3yow.durablekvcachejava.tests;

import com.github.h3yow.durablekvcachejava.Application;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 进程异常重启模拟：不做任何"优雅导出"，仅依赖 WAL，关闭并重建整个 Spring 上下文。
 */
class ProcessRestartSimulationTest {

    private static final Logger log = LoggerFactory.getLogger(ProcessRestartSimulationTest.class);

    @TempDir
    Path tempDir;

    @Test
    void dataSurvivesFullContextRestart() {
        String dir = tempDir.resolve("single").toString();
        String unique = "single-" + java.util.UUID.randomUUID();

        // —— 进程 1：启动、写入（返回成功即已 force 落盘）、直接关闭 ——
        try (ConfigurableApplicationContext ctx1 = new SpringApplicationBuilder(Application.class)
                .properties("kv-cache.data-dir=" + dir,
                        "spring.main.web-application-type=none",
                        "logging.pattern.console=",
                        "test.context.marker=" + unique + "-p1")
                .run("--kv-cache.data-dir=" + dir)) {
            com.github.h3yow.durablekvcachejava.cache.KeyValueCache cache1 =
                    ctx1.getBean(com.github.h3yow.durablekvcachejava.cache.KeyValueCache.class);
            cache1.put("durable", "i-survive", 600_000L);
            cache1.put("willdelete", "x", 600_000L);
            cache1.delete("willdelete");
            assertEquals("i-survive", cache1.get("durable"));
            log.info("重启模拟[进程1] key=durable 判定=PUT成功(WAL force) 恢复结果=即将直接关闭上下文");
        }

        // —— 进程 2：同目录冷启动，恢复期间不暴露服务；恢复后立即可读 ——
        try (ConfigurableApplicationContext ctx2 = new SpringApplicationBuilder(Application.class)
                .properties("kv-cache.data-dir=" + dir,
                        "spring.main.web-application-type=none",
                        "logging.pattern.console=",
                        "test.context.marker=" + unique + "-p2")
                .run("--kv-cache.data-dir=" + dir)) {
            com.github.h3yow.durablekvcachejava.cache.KeyValueCache cache2 =
                    ctx2.getBean(com.github.h3yow.durablekvcachejava.cache.KeyValueCache.class);
            assertEquals("i-survive", cache2.get("durable"),
                    "已确认写入在进程重启后必须可读");
            assertEquals(null, cache2.get("willdelete"), "删除结论也必须跨进程保持");
            assertNotNull(cache2.metrics());
            log.info("重启模拟[进程2] key=durable 依据=WAL重放恢复 恢复结果=durable=i-survive, "
                    + "willdelete=miss, state={}", cache2.metrics().state());
        }
    }

    @Test
    void confirmedGroupSurvivesFullContextRestartAsAWhole() {
        String dir = tempDir.resolve("group").toString();
        String unique = "group-" + java.util.UUID.randomUUID();

        // —— 进程 1：整组写入会话三件套（资料/权限/计数）+ 另一个整组做更新与删除 ——
        try (ConfigurableApplicationContext ctx1 = new SpringApplicationBuilder(Application.class)
                .properties("kv-cache.data-dir=" + dir,
                        "spring.main.web-application-type=none",
                        "logging.pattern.console=",
                        "test.context.marker=" + unique + "-p1")
                .run("--kv-cache.data-dir=" + dir)) {
            com.github.h3yow.durablekvcachejava.cache.KeyValueCache cache1 =
                    ctx1.getBean(com.github.h3yow.durablekvcachejava.cache.KeyValueCache.class);
            cache1.commitGroup("grp-session-open", java.util.List.of(
                    new com.github.h3yow.durablekvcachejava.cache.GroupMember(
                            "sess:1:profile",
                            com.github.h3yow.durablekvcachejava.cache.GroupAction.INSERT,
                            "alice", 600_000L),
                    new com.github.h3yow.durablekvcachejava.cache.GroupMember(
                            "sess:1:perms",
                            com.github.h3yow.durablekvcachejava.cache.GroupAction.INSERT,
                            "read,write", 600_000L),
                    new com.github.h3yow.durablekvcachejava.cache.GroupMember(
                            "sess:1:count",
                            com.github.h3yow.durablekvcachejava.cache.GroupAction.INSERT,
                            "1", 600_000L)));
            cache1.commitGroup("grp-session-bump", java.util.List.of(
                    new com.github.h3yow.durablekvcachejava.cache.GroupMember(
                            "sess:1:profile",
                            com.github.h3yow.durablekvcachejava.cache.GroupAction.UPDATE,
                            "alice2", 600_000L),
                    new com.github.h3yow.durablekvcachejava.cache.GroupMember(
                            "sess:1:count",
                            com.github.h3yow.durablekvcachejava.cache.GroupAction.UPDATE,
                            "2", 600_000L)));
            // 直接 kill：不做任何优雅关闭。
        }
        // —— 进程 2：整组必须完整恢复，不能只恢复其中一两个键 ——
        try (ConfigurableApplicationContext ctx2 = new SpringApplicationBuilder(Application.class)
                .properties("kv-cache.data-dir=" + dir,
                        "spring.main.web-application-type=none",
                        "logging.pattern.console=",
                        "test.context.marker=" + unique + "-p2")
                .run("--kv-cache.data-dir=" + dir)) {
            com.github.h3yow.durablekvcachejava.cache.KeyValueCache cache2 =
                    ctx2.getBean(com.github.h3yow.durablekvcachejava.cache.KeyValueCache.class);
            assertEquals("alice2", cache2.get("sess:1:profile"),
                    "组内 UPDATE 结论必须跨进程完整恢复");
            assertEquals("read,write", cache2.get("sess:1:perms"),
                    "同组的其它键必须一起恢复（不允许半组）");
            assertEquals("2", cache2.get("sess:1:count"));
            java.util.Map<String, String> snap = cache2.snapshot();
            org.junit.jupiter.api.Assertions.assertEquals(java.util.Set.of(
                            "sess:1:profile", "sess:1:perms", "sess:1:count"), snap.keySet(),
                    "恢复后只应有本组三个键，实际: " + snap);
            assertEquals(3, cache2.metrics().liveEntries());
            assertNotNull(cache2.groupStatus("grp-session-open"));
            assertNotNull(cache2.groupStatus("grp-session-bump"));
            // 相同 groupId 重放在真实上下文里也幂等：计数不叠加。
            var dup = cache2.commitGroup("grp-session-open", java.util.List.of(
                    new com.github.h3yow.durablekvcachejava.cache.GroupMember(
                            "sess:1:profile",
                            com.github.h3yow.durablekvcachejava.cache.GroupAction.INSERT,
                            "alice", 600_000L),
                    new com.github.h3yow.durablekvcachejava.cache.GroupMember(
                            "sess:1:perms",
                            com.github.h3yow.durablekvcachejava.cache.GroupAction.INSERT,
                            "read,write", 600_000L),
                    new com.github.h3yow.durablekvcachejava.cache.GroupMember(
                            "sess:1:count",
                            com.github.h3yow.durablekvcachejava.cache.GroupAction.INSERT,
                            "1", 600_000L)));
            assertTrue(dup.duplicate(), "跨进程重复提交相同组必须被识别为幂等");
            assertEquals("alice2", cache2.get("sess:1:profile"), "幂等重复不得回退新值");
            assertEquals(3, cache2.metrics().liveEntries(), "重复恢复/重复提交不得叠加条数");
            log.info("重启模拟[整组] group=grp-session-open/grp-session-bump "
                    + "依据=TXBEG..TXEND闭合帧重放 恢复结果=三键完整(alice2/read,write/2), "
                    + "重复提交幂等 live=3");
        }
    }
}
