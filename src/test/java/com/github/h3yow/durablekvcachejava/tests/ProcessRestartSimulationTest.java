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

/**
 * 进程异常重启模拟：不做任何"优雅导出"，仅依赖 WAL，关闭并重建整个 Spring 上下文。
 */
class ProcessRestartSimulationTest {

    private static final Logger log = LoggerFactory.getLogger(ProcessRestartSimulationTest.class);

    @TempDir
    Path tempDir;

    @Test
    void dataSurvivesFullContextRestart() {
        String dir = tempDir.toString();

        // —— 进程 1：启动、写入（返回成功即已 force 落盘）、直接关闭 ——
        try (ConfigurableApplicationContext ctx1 = new SpringApplicationBuilder(Application.class)
                .properties("kv-cache.data-dir=" + dir,
                        "spring.main.web-application-type=none",
                        "logging.pattern.console=")
                .run()) {
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
                        "logging.pattern.console=")
                .run()) {
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
}
