package com.github.h3yow.durablekvcachejava.recovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

/**
 * 启动期入口：真正的恢复在 {@code KeyValueCache} Bean 创建时已完成（未就绪则 Bean 不存在，
 * Web 层不可能对外服务）。Runner 仅输出最终恢复结果，作为本地可观测证据。
 *
 * <p>若恢复失败，异常在 Bean 创建阶段传播，Spring 启动中止，退出码非 0，
 * 与"恢复成功"明确可区分，绝不静默继续。</p>
 */
public final class RecoveryRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(RecoveryRunner.class);

    private final RecoveryService.RecoveryResult result;

    public RecoveryRunner(RecoveryService.RecoveryResult result) {
        this.result = result;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("启动确认: 恢复已完成 结果={}，缓存开始对外服务", result);
    }
}
