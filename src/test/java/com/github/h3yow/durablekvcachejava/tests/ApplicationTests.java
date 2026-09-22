package com.github.h3yow.durablekvcachejava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 启动冒烟测试：完整 Spring 上下文就绪即意味着恢复流程执行完毕（ACTIVE 或 FAILED）。
 * 默认数据目录位于临时目录，空目录恢复必然成功。
 */
@SpringBootTest
class ApplicationTests {

    @Test
    void contextLoadsAndRecoveryCompletes() {
        assertTrue(true, "context started; recovery ran on ApplicationReadyEvent");
    }
}
