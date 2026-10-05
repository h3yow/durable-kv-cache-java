package com.github.h3yow.durablekvcachejava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Spring 上下文装配冒烟测试。
 * 数据目录由 {@link IsolatedTestDir} 分配：不使用默认的 ./data，
 * 既不污染工作区，也不受工作区里任何历史 WAL 残留影响。
 */
@SpringBootTest
public class ApplicationTests {

    @DynamicPropertySource
    static void isolatedDataDir(DynamicPropertyRegistry registry) {
        registry.add("kv-cache.data-dir",
                () -> IsolatedTestDir.fresh("it-smoke").toString());
    }

    @Test
    public void contentLoad() {
    }
}
