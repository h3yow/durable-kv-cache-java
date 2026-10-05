package com.github.h3yow.durablekvcachejava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Path;

/**
 * Spring 上下文冒烟加载。数据目录用本轮独有的隔离目录，
 * 不触碰工作区默认 ./data，避免被历史残留 WAL 影响。
 */
@SpringBootTest
public class ApplicationTests {

    private static final Path DATA_DIR = IsolatedTestStorage.freshDir("context-smoke");

    @DynamicPropertySource
    static void cacheProps(DynamicPropertyRegistry registry) {
        registry.add("kv-cache.data-dir", () -> DATA_DIR.toString());
    }

    @Test
    public void contentLoad() {
    }
}
