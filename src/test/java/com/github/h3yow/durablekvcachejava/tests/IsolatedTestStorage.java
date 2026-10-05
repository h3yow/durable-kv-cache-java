package com.github.h3yow.durablekvcachejava.tests;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * 测试存储隔离工具：为每一次运行、每一个用例分配全新的独立数据目录。
 *
 * <p>目录形如 {@code target/test-data/<用例名>-<时间戳>-<随机串>}：</p>
 * <ul>
 *   <li>落在 {@code target/} 下，{@code mvn clean} 可一并清除；</li>
 *   <li>时间戳 + UUID 保证同一工作区连续多轮执行互不复用目录，
 *       上一轮（哪怕被强杀）留下的任何残留状态都不会进入本轮判定；</li>
 *   <li>创建即打印绝对路径，日志可直接看出每个用例操作的是哪一份存储。</li>
 * </ul>
 */
public final class IsolatedTestStorage {

    private static final Logger log = LoggerFactory.getLogger(IsolatedTestStorage.class);
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmssSSS");
    private static final Path BASE = Paths.get("target", "test-data");

    private IsolatedTestStorage() {
    }

    /**
     * 创建一个本轮独有的全新空目录并返回其绝对路径。
     *
     * @param caseName 用例标识（只用于目录名与日志，便于人工核对）
     */
    public static Path freshDir(String caseName) {
        String name = caseName + "-" + TS.format(LocalDateTime.now())
                + "-" + UUID.randomUUID().toString().substring(0, 8);
        Path dir = BASE.resolve(name).toAbsolutePath();
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("无法创建测试隔离目录: " + dir, e);
        }
        log.info("测试存储隔离 case={} dir={} (本轮全新, 不携带任何历史状态)", caseName, dir);
        return dir;
    }
}
