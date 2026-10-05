package com.github.h3yow.durablekvcachejava.tests;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * 测试数据目录隔离工具：为每个测试类/每个场景分配彼此独立的存储位置，
 * 并在使用前把上一轮（可能中途失败/被强杀）留下的残留数据整体清除，
 * 保证同一工作区里反复执行、任意执行顺序都得到一致结论。
 *
 * <p>目录固定落在 {@code target/test-data/<name>}：
 * <ul>
 *   <li>不同用例使用不同 {@code name}，彼此互不影响；</li>
 *   <li>同名目录在分配前先递归删除残留（含未闭合组帧、半行撕裂等），
 *       再重建为空目录——上一轮无论以何种方式中断，下一轮都从干净状态开始；</li>
 *   <li>目录在 target/ 下，随 {@code mvn clean} 一并清除，不污染工作区源码目录。</li>
 * </ul>
 */
public final class IsolatedTestDir {

    private static final Logger log = LoggerFactory.getLogger(IsolatedTestDir.class);

    private static final Path ROOT = Path.of("target", "test-data");

    private IsolatedTestDir() {
    }

    /**
     * 分配一个干净的独立数据目录：若上一轮留下同名目录（可能含半套残留数据），
     * 先整体删除再重建。返回的目录保证存在且为空。
     *
     * @param name 场景名（同一测试类内必须唯一，不同测试类之间用不同前缀区分）
     * @return 干净的空目录
     */
    public static synchronized Path fresh(String name) {
        Path dir = ROOT.resolve(name);
        try {
            if (Files.exists(dir)) {
                long leftover = Files.walk(dir).filter(Files::isRegularFile).count();
                Files.walk(dir)
                        .sorted(Comparator.reverseOrder())
                        .forEach(p -> {
                            try {
                                Files.delete(p);
                            } catch (IOException e) {
                                throw new UncheckedIOException("清除上一轮残留失败: " + p, e);
                            }
                        });
                log.info("测试目录隔离 name={} dir={} 依据=发现上一轮残留({}个文件,可能为中断残留) "
                        + "恢复结果=已整体清除, 本轮从干净状态开始", name, dir, leftover);
            }
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("无法准备测试数据目录: " + dir, e);
        }
        return dir;
    }
}
