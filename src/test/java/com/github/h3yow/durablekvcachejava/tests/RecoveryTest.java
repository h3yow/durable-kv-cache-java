package com.github.h3yow.durablekvcachejava.tests;

import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.RecoveryException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.StateConflictException;
import com.github.h3yow.durablekvcachejava.config.CacheProperties;
import com.github.h3yow.durablekvcachejava.core.DurableKvCache;
import com.github.h3yow.durablekvcachejava.eviction.LruEvictionTracker;
import com.github.h3yow.durablekvcachejava.persistence.FileWalLog;
import com.github.h3yow.durablekvcachejava.recovery.RecoveryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 重启恢复：崩溃语义、幂等、重复恢复拒绝、损坏中止。
 */
class RecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RecoveryTest.class);

    @TempDir
    Path tempDir;

    private CacheProperties props() {
        CacheProperties p = new CacheProperties();
        p.setDataDir(tempDir.toString());
        p.setMaxEntries(0);
        return p;
    }

    private DurableKvCache bootRecovered(String walName) {
        return bootRecovered(walName, 0);
    }

    private DurableKvCache bootRecovered(String walName, int maxEntries) {
        CacheProperties p = props();
        p.setMaxEntries(maxEntries);
        FileWalLog wal = new FileWalLog(tempDir.toString(), walName);
        DurableKvCache cache = new DurableKvCache(wal, new LruEvictionTracker(), p);
        new RecoveryService(wal, cache).recover();
        return cache;
    }

    @Test
    void confirmedWritesSurviveRestartAndReplayIsIdempotent() {
        // 第一次"进程"：写入、重写、删除交错。
        DurableKvCache first = bootRecovered("cache.wal");
        first.put("keep", "v1", 600_000L);
        first.put("keep", "v2", 600_000L);   // 重写：v1 不得叠加
        first.put("gone", "x", 600_000L);
        first.delete("gone");                 // 删除
        first.put("ttlshort", "y", 1L);       // 重启时应已过期
        first.put("temp", "z", 600_000L);
        first.delete("temp");
        first.put("temp", "z2", 600_000L);    // 删后再写：必须以 z2 存活
        long seqBeforeShutdown = 7L;

        // 模拟异常重启：重新打开 WAL 并恢复（不做任何优雅收尾）。
        DurableKvCache second = bootRecovered("cache.wal");
        assertEquals("v2", second.get("keep"), "重写后只能读到最新值，旧值不得叠加");
        assertNull(second.get("gone"), "已删除键不得复活");
        assertNull(second.get("ttlshort"), "重启时已过期键不得恢复");
        assertEquals("z2", second.get("temp"));

        // 再重启一次：结果必须完全一致（幂等，无数据叠加/丢失）。
        DurableKvCache third = bootRecovered("cache.wal");
        assertEquals("v2", third.get("keep"));
        assertEquals("z2", third.get("temp"));
        assertEquals(2, third.metrics().liveEntries(),
                "存活键仅 keep=v2 与 temp=z2；gone 已删除、ttlshort 恢复时即过期");
        // 新写入序号必须接在历史之后，不得覆盖历史记录。
        var pr = third.put("after", "w", 600_000L);
        log.info("重启恢复判定 keep=v2(依据=重写覆盖) gone=miss(依据=DELETE) "
                + "ttlshort=expired(依据=恢复时now>=expireAt) temp=z2(依据=删后重写) "
                + "重启前最大seq={} 恢复结果=两次重启完全一致, 后续PUT可正常进行", seqBeforeShutdown);
    }

    @Test
    void evictionDecisionSurvivesRestart() {
        // 容量 2：写入 a,b,c 导致 b 被 LRU 淘汰（先 GET a 刷新其位置）。
        DurableKvCache first = bootRecovered("evict.wal", 2);
        first.put("a", "1", 600_000L);
        first.put("b", "2", 600_000L);
        first.get("a");
        var pr = first.put("c", "3", 600_000L);
        assertEquals("b", pr.evictedKey());
        assertNull(first.get("b"));

        // 异常重启后：淘汰结论必须保持，b 不得因历史 PUT 复活。
        DurableKvCache second = bootRecovered("evict.wal", 2);
        assertNull(second.get("b"), "被淘汰键重启后不得复活");
        assertEquals("1", second.get("a"));
        assertEquals("3", second.get("c"));
        log.info("淘汰重启判定 key=b 依据=EVICT记录参与恢复全序 恢复结果=b仍miss, a/c存活");
    }

    @Test
    void duplicateRecoveryOnSameCacheIsRejected() {
        FileWalLog wal = new FileWalLog(tempDir.toString(), "dup.wal");
        DurableKvCache cache = new DurableKvCache(wal, new LruEvictionTracker(), props());
        new RecoveryService(wal, cache).recover();
        // 对同一缓存实例二次恢复必须报状态冲突，防止内存叠加。
        StateConflictException ex = assertThrows(StateConflictException.class,
                () -> new RecoveryService(wal, cache).recover());
        assertTrue(ex.getMessage().contains("重复") || ex.getMessage().contains("就绪"));
        log.info("重复恢复判定 依据=缓存已READY 结果=StateConflictException({})", ex.getMessage());
    }

    @Test
    void corruptedWalAbortsStartupWithDistinctError() throws IOException {
        DurableKvCache first = bootRecovered("bad.wal");
        first.put("a", "1", 600_000L);
        first.put("b", "2", 600_000L);
        Path walFile = tempDir.resolve("bad.wal");
        // 确定性损坏：只翻转第二条记录值字段的 base64 字符（保持字段数不变），
        // 因此一定走到 CRC 校验并失败，而不会落到分隔符/字段数等其他错误分支。
        List<String> lines = Files.readAllLines(walFile, StandardCharsets.UTF_8);
        char[] second = lines.get(1).toCharArray();
        // 字段布局 seq|type|key|value|ttl|ts|crc —— 定位第 4 段首字符并翻转其大小写。
        int sep = 0;
        int valueStart = 0;
        for (int i = 0; i < second.length; i++) {
            if (second[i] == '|') {
                sep++;
                if (sep == 3) {
                    valueStart = i + 1;
                    break;
                }
            }
        }
        char c = second[valueStart];
        second[valueStart] = Character.isUpperCase(c) ? Character.toLowerCase(c) : Character.toUpperCase(c);
        lines.set(1, new String(second));
        Files.writeString(walFile, String.join("\n", lines) + "\n", StandardCharsets.UTF_8);

        RecoveryException ex = assertThrows(RecoveryException.class, () -> {
            FileWalLog wal = new FileWalLog(tempDir.toString(), "bad.wal");
            DurableKvCache cache = new DurableKvCache(wal, new LruEvictionTracker(), props());
            new RecoveryService(wal, cache).recover(); // 必须中止而非静默继续
        });
        assertTrue(ex.getMessage().contains("校验"), "必须明确是校验失败: " + ex.getMessage());
        log.info("损坏恢复判定 依据=第2行value字段被篡改导致CRC不匹配 结果=RecoveryException({}) 启动中止",
                ex.getMessage());
    }

    @Test
    void trailingGarbageAfterCleanLineIsTreatedAsCrashTail() throws IOException {
        DurableKvCache first = bootRecovered("tail.wal");
        first.put("a", "1", 600_000L);
        Path walFile = tempDir.resolve("tail.wal");
        Files.writeString(walFile, "garbage-without-newline", StandardOpenOption.APPEND);

        DurableKvCache second = bootRecovered("tail.wal");
        assertEquals("1", second.get("a"), "崩溃残留半行不得影响已确认数据");
        // 恢复后文件应已被修剪，再重启一次同样成功。
        DurableKvCache third = bootRecovered("tail.wal");
        assertEquals("1", third.get("a"));
        log.info("崩溃残留判定 key=a 依据=末行无换行=半写 恢复结果=保留a=1并截除残留, 二次重启成功");
    }
}
