package com.github.h3yow.durablekvcachejava.tests;

import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.InvalidRequestException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.StateConflictException;
import com.github.h3yow.durablekvcachejava.cache.KeyValueCache;
import com.github.h3yow.durablekvcachejava.config.CacheProperties;
import com.github.h3yow.durablekvcachejava.core.DurableKvCache;
import com.github.h3yow.durablekvcachejava.eviction.LruEvictionTracker;
import com.github.h3yow.durablekvcachejava.persistence.FileWalLog;
import com.github.h3yow.durablekvcachejava.persistence.PersistenceLog;
import com.github.h3yow.durablekvcachejava.recovery.RecoveryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 状态机测试：正常/边界/失败路径、并发读写与过期竞争。
 */
class DurableKvCacheTest {

    private static final Logger log = LoggerFactory.getLogger(DurableKvCacheTest.class);

    @TempDir
    Path tempDir;

    private record Harness(DurableKvCache cache, PersistenceLog wal, CacheProperties props) {
    }

    private Harness newCache(int maxEntries) {
        CacheProperties p = new CacheProperties();
        p.setDataDir(tempDir.toString());
        p.setMaxEntries(maxEntries);
        FileWalLog wal = new FileWalLog(tempDir.toString(), "cache.wal");
        DurableKvCache cache = new DurableKvCache(wal, new LruEvictionTracker(), p);
        new RecoveryService(wal, cache).recover();
        return new Harness(cache, wal, p);
    }

    @Test
    void putGetDeleteBasic() {
        Harness h = newCache(10);
        h.cache().put("k1", "v1", 5000L);
        assertEquals("v1", h.cache().get("k1"));
        assertTrue(h.cache().delete("k1"));
        assertNull(h.cache().get("k1"), "删除后必须读不到");
        assertFalse(h.cache().delete("k1"), "重复删除幂等且返回 false");
        log.info("基础读写删判定 key=k1 依据=PUT后命中/DELETE后miss 恢复结果=无(在线操作)");
    }

    @Test
    void expiredValueNeverReturned() throws InterruptedException {
        Harness h = newCache(10);
        h.cache().put("k1", "v1", 40L);
        Thread.sleep(70);
        assertNull(h.cache().get("k1"), "已过期值不得返回");
        // 过期后重写：新值立即生效，旧值不得复活。
        h.cache().put("k1", "v2", 5000L);
        assertEquals("v2", h.cache().get("k1"));
        log.info("过期判定 key=k1 依据=now>=expireAt 结果=miss；重写后新值v2命中");
    }

    @Test
    void lruEvictionOrderIsDeterministic() {
        Harness h = newCache(2);
        h.cache().put("a", "1", 5000L);
        h.cache().put("b", "2", 5000L);
        assertEquals("1", h.cache().get("a"), "访问 a，使 b 成为最旧");
        var r = h.cache().put("c", "3", 5000L);
        assertTrue(r.evicted());
        assertEquals("b", r.evictedKey(), "LRU 必须淘汰最久未使用的 b");
        assertNull(h.cache().get("b"));
        assertEquals("1", h.cache().get("a"));
        assertEquals("3", h.cache().get("c"));
        assertEquals(1L, h.cache().metrics().evictionCount());
        log.info("淘汰判定 key=b 依据=LRU顺序(a被GET刷新,b最旧) 结果=b被淘汰 live=2");
    }

    @Test
    void rejectsIllegalRequestsWithDistinctReasons() {
        Harness h = newCache(10);
        assertThrows(InvalidRequestException.class, () -> h.cache().put("", "v", 10L));
        assertThrows(InvalidRequestException.class, () -> h.cache().put("k", null, 10L));
        assertThrows(InvalidRequestException.class, () -> h.cache().put("k", "v", 0L));
        assertThrows(InvalidRequestException.class, () -> h.cache().put("k", "v", -1L));
        assertThrows(InvalidRequestException.class, () -> h.cache().put("k", "v", null));
        CacheProperties small = new CacheProperties();
        small.setDataDir(tempDir.toString());
        small.setMaxEntries(10);
        small.setMaxValueBytes(3);
        FileWalLog wal = new FileWalLog(tempDir.toString(), "small.wal");
        DurableKvCache c = new DurableKvCache(wal, new LruEvictionTracker(), small);
        new RecoveryService(wal, c).recover();
        InvalidRequestException ex = assertThrows(InvalidRequestException.class,
                () -> c.put("k", "hello", 10L));
        assertTrue(ex.getMessage().contains("value 超长"));
        log.info("非法请求判定 依据=空key/null值/非正ttl/超长value 结果=全部InvalidRequestException("
                + ex.getMessage() + ")");
    }

    @Test
    void accessBeforeReadyIsRejected() {
        CacheProperties p = new CacheProperties();
        p.setDataDir(tempDir.toString());
        FileWalLog wal = new FileWalLog(tempDir.toString(), "noready.wal");
        DurableKvCache cache = new DurableKvCache(wal, new LruEvictionTracker(), p);
        assertThrows(StateConflictException.class, () -> cache.get("k"));
        assertThrows(StateConflictException.class, () -> cache.put("k", "v", 10L));
        log.info("恢复屏障判定 key=k 依据=未markReady时状态=RECOVERING 结果=StateConflictException");
    }

    @Test
    void concurrentOverwritersNeverObserveStaleOrNull() throws Exception {
        Harness h = newCache(0); // 不限制容量
        int threads = 16;
        int rounds = 200;
        h.cache().put("hot", "init", 60_000L);
        Set<String> legal = new HashSet<>();
        legal.add("init");
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);
        Map<Integer, Throwable> errors = new ConcurrentHashMap<>();
        for (int t = 0; t < threads; t++) {
            final int id = t;
            legal.add("v" + id);
            pool.submit(() -> {
                try {
                    barrier.await();
                    for (int i = 0; i < rounds; i++) {
                        if (id % 2 == 0) {
                            h.cache().put("hot", "v" + id, 60_000L);
                        } else {
                            String got = h.cache().get("hot");
                            assertTrue(legal.contains(got),
                                    "读到非任何写入者的值(旧值/脏值): " + got);
                        }
                    }
                } catch (Throwable e) {
                    errors.put(id, e);
                }
            });
        }
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        assertTrue(errors.isEmpty(), "并发过程不得出现异常: " + errors);
        String finalValue = h.cache().get("hot");
        assertTrue(legal.contains(finalValue));
        log.info("并发判定 key=hot 依据=单锁全序 结果={}个写入线程完成, 最终值={} 属于已写入集合, 无旧值/null",
                threads, finalValue);
    }

    @Test
    void expiryRewriteDeleteRaceHasOneDeterministicOutcome() throws Exception {
        // 8 组：过期GET、重写PUT、删除DELETE 同时冲击同一短TTL键，
        // 任何调度下结论必须自洽：绝不返回旧值 v1。
        for (int trial = 0; trial < 8; trial++) {
            Harness h = newCache(10);
            h.cache().put("race", "v1", 30L);
            ExecutorService pool = Executors.newFixedThreadPool(3);
            CyclicBarrier barrier = new CyclicBarrier(3);
            Future<String> getAfter = pool.submit(() -> {
                barrier.await();
                Thread.sleep(45); // 确保越过 v1 过期点
                return h.cache().get("race");
            });
            Future<?> writer = pool.submit(() -> {
                barrier.await();
                Thread.sleep(45);
                h.cache().put("race", "v2", 60_000L);
                return null;
            });
            Future<Boolean> deleter = pool.submit(() -> {
                barrier.await();
                Thread.sleep(45);
                return h.cache().delete("race");
            });
            String g = getAfter.get(10, TimeUnit.SECONDS);
            writer.get(10, TimeUnit.SECONDS);
            deleter.get(10, TimeUnit.SECONDS);
            pool.shutdown();
            assertTrue(g == null || "v2".equals(g),
                    "trial " + trial + ": 过期竞争中返回了旧值 v1 -> " + g);
            String settled = h.cache().get("race");
            assertTrue(settled == null || "v2".equals(settled));
            log.info("过期竞争 trial={} key=race 依据=串行顺序 瞬时GET={} 最终={} (均不含旧值v1)",
                    trial, g, settled);
        }
    }

    @Test
    void metricsReflectCapacityAndEviction() {
        Harness h = newCache(1);
        h.cache().put("a", "1", 5000L);
        h.cache().put("b", "2", 5000L); // 淘汰 a
        KeyValueCache.Metrics m = h.cache().metrics();
        assertEquals(1, m.maxEntries());
        assertEquals(1, m.liveEntries());
        assertEquals(1, m.evictionCount());
        log.info("指标判定 依据=容量1连续PUT 结果={}", m);
    }
}
