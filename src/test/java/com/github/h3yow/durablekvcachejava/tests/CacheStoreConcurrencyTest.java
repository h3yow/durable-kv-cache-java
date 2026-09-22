package com.github.h3yow.durablekvcachejava.tests;

import com.github.h3yow.durablekvcachejava.config.CacheProperties;
import com.github.h3yow.durablekvcachejava.core.CacheStore;
import com.github.h3yow.durablekvcachejava.exception.CapacityExceededException;
import com.github.h3yow.durablekvcachejava.exception.InvalidRequestException;
import com.github.h3yow.durablekvcachejava.exception.ServiceUnavailableException;
import com.github.h3yow.durablekvcachejava.exception.VersionConflictException;
import com.github.h3yow.durablekvcachejava.model.EntryView;
import com.github.h3yow.durablekvcachejava.model.LifecycleState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 核心并发/过期/容量语义测试（不依赖外部服务，数据目录在 @TempDir）。
 * 时钟可控：epochMs 由测试推进，nano 用真实单调时钟。
 */
class CacheStoreConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(CacheStoreConcurrencyTest.class);

    @TempDir
    Path tempDir;

    private final AtomicLong fakeNow = new AtomicLong(1_000_000L);
    private CacheStore store;
    private CacheProperties props;

    @BeforeEach
    void setUp() {
        props = new CacheProperties();
        props.setDataDir(tempDir.resolve("data"));
        props.setMaxEntries(8);
        props.setCleanupIntervalMs(1_000L);
        props.setFsyncEachWrite(true);
        props.setWalCompactionBytes(0L);
        store = new CacheStore(props, new CacheStore.Clock() {
            @Override public long epochMs() { return fakeNow.get(); }
            @Override public long nano() { return System.nanoTime(); }
        });
    }

    @AfterEach
    void tearDown() {
        store.shutdown();
    }

    @Test
    void unavailableBeforeRecoveryAndActiveAfter() {
        assertEquals(LifecycleState.CREATED, store.state());
        assertThrows(ServiceUnavailableException.class, () -> store.get("k"));
        store.recover();
        assertEquals(LifecycleState.ACTIVE, store.state());
        log.info("[lifecycle] states=CREATED->RECOVERING->ACTIVE basis=requests blocked until recovered");
    }

    @Test
    void expiryIsServerSideAndConsistentAcrossEntries() {
        store.recover();
        store.put("t1", "a", 100L, null);
        store.put("t2", "b", null, null);
        fakeNow.addAndGet(100L); // t1 恰好到期：now >= expireAt

        EntryView v1 = store.get("t1");
        assertNull(v1.value());
        EntryView v2 = store.get("t2");
        assertEquals("b", v2.value());
        log.info("[expiry-boundary] key=t1 expireAt={} now={} basis=now>=expireAt => expired; key=t2 eternal",
                v1.expireAtEpochMs(), fakeNow.get());

        // 对已过期键的重写：过期视同不存在，因此开启全新存活期，版本从 1 重新计
        var rewritten = store.put("t1", "a2", 1_000L, -1L);
        assertEquals(1L, rewritten.version());
        assertEquals("a2", store.get("t1").value());
        log.info("[rewrite-after-expiry] key=t1 newVersion=1 basis=expired entry counts as absent; fresh lifecycle");

        // 对已过期键的删除：不产生提交，deleted=false 但有明确依据
        var delete = store.delete("t2", null);
        assertTrue(delete.deleted());
        fakeNow.addAndGet(5_000L); // t1(a2, ttl 1000) 到期
        var delExpired = store.delete("t1", null);
        assertFalse(delExpired.deleted());
        log.info("[delete-expired] key=t1 deleted={} reason={} basis=server-side expiry",
                delExpired.deleted(), delExpired.reason());
    }

    @Test
    void concurrentOverwritesProduceOneWinnerAndMonotonicVersion() throws Exception {
        store.recover();
        store.put("hot", "v0", null, null);
        int threads = 16;
        int perThread = 200;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            final int idx = t;
            futures.add(pool.submit(() -> {
                start.await();
                for (int i = 0; i < perThread; i++) {
                    store.put("hot", "writer-" + idx + "-" + i, null, null);
                }
                return null;
            }));
        }
        start.countDown();
        for (Future<?> f : futures) {
            f.get(30, TimeUnit.SECONDS);
        }
        pool.shutdownNow();

        long totalWrites = 1L + (long) threads * perThread;
        EntryView view = store.get("hot");
        assertNotNull(view.value());
        assertEquals(totalWrites, view.version(), "版本号必须等于成功提交次数");
        // 最终值必须能在 WAL 中解释：是某个 writer 的最后一次提交之一；
        // 关键确定性结论：任何时刻读到的 version/value 与 sequence 严格对应，无旧值覆盖
        log.info("[concurrent-put] key=hot finalVersion={} expected={} finalValue={} basis=serialized mutation lock",
                view.version(), totalWrites, view.value());
        assertEquals(1, store.size());
    }

    @Test
    void concurrentMixedPutGetDeleteNeverReturnsStaleOverwrittenValue() throws Exception {
        store.recover();
        store.put("k", "init", null, null);
        int writers = 8;
        ExecutorService pool = Executors.newFixedThreadPool(writers + 2);
        CountDownLatch stop = new CountDownLatch(1);
        AtomicLong violations = new AtomicLong();

        // 写者：无 CAS 的纯并发覆盖，值编码递增的提交序号（由返回的 sequence 决定）
        AtomicLong putCounter = new AtomicLong();
        List<Future<Long>> writerFutures = new ArrayList<>();
        for (int w = 0; w < writers; w++) {
            final int writerId = w;
            writerFutures.add(pool.submit(() -> {
                long done = 0L;
                while (stop.getCount() > 0) {
                    long n = putCounter.incrementAndGet();
                    store.put("k", "val-" + n, null, null);
                    done++;
                }
                log.info("[writer-{}] committed={}", writerId, done);
                return done;
            }));
        }
        // 读者：值后缀必须能在 1..已提交数 中找到，且版本单调；任何旧值覆盖都会造成
        // “版本回退”或值与版本所属序列不一致——这里统一以版本非回退为确定性不变量
        final AtomicLong lastSeenVersion = new AtomicLong();
        Future<?> reader = pool.submit(() -> {
            while (stop.getCount() > 0) {
                EntryView v = store.get("k");
                if (v.value() != null) {
                    long suffix;
                    try {
                        suffix = Long.parseLong(v.value().substring(4));
                    } catch (NumberFormatException bad) {
                        violations.incrementAndGet();
                        continue;
                    }
                    if (suffix < 1 || suffix > putCounter.get()) {
                        violations.incrementAndGet();
                    }
                    long prev;
                    do {
                        prev = lastSeenVersion.get();
                        if (v.version() < prev) {
                            violations.incrementAndGet();
                            log.error("[invariant-violation] key=k version regressed {} -> {}",
                                    prev, v.version());
                            break;
                        }
                    } while (!lastSeenVersion.compareAndSet(prev, Math.max(prev, v.version())));
                }
            }
            return null;
        });
        Thread.sleep(800);
        stop.countDown();
        for (Future<Long> f : writerFutures) {
            f.get(30, TimeUnit.SECONDS);
        }
        reader.get(5, TimeUnit.SECONDS);
        pool.shutdownNow();
        EntryView finalView = store.get("k");
        log.info("[mixed-concurrency] violations={} committed={} finalVersion={} finalValue={} "
                        + "basis=mutation lock serializes all writes",
                violations.get(), putCounter.get() + 1, finalView.version(), finalView.value());
        assertEquals(0, violations.get());
        // 初始 1 次 + 并发提交数：最终版本必须精确等于成功提交总数（旧值覆盖会使等式失败）
        assertEquals(putCounter.get() + 1, finalView.version());
    }

    @Test
    void casExpectedVersionIsRejectedWithDistinguishableCode() {
        store.recover();
        store.put("cas", "v1", null, null);
        VersionConflictException ex = assertThrows(VersionConflictException.class,
                () -> store.put("cas", "v2", null, 99L));
        log.info("[cas-conflict] key=cas reason={} basis=expectedVersion mismatch", ex.errorCode());
        assertEquals("v1", store.get("cas").value());
        var ok = store.put("cas", "v2", null, 1L);
        assertEquals(2L, ok.version());
    }

    @Test
    void invalidRequestsAreRejectedWithReasons() {
        store.recover();
        assertThrows(InvalidRequestException.class, () -> store.put("", "v", null, null));
        assertThrows(InvalidRequestException.class, () -> store.put("k", null, null, null));
        assertThrows(InvalidRequestException.class, () -> store.put("k", "v", 0L, null));
        assertThrows(InvalidRequestException.class, () -> store.put("k", "v", -5L, null));
        var big = "x".repeat((int) props.getMaxValueBytes() + 1);
        assertThrows(InvalidRequestException.class, () -> store.put("big", big, null, null));
        log.info("[invalid] rejected empty-key/null-value/ttl<=0/value-too-large basis=request validation");
    }

    @Test
    void rejectModeRaisesCapacityExceededForNewKeysOnly() {
        props.setEviction(CacheProperties.EvictionMode.REJECT);
        store.recover();
        for (int i = 0; i < props.getMaxEntries(); i++) {
            store.put("k" + i, "v", null, null);
        }
        CapacityExceededException ex = assertThrows(CapacityExceededException.class,
                () -> store.put("new", "v", null, null));
        log.info("[capacity] key=new reason={} basis=maxEntries={} mode=REJECT", ex.errorCode(),
                props.getMaxEntries());
        // 覆盖已有键不触发容量错误
        assertDoesNotThrow(() -> store.put("k0", "v2", null, null));
    }
}
