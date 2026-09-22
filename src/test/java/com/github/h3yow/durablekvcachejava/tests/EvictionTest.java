package com.github.h3yow.durablekvcachejava.tests;

import com.github.h3yow.durablekvcachejava.config.CacheProperties;
import com.github.h3yow.durablekvcachejava.core.CacheStore;
import com.github.h3yow.durablekvcachejava.exception.CapacityExceededException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 容量与淘汰的确定性测试：时钟与访问顺序受控，淘汰目标可精确预测；
 * 淘汰结果同时通过计数与最近淘汰明细可观测，并在重启后不复活。
 */
class EvictionTest {

    private static final Logger log = LoggerFactory.getLogger(EvictionTest.class);

    @TempDir
    Path tempDir;

    private final AtomicLong now = new AtomicLong(1_000L);
    private CacheProperties props;
    private CacheStore store;

    private CacheStore build(CacheProperties p) {
        return new CacheStore(p, new CacheStore.Clock() {
            @Override public long epochMs() { return now.get(); }
            @Override public long nano() { return System.nanoTime(); }
        });
    }

    @BeforeEach
    void setUp() {
        props = new CacheProperties();
        props.setDataDir(tempDir.resolve("data"));
        props.setMaxEntries(3);
        props.setEviction(CacheProperties.EvictionMode.EVICT_LRU);
        props.setFsyncEachWrite(true);
        props.setWalCompactionBytes(0L);
        props.setCleanupIntervalMs(60_000L);
        store = build(props);
        store.recover();
    }

    @AfterEach
    void tearDown() {
        store.shutdown();
    }

    @Test
    void expiredKeyIsReclaimedBeforeLiveLruVictim() {
        // e 先过期；a/b 为存活键
        store.put("e", "ev", 10L, null);
        store.put("a", "av", null, null);
        store.put("b", "bv", null, null);
        store.get("e"); // 让 e 反而是最近访问过的：即使如此，容量回收时过期仍优先于 LRU
        now.addAndGet(10L);
        // 写入前清扫使容量恢复为 2/3，因此新键直接写入，存活键一个都不会被 LRU 淘汰
        store.put("c", "cv", null, null);

        assertNull(store.get("e").value(), "过期键必须在容量判定前被回收，即使它最近被访问");
        assertEquals("av", store.get("a").value());
        assertEquals("bv", store.get("b").value());
        assertEquals("cv", store.get("c").value());
        assertEquals(0L, store.status().totalEvictions(), "过期回收不算 LRU 淘汰");
        assertEquals(3, store.size(), "过期残条目已被清扫，容量恢复给新键");
        assertTrue(store.status().totalExpiredPurges() >= 1L, "写路径清扫应计入过期回收观测");
        long before = store.status().totalExpiredPurges();
        store.purgeExpired();
        assertEquals(before, store.status().totalExpiredPurges(), "再次 purge 幂等，无新增");
        log.info("[eviction-order] expired reclaimed before capacity eviction; live LRU victims=0 "
                + "basis=expiry-first reclamation, then LRU only among live entries");
    }

    @Test
    void liveVictimIsLruThenKeyAscAndObservable() {
        store.put("k1", "1", null, null);
        store.put("k2", "2", null, null);
        store.put("k3", "3", null, null);
        store.get("k3"); // k3 最新；k1/k2 访问时间相同（写入时刻近似），按键名 k1 先淘汰
        store.put("k4", "4", null, null);
        assertNull(store.get("k1").value());
        assertEquals(3, store.size());
        String detail = store.status().recentEvictions().get(0);
        assertTrue(detail.contains("key=k1") && detail.contains("lru-then-key-asc"), detail);
        log.info("[eviction-order] {} basis=lru with key-asc tie-break", detail);

        // 再次填满：现在 k2 是最久未访问
        store.put("k5", "5", null, null);
        assertNull(store.get("k2").value());
        assertEquals(2L, store.status().totalEvictions());
        log.info("[eviction-observable] totalEvictions={} entries={}/{} basis=status counters",
                store.status().totalEvictions(), store.status().entries(), props.getMaxEntries());
    }

    @Test
    void evictionSurvivesRestartAsTombstone() {
        store.put("old", "o", null, null);
        store.put("new1", "n1", null, null);
        store.put("new2", "n2", null, null);
        store.put("new3", "n3", null, null); // old 被淘汰
        assertNull(store.get("old").value());
        store.shutdown();

        CacheStore restarted = build(props);
        restarted.recover();
        assertNull(restarted.get("old").value(), "淘汰 DELETE 记录重放后，被淘汰键不得复活");
        assertEquals(3, restarted.size());
        log.info("[eviction-restart] key=old stays absent after restart basis=eviction appended DELETE seq");
        restarted.shutdown();
    }

    @Test
    void rejectModeKeepsAllEntriesAndReportsDistinctError() {
        props.setEviction(CacheProperties.EvictionMode.REJECT);
        CacheStore rejecting = build(props);
        rejecting.recover();
        rejecting.put("a", "1", null, null);
        rejecting.put("b", "2", null, null);
        rejecting.put("c", "3", null, null);
        CapacityExceededException ex = assertThrows(CapacityExceededException.class,
                () -> rejecting.put("d", "4", null, null));
        assertEquals("CAPACITY_EXCEEDED", ex.errorCode());
        assertEquals(3, rejecting.size());
        assertNull(rejecting.get("d").value());
        // 覆盖已有键不受影响
        rejecting.put("a", "11", null, null);
        assertEquals("11", rejecting.get("a").value());
        log.info("[eviction-reject] rejected key=d code={} basis=mode=REJECT maxEntries=3; overwrite allowed",
                ex.errorCode());
        rejecting.shutdown();
    }
}
