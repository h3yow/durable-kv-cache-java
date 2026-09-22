package com.github.h3yow.durablekvcachejava.tests;

import com.github.h3yow.durablekvcachejava.config.CacheProperties;
import com.github.h3yow.durablekvcachejava.core.CacheStore;
import com.github.h3yow.durablekvcachejava.exception.ServiceUnavailableException;
import com.github.h3yow.durablekvcachejava.model.LifecycleState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 进程异常重启语义：用“同一数据目录上新建 CacheStore”模拟重启；
 * fsyncEachWrite=true 下已确认写入必须存活，未到期键可读，已到期键在恢复时丢弃。
 */
class RecoveryPersistenceTest {

    private static final Logger log = LoggerFactory.getLogger(RecoveryPersistenceTest.class);

    @TempDir
    Path tempDir;

    private final AtomicLong clockMs = new AtomicLong(10_000_000L);
    private CacheStore store;

    private CacheStore newStoreInstance(CacheProperties props) {
        return new CacheStore(props, new CacheStore.Clock() {
            @Override public long epochMs() { return clockMs.get(); }
            @Override public long nano() { return System.nanoTime(); }
        });
    }

    private CacheProperties props() {
        CacheProperties p = new CacheProperties();
        p.setDataDir(tempDir.resolve("data"));
        p.setMaxEntries(100);
        p.setFsyncEachWrite(true);
        p.setWalCompactionBytes(0L);
        p.setCleanupIntervalMs(60_000L);
        return p;
    }

    @AfterEach
    void tearDown() {
        if (store != null) {
            store.shutdown();
        }
    }

    @Test
    void confirmedWritesSurviveRestartIncludingDeleteAndOverwriteOrder() {
        CacheProperties p1 = props();
        store = newStoreInstance(p1);
        store.recover();
        store.put("keep", "v1", null, null);
        store.put("ttl-long", "alive", 10_000L, null);
        store.put("ttl-short", "dead-on-recovery", 100L, null);
        store.put("gone", "x", null, null);
        store.put("keep", "v2", 5_000L, null); // 重写：恢复时只能看到 v2
        var del = store.delete("gone", null);
        assertTrue(del.deleted());
        long walBytes = store.status().currentWalBytes();
        log.info("[pre-restart] wrote 4 keys, overwrote keep, deleted gone; walBytes={}", walBytes);
        store.shutdown(); // 模拟进程终止（句柄关闭；数据已逐条 fsync）

        // 时间推进到：ttl-short 已过期；ttl-long、keep(v2,ttl5000) 仍存活
        clockMs.addAndGet(500L);

        store = newStoreInstance(props());
        store.recover();
        assertEquals("v2", store.get("keep").value(), "恢复后不得读到被覆盖的旧值 v1");
        assertEquals("alive", store.get("ttl-long").value());
        assertNull(store.get("ttl-short").value(), "恢复时点已过期的键必须丢弃");
        assertNull(store.get("gone").value(), "删除墓碑重放后键不得复活");
        log.info("[post-restart] keep=v2(version=2) ttl-long=alive ttl-short=dropped(expired) gone=absent(deleted) "
                + "basis=WAL replay in sequence order; expiry judged at recovery time");

        // 恢复后写入继续单调：keep 重放后为 v2，CAS 2 -> v3
        var after = store.put("keep", "v3", null, 2L);
        assertEquals(3L, after.version());
        log.info("[post-restart-write] key=keep version=3 basis=version continues per-key monotonic");
    }

    @Test
    void restartWithEmptyDirIsCleanRecoveryAndRestartIsRepeatable() {
        store = newStoreInstance(props());
        store.recover();
        assertEquals(0, store.size());
        store.put("a", "1", null, null);
        store.shutdown();

        store = newStoreInstance(props());
        store.recover();
        assertEquals("1", store.get("a").value());
        // “重复恢复”：同一实例再次 recover 必须是明确的状态冲突，而不是叠加
        assertThrows(ServiceUnavailableException.class, () -> store.recover());
        assertEquals(LifecycleState.ACTIVE, store.state());
        assertEquals(1, store.size(), "重复恢复不得造成数据叠加");
        log.info("[repeat-recovery] second recover() on same instance rejected; size stays 1 "
                + "basis=CREATED-only one-shot state machine");
    }

    @Test
    void eternalAndExpiredMixAcrossRestartBoundary() {
        store = newStoreInstance(props());
        store.recover();
        store.put("eternal", "forever", null, null);
        store.put("boundary", "edge", 500L, null);
        store.shutdown();

        clockMs.addAndGet(500L); // 恰好到期
        store = newStoreInstance(props());
        store.recover();
        assertEquals("forever", store.get("eternal").value());
        assertNull(store.get("boundary").value());
        log.info("[boundary-restart] eternal survives; key=boundary expireAt reached at recovery now={} "
                + "basis=now>=expireAt inclusive", clockMs.get());
    }
}
