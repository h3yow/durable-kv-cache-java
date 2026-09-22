package com.github.h3yow.durablekvcachejava.tests;

import com.github.h3yow.durablekvcachejava.config.CacheProperties;
import com.github.h3yow.durablekvcachejava.core.CacheStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 快照压缩：压缩后数据不丢不叠、WAL 轮转、归档文件与快照按 sequence 去重重放。
 */
class CheckpointCompactionTest {

    private static final Logger log = LoggerFactory.getLogger(CheckpointCompactionTest.class);

    @TempDir
    Path tempDir;

    private final AtomicLong now = new AtomicLong(7_000_000L);
    private CacheStore store;

    private CacheStore build(CacheProperties p) {
        return new CacheStore(p, new CacheStore.Clock() {
            @Override public long epochMs() { return now.get(); }
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

    private List<Path> list(String suffix) throws Exception {
        try (Stream<Path> s = Files.list(props().getDataDir())) {
            return s.filter(f -> f.getFileName().toString().endsWith(suffix)).sorted().toList();
        }
    }

    @AfterEach
    void tearDown() {
        if (store != null) {
            store.shutdown();
        }
    }

    @Test
    void checkpointCompactsAndRestartReplaysWithoutDuplication() throws Exception {
        CacheProperties p = props();
        store = build(p);
        store.recover();
        for (int i = 0; i < 10; i++) {
            store.put("k" + i, "v" + i, null, null);
        }
        store.put("k0", "rewritten", null, null); // 快照里只应出现最新版
        store.delete("k9", null);                 // 快照里不应出现
        long oldWal = store.status().currentWalBytes();
        long lastSeq = store.checkpoint();
        log.info("[checkpoint] lastSeq={} oldWalBytes={} newWalBytes={}",
                lastSeq, oldWal, store.status().currentWalBytes());

        assertTrue(Files.exists(p.getDataDir().resolve("snapshot.db")));
        List<Path> activeWals = list(".log");
        List<Path> archived = list(".archived");
        assertEquals(1, activeWals.size(), "轮转后只有一个活动 WAL");
        assertEquals(1, archived.size(), "旧 WAL 被归档而非删除（崩溃点安全）");
        assertTrue(store.status().currentWalBytes() < oldWal);

        // 压缩后继续写入
        store.put("after", "yes", null, null);
        store.shutdown();

        // 重启：快照 + 新 WAL + 归档 WAL 共同重放，重复 sequence 去重，结果不叠加
        CacheStore restarted = build(props());
        restarted.recover();
        assertEquals("rewritten", restarted.get("k0").value());
        assertNull(restarted.get("k9").value());
        assertEquals("yes", restarted.get("after").value());
        for (int i = 1; i < 9; i++) {
            assertEquals("v" + i, restarted.get("k" + i).value());
        }
        assertEquals(10, restarted.size(), "9 个 k1..k8 + k0 + after = 10，删除/重复不叠加");
        log.info("[post-compaction-restart] size=10 k0=rewritten k9=absent after=yes "
                + "basis=snapshot latest records + wal replay with sequence dedupe");
        restarted.shutdown();
    }

    @Test
    void repeatedCheckpointsAreStable() throws Exception {
        store = build(props());
        store.recover();
        store.put("a", "1", null, null);
        store.checkpoint();
        store.put("b", "2", null, null);
        store.checkpoint();
        store.put("c", "3", null, null);
        store.shutdown();

        CacheStore restarted = build(props());
        restarted.recover();
        assertEquals(3, restarted.size());
        assertEquals("1", restarted.get("a").value());
        assertEquals("3", restarted.get("c").value());
        log.info("[double-checkpoint-restart] a/b/c all present basis=multiple checkpoints stay replayable");
        restarted.shutdown();
    }
}
