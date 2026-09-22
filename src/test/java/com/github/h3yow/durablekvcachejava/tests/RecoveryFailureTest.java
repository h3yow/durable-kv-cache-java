package com.github.h3yow.durablekvcachejava.tests;

import com.github.h3yow.durablekvcachejava.config.CacheProperties;
import com.github.h3yow.durablekvcachejava.core.CacheStore;
import com.github.h3yow.durablekvcachejava.exception.RecoveryException;
import com.github.h3yow.durablekvcachejava.exception.ServiceUnavailableException;
import com.github.h3yow.durablekvcachejava.model.LifecycleState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 恢复失败路径：损坏必须给出可区分原因并使服务进入 FAILED，
 * 之后任何数据请求都被 SERVICE_UNAVAILABLE 拒绝，绝不静默继续。
 */
class RecoveryFailureTest {

    private static final Logger log = LoggerFactory.getLogger(RecoveryFailureTest.class);

    @TempDir
    Path tempDir;

    private CacheStore store;

    private CacheProperties props() {
        CacheProperties p = new CacheProperties();
        p.setDataDir(tempDir.resolve("data"));
        p.setMaxEntries(10);
        p.setFsyncEachWrite(true);
        p.setWalCompactionBytes(0L);
        p.setCleanupIntervalMs(60_000L);
        return p;
    }

    private CacheStore storeWithFixedClock(CacheProperties p) {
        AtomicLong now = new AtomicLong(5_000_000L);
        return new CacheStore(p, new CacheStore.Clock() {
            @Override public long epochMs() { return now.get(); }
            @Override public long nano() { return System.nanoTime(); }
        });
    }

    @AfterEach
    void tearDown() {
        if (store != null && store.state() == LifecycleState.ACTIVE) {
            store.shutdown();
        }
    }

    @Test
    void corruptedCommittedWalLineFailsWithChecksumMismatch() throws Exception {
        CacheProperties p = props();
        store = storeWithFixedClock(p);
        store.recover();
        store.put("k1", "v1", null, null);
        store.put("k2", "v2", null, null);
        store.shutdown();

        // 篡改已提交行（第一行数据行中的一个字符），保持换行存在 -> CRC 不匹配
        Path wal = Files.list(p.getDataDir()).filter(x -> x.getFileName().toString().endsWith(".log"))
                .findFirst().orElseThrow();
        List<String> lines = Files.readAllLines(wal, StandardCharsets.UTF_8);
        String first = lines.get(0);
        // 找到 value 段（最后一段）翻转一个字符
        int cut = first.lastIndexOf('|') + 1;
        String valueField = first.substring(cut);
        char c = valueField.charAt(0);
        char d = c == 'A' ? 'B' : 'A';
        String tampered = first.substring(0, cut) + d + valueField.substring(1);
        Files.writeString(wal, tampered + "\n" + lines.get(1) + "\n", StandardCharsets.UTF_8);

        store = storeWithFixedClock(props());
        RecoveryException ex = assertThrows(RecoveryException.class, () -> store.recover());
        assertEquals("CHECKSUM_MISMATCH", ex.reason());
        assertEquals(LifecycleState.FAILED, store.state());
        assertThrows(ServiceUnavailableException.class, () -> store.get("k1"));
        log.info("[failure-checksum] reason={} state=FAILED requests=503 basis=committed line tampered",
                ex.reason());
    }

    @Test
    void danglingLastLineAfterCrashIsToleratedButCommittedLinesRemain() throws Exception {
        CacheProperties p = props();
        store = storeWithFixedClock(p);
        store.recover();
        store.put("safe", "v", null, null);
        store.shutdown();

        Path wal = Files.list(p.getDataDir()).filter(x -> x.getFileName().toString().endsWith(".log"))
                .findFirst().orElseThrow();
        String committed = Files.readString(wal, StandardCharsets.UTF_8);
        // 模拟崩溃残尾：已提交行 + 没有换行符的半条记录
        Files.writeString(wal, committed + "DKVC|1|999|garbage-partial", StandardCharsets.UTF_8);

        store = storeWithFixedClock(props());
        store.recover();
        assertEquals(LifecycleState.ACTIVE, store.state());
        assertEquals("v", store.get("safe").value());
        log.info("[dangling-tail] key=safe recovered; partial last line ignored as uncommitted "
                + "basis=no trailing newline => never fsync-confirmed");
    }

    @Test
    void unrecognizedFileInDataDirFailsWithUnsupportedFormat() throws Exception {
        CacheProperties p = props();
        store = storeWithFixedClock(p);
        store.recover();
        store.put("k", "v", null, null);
        store.shutdown();

        Files.writeString(p.getDataDir().resolve("random-file.dat"), "xxx", StandardCharsets.UTF_8);
        store = storeWithFixedClock(props());
        RecoveryException ex = assertThrows(RecoveryException.class, () -> store.recover());
        assertEquals("UNSUPPORTED_FORMAT", ex.reason());
        log.info("[failure-stranger-file] reason={} basis=only snapshot.db / wal-*.log allowed", ex.reason());
    }

    @Test
    void corruptedSnapshotFailsWithSnapshotCorrupt() throws Exception {
        CacheProperties p = props();
        store = storeWithFixedClock(p);
        store.recover();
        store.put("snap1", "v1", null, null);
        store.put("snap2", "v2", null, null);
        store.checkpoint(); // 产生 snapshot.db 并归档旧 WAL
        store.put("snap3", "v3", null, null);
        store.shutdown();

        Path snap = p.getDataDir().resolve("snapshot.db");
        String content = Files.readString(snap, StandardCharsets.UTF_8);
        String[] lines = content.split("\n", -1);
        // 破坏头部记录计数：records=2 -> records=9，实际数据校验会失败
        String badHeader = lines[0].replace("records=2", "records=9");
        StringBuilder sb = new StringBuilder(badHeader).append('\n');
        for (int i = 1; i < lines.length; i++) {
            sb.append(lines[i]).append('\n');
        }
        Files.writeString(snap, sb.toString(), StandardCharsets.UTF_8);

        store = storeWithFixedClock(props());
        RecoveryException ex = assertThrows(RecoveryException.class, () -> store.recover());
        assertEquals("SNAPSHOT_CORRUPT", ex.reason());
        log.info("[failure-snapshot] reason={} state=FAILED basis=header/body inconsistency", ex.reason());
    }

    @Test
    void illegalConfigurationFailsBeforeRecovery() {
        CacheProperties p = props();
        p.setMaxEntries(0);
        store = storeWithFixedClock(p);
        var ex = assertThrows(com.github.h3yow.durablekvcachejava.exception.IllegalConfigurationException.class,
                () -> store.recover());
        assertEquals(LifecycleState.FAILED, store.state());
        log.info("[failure-config] code={} basis=validate() before touching data", ex.errorCode());
    }
}
