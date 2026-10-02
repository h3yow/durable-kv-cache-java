package com.github.h3yow.durablekvcachejava.tests;

import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.RecoveryException;
import com.github.h3yow.durablekvcachejava.core.OperationRecord;
import com.github.h3yow.durablekvcachejava.core.OperationType;
import com.github.h3yow.durablekvcachejava.persistence.FileWalLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * WAL 正常/边界/失败路径单测。
 */
class FileWalLogTest {

    private static final Logger log = LoggerFactory.getLogger(FileWalLogTest.class);

    @TempDir
    Path tempDir;

    @Test
    void appendAndReadRoundTrip() {
        FileWalLog wal = new FileWalLog(tempDir.toString(), "c.wal");
        wal.append(new OperationRecord(1, OperationType.PUT, "k1", "v1|中文", 5000L, 100L));
        wal.append(new OperationRecord(2, OperationType.DELETE, "k1", null, 0L, 200L));

        List<OperationRecord> all = new ArrayList<>();
        wal.readAll().forEach(all::add);
        assertEquals(2, all.size());
        assertEquals("v1|中文", all.get(0).value());
        assertEquals(OperationType.DELETE, all.get(1).type());
        assertEquals(3L, wal.nextSeq());
        log.info("WAL往返判定 key=k1 依据=两条记录顺序读回且字段一致 恢复结果=nextSeq=3");
    }

    @Test
    void truncatedLastLineIsToleratedAndTrimmed() throws IOException {
        FileWalLog wal = new FileWalLog(tempDir.toString(), "c.wal");
        wal.append(new OperationRecord(1, OperationType.PUT, "k1", "v1", 5000L, 100L));
        wal.close();
        // 模拟崩溃时半行落盘：追加一段无换行的垃圾。
        Files.writeString(Path.of(tempDir.toString(), "c.wal"),
                "2|PUT|a|b|10|200|999", java.nio.file.StandardOpenOption.APPEND);

        FileWalLog wal2 = new FileWalLog(tempDir.toString(), "c.wal");
        List<OperationRecord> all = new ArrayList<>();
        wal2.readAll().forEach(all::add);
        assertEquals(1, all.size(), "截断半行必须被忽略");
        // 截断数据应被截除，文件以完整换行结尾。
        String content = Files.readString(Path.of(tempDir.toString(), "c.wal"), StandardCharsets.UTF_8);
        assertTrue(content.endsWith("\n") && !content.contains("999"), "残留半行必须被截除");
        assertEquals(2L, wal2.nextSeq());
        log.info("WAL截断判定 key=k1 依据=末行无换行视为崩溃残留 恢复结果=保留1条并截除残留 nextSeq=2");
    }

    @Test
    void checksumCorruptionFailsRecoveryWithDistinctError() throws IOException {
        FileWalLog wal = new FileWalLog(tempDir.toString(), "c.wal");
        wal.append(new OperationRecord(1, OperationType.PUT, "k1", "v1", 5000L, 100L));
        wal.close();
        Path p = Path.of(tempDir.toString(), "c.wal");
        String content = Files.readString(p, StandardCharsets.UTF_8);
        // 篡改值字段但保留同样长度结构，使 CRC 不匹配。
        String corrupted = content.replace("djE", "djI"); // v1 -> v2 的 base64
        Files.writeString(p, corrupted);

        FileWalLog wal2 = new FileWalLog(tempDir.toString(), "c.wal");
        RecoveryException ex = assertThrows(RecoveryException.class, wal2::readAll);
        assertTrue(ex.getMessage().contains("校验和"), "必须给出可区分原因: " + ex.getMessage());
        log.info("WAL损坏判定 key=k1 依据=CRC校验不匹配 恢复结果=RecoveryException(" + ex.getMessage() + ")");
    }

    @Test
    void committedGroupFramesRoundTripAndUncommittedGroupIsTrimmed() throws IOException {
        FileWalLog wal = new FileWalLog(tempDir.toString(), "g.wal");
        wal.append(new OperationRecord(1, OperationType.PUT, "solo", "s", 5000L, 100L));
        wal.append(OperationRecord.frame(2, OperationType.GBEGIN, "g1", 2, 200L));
        wal.append(new OperationRecord(3, OperationType.PUT, "a", "1", 5000L, 200L, "g1"));
        wal.append(new OperationRecord(4, OperationType.DELETE, "solo", null, 0L, 200L, "g1"));
        wal.append(OperationRecord.frame(5, OperationType.GCOMMIT, "g1", 0, 200L));

        List<OperationRecord> all = new ArrayList<>();
        wal.readAll().forEach(all::add);
        assertEquals(5, all.size(), "GBEGIN/GCOMMIT 也占用序号并被读回");
        assertEquals(OperationType.GBEGIN, all.get(1).type());
        assertEquals("g1", all.get(1).groupId());
        assertEquals(OperationType.GCOMMIT, all.get(4).type());
        assertEquals(6L, wal.nextSeq());

        // 强杀残留：未闭合整组（GBEGIN + 1 成员 + 无换行半行）必须整体截除到最后提交点 seq=5。
        wal.append(OperationRecord.frame(6, OperationType.GBEGIN, "g2", 1, 300L));
        wal.close();
        Files.writeString(Path.of(tempDir.toString(), "g.wal"),
                "7|PUT|" + java.util.Base64.getEncoder().encodeToString("z".getBytes())
                        + "|" + java.util.Base64.getEncoder().encodeToString("9".getBytes())
                        + "|5000|300|" + java.util.Base64.getEncoder().encodeToString("g2".getBytes()),
                java.nio.file.StandardOpenOption.APPEND);

        FileWalLog wal2 = new FileWalLog(tempDir.toString(), "g.wal");
        List<OperationRecord> after = new ArrayList<>();
        wal2.readAll().forEach(after::add);
        assertEquals(5, after.size(), "未闭合整组（含 GBEGIN）必须被整体截除");
        assertEquals(6L, wal2.nextSeq(), "下一个序号必须接在已提交的 GCOMMIT 之后");
        String content = Files.readString(Path.of(tempDir.toString(), "g.wal"), StandardCharsets.UTF_8);
        assertTrue(content.endsWith("\n") && !content.contains("g2"), "未闭合组必须被物理截除");
        log.info("WAL整组判定 组=g1(闭合往返)/g2(强杀未提交) 依据=帧占序号+无GCOMMIT整段截除 恢复结果=6条,nextSeq=6");
    }

    @Test
    void orphanFrameRecordOutsideGroupFailsRecovery() throws IOException {
        FileWalLog wal = new FileWalLog(tempDir.toString(), "orphan.wal");
        wal.append(new OperationRecord(1, OperationType.PUT, "a", "1", 5000L, 100L));
        wal.close();
        // 直接构造一条 CRC 合法但"带 groupId 且不在帧内"的完整记录（8 段，末段为 CRC）。
        String gidB64 = java.util.Base64.getEncoder().encodeToString("gx".getBytes());
        long crc = FileWalLog.checksum(new OperationRecord(2, OperationType.PUT, "b", "2", 5000L, 200L, "gx"));
        String line = "2|PUT|" + java.util.Base64.getEncoder().encodeToString("b".getBytes())
                + "|" + java.util.Base64.getEncoder().encodeToString("2".getBytes())
                + "|5000|200|" + gidB64 + "|" + crc + "\n";
        Files.writeString(Path.of(tempDir.toString(), "orphan.wal"), line,
                java.nio.file.StandardOpenOption.APPEND);
        FileWalLog wal2 = new FileWalLog(tempDir.toString(), "orphan.wal");
        RecoveryException ex = assertThrows(RecoveryException.class, wal2::readAll);
        assertTrue(ex.getMessage().contains("GBEGIN"), "必须明确是帧结构错误: " + ex.getMessage());
        log.info("WAL孤儿成员判定 groupId=gx 依据=有groupId无帧 恢复结果={}", ex.getMessage());
    }

    @Test
    void nonStrictSequenceFailsRecovery() {
        FileWalLog wal = new FileWalLog(tempDir.toString(), "c.wal");
        wal.append(new OperationRecord(1, OperationType.PUT, "k1", "v1", 5000L, 100L));
        wal.append(new OperationRecord(2, OperationType.PUT, "k2", "v2", 5000L, 200L));
        // nextSeq 要求严格递增；手工构造一个新 WAL 实例直接校验。
        RecoveryException ex = assertThrows(RecoveryException.class, () -> {
            FileWalLog same = new FileWalLog(tempDir.toString(), "c.wal");
            // 读到两条正常记录后，nextSeq 本身不抛异常；这里通过篡改序号验证检测能力：
            // 直接校验逻辑已在 RecoveryService 连续性检查中覆盖。
            same.append(new OperationRecord(2, OperationType.PUT, "k3", "v3", 1L, 300L));
            same.nextSeq();
        });
        assertTrue(ex.getMessage().contains("序号"));
        log.info("WAL序号判定 key=k3 依据=序号非严格递增 恢复结果=" + ex.getMessage());
    }
}
