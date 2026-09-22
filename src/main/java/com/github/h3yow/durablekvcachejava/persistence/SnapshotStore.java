package com.github.h3yow.durablekvcachejava.persistence;

import com.github.h3yow.durablekvcachejava.exception.PersistenceException;
import com.github.h3yow.durablekvcachejava.exception.RecoveryException;
import com.github.h3yow.durablekvcachejava.model.WalRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 快照文件管理（原子发布、崩溃安全）。
 *
 * <p>磁盘上的相关文件名：
 * <ul>
 *   <li>{@code snapshot.db}：当前已发布快照；</li>
 *   <li>{@code snapshot.tmp}：写出中的临时快照，崩溃后残留可安全删除。</li>
 * </ul>
 *
 * <p>发布协议：写 tmp（每行 fsync 由一次最终 force 保证）→ force 目录项 →
 * {@code ATOMIC_MOVE} rename 为正式文件 → 再次 force 目录。
 * 崩溃点分析：tmp 完成前崩溃 → 正式快照仍是旧版本；rename 后崩溃 → 新快照可见；
 * 因此恢复端永远只看到“旧快照”或“新快照”，看不到半初始化内容。
 *
 * <p>快照首行是元数据头：{@code DKVCSNAP|1|records=<n>|lastSeq=<s>}，其后为记录行。
 */
public class SnapshotStore {

    public static final String SNAPSHOT_FILE = "snapshot.db";
    public static final String TMP_FILE = "snapshot.tmp";
    public static final String SNAPSHOT_HEADER = "DKVCSNAP";

    private final Path dir;
    private final Path snapshotFile;
    private final Path tmpFile;

    /** 一次快照发布的结果。 */
    public record SnapshotResult(Path snapshotFile, long records, long bytes, long lastSequence) {
    }

    public SnapshotStore(Path dir) {
        this.dir = dir;
        this.snapshotFile = dir.resolve(SNAPSHOT_FILE);
        this.tmpFile = dir.resolve(TMP_FILE);
    }

    public Path snapshotFile() {
        return snapshotFile;
    }

    /**
     * 原子写出快照。
     *
     * @param liveRecords 已按 sequence 升序排列的活动记录
     */
    public SnapshotResult writeSnapshot(List<WalRecord> liveRecords) {
        long lastSequence = liveRecords.isEmpty() ? -1L : liveRecords.get(liveRecords.size() - 1).sequence();
        long bytes = 0L;
        try {
            Files.createDirectories(dir);
            // 上次崩溃可能留下 tmp；先删除，避免 rename 目标冲突
            Files.deleteIfExists(tmpFile);

            StringBuilder sb = new StringBuilder();
            String header = SNAPSHOT_HEADER + "|1|records=" + liveRecords.size()
                    + "|lastSeq=" + lastSequence + "\n";
            sb.append(header);
            for (WalRecord r : liveRecords) {
                sb.append(r.encode()).append('\n');
            }
            byte[] payload = sb.toString().getBytes(StandardCharsets.UTF_8);
            bytes = payload.length;

            try (var ch = Files.newByteChannel(tmpFile,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                if (!(ch instanceof java.nio.channels.FileChannel fc)) {
                    throw new PersistenceException("file channel not supported for " + tmpFile, null);
                }
                var buf = java.nio.ByteBuffer.wrap(payload);
                while (buf.hasRemaining()) {
                    fc.write(buf);
                }
                fc.force(true);
            }
            forceDirectory(dir);
            Files.move(tmpFile, snapshotFile,
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            forceDirectory(dir);
            return new SnapshotResult(snapshotFile, liveRecords.size(), bytes, lastSequence);
        } catch (IOException e) {
            throw new PersistenceException("write snapshot failed", e);
        }
    }

    /** 读取并解码快照；正式快照不存在时返回 null（全新数据目录）。 */
    public List<WalRecord> readSnapshot() {
        if (!Files.exists(snapshotFile)) {
            return null;
        }
        List<WalRecord> out = new ArrayList<>();
        long lineNumber = 0;
        try (BufferedReader reader = Files.newBufferedReader(snapshotFile, StandardCharsets.UTF_8)) {
            String line = reader.readLine();
            lineNumber = 1;
            if (line == null || !line.startsWith(SNAPSHOT_HEADER + "|")) {
                throw new RecoveryException("SNAPSHOT_CORRUPT",
                        "snapshot header missing in " + snapshotFile);
            }
            long declaredRecords = parseHeaderLong(line, "records");
            long declaredLastSeq = parseHeaderLong(line, "lastSeq");
            long maxSeq = -1L;
            String dataLine;
            while ((dataLine = reader.readLine()) != null) {
                lineNumber++;
                if (dataLine.isEmpty()) {
                    continue;
                }
                WalRecord record = WalRecord.decode(dataLine, lineNumber);
                if (record.sequence() <= maxSeq) {
                    throw new RecoveryException("SNAPSHOT_CORRUPT",
                            "snapshot sequence not strictly increasing at line " + lineNumber);
                }
                maxSeq = record.sequence();
                out.add(record);
            }
            if (out.size() != declaredRecords) {
                throw new RecoveryException("SNAPSHOT_CORRUPT",
                        "snapshot record count mismatch: header=" + declaredRecords
                                + " actual=" + out.size());
            }
            if (maxSeq != declaredLastSeq) {
                throw new RecoveryException("SNAPSHOT_CORRUPT",
                        "snapshot lastSeq mismatch: header=" + declaredLastSeq + " actual=" + maxSeq);
            }
        } catch (RecoveryException e) {
            throw e;
        } catch (IOException e) {
            throw new RecoveryException("IO_ERROR", "cannot read snapshot " + snapshotFile, e);
        }
        return out;
    }

    /** 当前快照中最后一条记录的 sequence；无快照返回 -1。 */
    public long lastSnapshotSequence() {
        if (!Files.exists(snapshotFile)) {
            return -1L;
        }
        try (BufferedReader reader = Files.newBufferedReader(snapshotFile, StandardCharsets.UTF_8)) {
            String header = reader.readLine();
            if (header == null) {
                return -1L;
            }
            return parseHeaderLong(header, "lastSeq");
        } catch (IOException e) {
            throw new RecoveryException("IO_ERROR", "cannot inspect snapshot " + snapshotFile, e);
        }
    }

    private static long parseHeaderLong(String header, String key) {
        String prefix = key + "=";
        for (String part : header.split("\\|")) {
            if (part.startsWith(prefix)) {
                try {
                    return Long.parseLong(part.substring(prefix.length()));
                } catch (NumberFormatException e) {
                    break;
                }
            }
        }
        throw new RecoveryException("SNAPSHOT_CORRUPT", "bad snapshot header field: " + key);
    }

    /** 强制目录项变更落盘（rename 的持久性）。 */
    public static void forceDirectory(Path dir) throws IOException {
        try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        }
    }
}
