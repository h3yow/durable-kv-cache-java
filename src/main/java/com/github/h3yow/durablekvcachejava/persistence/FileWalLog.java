package com.github.h3yow.durablekvcachejava.persistence;

import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.PersistenceException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.RecoveryException;
import com.github.h3yow.durablekvcachejava.core.OperationRecord;
import com.github.h3yow.durablekvcachejava.core.OperationType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * 基于单文件 + {@link java.nio.channels.FileChannel#force} 的 WAL 实现。
 *
 * <p>行格式（每条一行，UTF-8，\n 结尾）：
 * {@code SEQ|TYPE|keyB64|valueB64|ttl|timestamp[|groupIdB64]|crc64}。
 * 单键历史记录为 7 段（无 groupId）；整组帧/成员为 8 段（末段前为 groupId）。
 * DELETE/EVICT 值字段为空串。</p>
 *
 * <p>整组原子性的文件侧依据：一个整组在 WAL 中形如
 * {@code GBEGIN, 成员..., GCOMMIT}，三者各有独立连续序号。恢复读取时只返回
 * 截至最后一个"完整提交点"（以 \n 结尾的普通记录，或 GCOMMIT 帧）的记录；
 * 末尾的半截行、或只有 GBEGIN 没有匹配 GCOMMIT 的整段内容一律视为崩溃残留并截除。
 * 已经完整落盘的行若内容非法（CRC/字段/帧结构错误）则恢复必须失败。</p>
 */
public final class FileWalLog implements PersistenceLog {

    private final Path file;
    private final Object appendLock = new Object();
    private volatile boolean closed = false;

    public FileWalLog(String dataDir, String fileName) {
        try {
            Path dir = Path.of(dataDir);
            Files.createDirectories(dir);
            this.file = dir.resolve(fileName);
            // 以读写方式创建/打开，保证文件存在且后续 force 作用于同一文件描述符。
            try (var ch = java.nio.channels.FileChannel.open(file,
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                ch.force(true);
            }
        } catch (IOException e) {
            throw new PersistenceException("无法初始化 WAL 文件: " + dataDir + "/" + fileName, e);
        }
    }

    @Override
    public void append(OperationRecord r) {
        String line = r.seq() + "|" + r.type().name() + "|"
                + encode(r.key()) + "|"
                + (r.type() == OperationType.EVICT ? "" : encode(r.value())) + "|"
                + r.ttlMillis() + "|" + r.timestampMs()
                + (r.groupId() == null ? "" : "|" + encode(r.groupId()))
                + "|" + checksum(r) + "\n";
        byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
        synchronized (appendLock) {
            if (closed) {
                throw new PersistenceException("WAL 已关闭，无法追加 seq=" + r.seq(), null);
            }
            try (var ch = java.nio.channels.FileChannel.open(file, StandardOpenOption.WRITE,
                    StandardOpenOption.APPEND)) {
                ByteBuffer buf = ByteBuffer.wrap(bytes);
                int written = 0;
                while (buf.hasRemaining()) {
                    written += ch.write(buf);
                }
                // 强制内容与元数据落盘，确认成功后崩溃也不丢。
                ch.force(true);
                if (written != bytes.length) {
                    throw new PersistenceException(
                            "WAL 短写入: expected=" + bytes.length + " actual=" + written, null);
                }
            } catch (IOException e) {
                throw new PersistenceException("WAL 追加失败 seq=" + r.seq(), e);
            }
        }
    }

    @Override
    public Iterable<OperationRecord> readAll() {
        try {
            byte[] raw = Files.readAllBytes(file);
            java.util.List<OperationRecord> records = new java.util.ArrayList<>();
            int start = 0;
            int lineNo = 0;
            // 最后一个完整提交点（完整普通记录结尾，或 GCOMMIT 帧结尾）的字节偏移。
            long committedEnd = 0L;
            // 若当前位于一个整组帧内，记录其 GBEGIN 之前的提交点偏移；-1 表示不在组内。
            long groupStartOffset = -1L;
            String openGroupId = null;
            int recordsRemaining = 0;
            int recordsAppended = 0;

            for (int i = 0; i < raw.length; i++) {
                if (raw[i] != '\n') {
                    continue;
                }
                lineNo++;
                String line = new String(raw, start, i - start, StandardCharsets.UTF_8);
                OperationRecord r = parseLine(line, lineNo);

                if (openGroupId == null) {
                    if (r.type() == OperationType.GBEGIN) {
                        groupStartOffset = committedEnd;
                        openGroupId = r.key();
                        // GBEGIN 声明的是帧内记录总数（淘汰 EVICT + 成员 PUT/DELETE）。
                        recordsRemaining = (int) r.ttlMillis();
                        recordsAppended = 0;
                        if (recordsRemaining <= 0) {
                            throw new RecoveryException(
                                    "WAL 第 " + lineNo + " 行 GBEGIN 帧内记录数非法: " + recordsRemaining, null);
                        }
                        // 帧本身同样进入返回流（占用序号），但不算"已提交内容"。
                        records.add(r);
                    } else if (r.type() == OperationType.GCOMMIT) {
                        throw new RecoveryException(
                                "WAL 第 " + lineNo + " 行出现无匹配 GBEGIN 的 GCOMMIT(groupId="
                                        + r.key() + ")", null);
                    } else {
                        if (r.groupId() != null) {
                            // 带 groupId 的成员记录却不在任何整组帧内：帧结构损坏，必须失败。
                            throw new RecoveryException(
                                    "WAL 第 " + lineNo + " 行记录带 groupId=" + r.groupId()
                                            + " 但不在 GBEGIN/GCOMMIT 帧内", null);
                        }
                        records.add(r);
                        committedEnd = (long) i + 1;
                    }
                } else {
                    // 帧内：只允许 PUT/DELETE/EVICT 记录以及匹配的 GCOMMIT。
                    if (r.type() == OperationType.GBEGIN) {
                        throw new RecoveryException(
                                "WAL 第 " + lineNo + " 行整组(groupId=" + openGroupId
                                        + ")未提交又出现新的 GBEGIN", null);
                    }
                    if (r.type() == OperationType.GCOMMIT) {
                        if (!openGroupId.equals(r.key()) || !openGroupId.equals(r.groupId())) {
                            throw new RecoveryException(
                                    "WAL 第 " + lineNo + " 行 GCOMMIT 与 GBEGIN 不匹配: gbegin="
                                            + openGroupId + " 实际=" + r.key(), null);
                        }
                        if (recordsRemaining != 0) {
                            throw new RecoveryException(
                                    "WAL 第 " + lineNo + " 行整组(groupId=" + openGroupId
                                            + ")记录数不足, 缺少 " + recordsRemaining + " 条", null);
                        }
                        records.add(r);
                        committedEnd = (long) i + 1;
                        openGroupId = null;
                        groupStartOffset = -1L;
                        recordsAppended = 0;
                    } else {
                        if (!openGroupId.equals(r.groupId())) {
                            throw new RecoveryException(
                                    "WAL 第 " + lineNo + " 行整组帧内记录 groupId 不匹配: 期望="
                                            + openGroupId + " 实际=" + r.groupId(), null);
                        }
                        records.add(r);
                        recordsAppended++;
                        recordsRemaining--;
                        if (recordsRemaining < 0) {
                            throw new RecoveryException(
                                    "WAL 第 " + lineNo + " 行整组(groupId=" + openGroupId
                                            + ")记录数超过 GBEGIN 声明", null);
                        }
                    }
                }
                start = i + 1;
            }

            // 未闭合整组：从 GBEGIN（含）起的所有记录都只是临时加入，需整体回退到 GBEGIN 之前。
            if (openGroupId != null) {
                int toRemove = recordsAppended + 1; // +1：GBEGIN 帧本身
                for (int k = 0; k < toRemove; k++) {
                    records.remove(records.size() - 1);
                }
                committedEnd = groupStartOffset;
            }
            // 末尾存在无换行残留，或存在未闭合整组：均为崩溃残留，统一截到最后完整提交点。
            if (committedEnd < raw.length) {
                truncate(committedEnd);
            }
            return records;
        } catch (RecoveryException e) {
            throw e;
        } catch (IOException e) {
            throw new RecoveryException("WAL 读取失败: " + file, e);
        }
    }

    @Override
    public long nextSeq() {
        long max = 0L;
        for (OperationRecord r : readAll()) {
            if (r.seq() <= max) {
                throw new RecoveryException(
                        "WAL 序号非严格递增: seq=" + r.seq() + " 上一最大=" + max, null);
            }
            max = r.seq();
        }
        return max + 1;
    }

    @Override
    public void truncateTo(long byteSize) {
        truncate(byteSize);
    }

    @Override
    public long sizeBytes() {
        synchronized (appendLock) {
            try {
                return Files.size(file);
            } catch (IOException e) {
                throw new PersistenceException("WAL 大小读取失败: " + file, e);
            }
        }
    }

    @Override
    public void close() {
        synchronized (appendLock) {
            closed = true;
        }
    }

    private void truncate(long size) {
        try (var ch = java.nio.channels.FileChannel.open(file, StandardOpenOption.WRITE)) {
            ch.truncate(size);
            ch.force(true);
        } catch (IOException e) {
            throw new RecoveryException("WAL 截断残留数据失败: " + file, e);
        }
    }

    private static OperationRecord parseLine(String line, int lineNo) {
        // seq|type|key|value|ttl|ts|[groupId]|crc —— 历史单键记录 7 段，整组记录 8 段。
        String[] parts = line.split("\\|", -1);
        if (parts.length != 7 && parts.length != 8) {
            throw new RecoveryException("WAL 第 " + lineNo + " 行字段数非法: " + parts.length, null);
        }
        try {
            long seq = Long.parseLong(parts[0]);
            OperationType type = OperationType.valueOf(parts[1]);
            String key = decode(parts[2]);
            String value = (parts[3].isEmpty() || type == OperationType.EVICT
                    || type == OperationType.DELETE
                    || type == OperationType.GBEGIN || type == OperationType.GCOMMIT)
                    ? null : decode(parts[3]);
            long ttl = Long.parseLong(parts[4]);
            long ts = Long.parseLong(parts[5]);
            String groupId;
            long crc;
            if (parts.length == 8) {
                groupId = parts[6].isEmpty() ? null : decode(parts[6]);
                crc = Long.parseLong(parts[7]);
            } else {
                groupId = null;
                crc = Long.parseLong(parts[6]);
            }
            OperationRecord r = new OperationRecord(seq, type, key, value, ttl, ts, groupId);
            if (checksum(r) != crc) {
                throw new RecoveryException("WAL 第 " + lineNo + " 行校验和不匹配 (key=" + key + ")", null);
            }
            boolean frame = type == OperationType.GBEGIN || type == OperationType.GCOMMIT;
            if (!parts[3].isEmpty() && (type == OperationType.DELETE || type == OperationType.EVICT
                    || frame)) {
                throw new RecoveryException("WAL 第 " + lineNo + " 行 " + type + " 的 value 字段必须为空", null);
            }
            if (type == OperationType.PUT && (value == null || ttl <= 0)) {
                throw new RecoveryException("WAL 第 " + lineNo + " 行 PUT 数据不完整", null);
            }
            if ((type == OperationType.DELETE || type == OperationType.EVICT) && ttl != 0L) {
                throw new RecoveryException("WAL 第 " + lineNo + " 行 " + type + " 的 ttl 必须为 0", null);
            }
            if (type == OperationType.GBEGIN) {
                if (ttl <= 0) {
                    throw new RecoveryException("WAL 第 " + lineNo + " 行 GBEGIN 成员数非法: " + ttl, null);
                }
                if (groupId == null || !groupId.equals(key)) {
                    throw new RecoveryException("WAL 第 " + lineNo + " 行 GBEGIN 缺少 groupId", null);
                }
            }
            if (type == OperationType.GCOMMIT) {
                if (ttl != 0L || groupId == null || !groupId.equals(key)) {
                    throw new RecoveryException("WAL 第 " + lineNo + " 行 GCOMMIT 字段非法", null);
                }
            }
            if (!frame && parts.length == 8 && groupId == null) {
                throw new RecoveryException("WAL 第 " + lineNo + " 行整组成员缺少 groupId", null);
            }
            return r;
        } catch (IllegalArgumentException e) {
            throw new RecoveryException("WAL 第 " + lineNo + " 行解析失败: " + e.getMessage(), e);
        }
    }

    private static String encode(String s) {
        if (s == null) {
            return "";
        }
        return java.util.Base64.getEncoder().encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    private static String decode(String s) {
        if (s.isEmpty()) {
            return null;
        }
        return new String(java.util.Base64.getDecoder().decode(s), StandardCharsets.UTF_8);
    }

    /**
     * 与具体内容绑定的简单 FNV-1a 64 位校验和，用于检测行损坏/截断拼接。
     * 计算口径覆盖到 groupId（若有），历史无 groupId 记录的校验值保持不变。
     */
    public static long checksum(OperationRecord r) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeUtf8(out, r.seq() + "|" + r.type().name() + "|");
        writeUtf8(out, r.key() == null ? "" : r.key());
        out.write('|');
        writeUtf8(out, r.value() == null ? "" : r.value());
        out.write('|');
        writeUtf8(out, Long.toString(r.ttlMillis()));
        out.write('|');
        writeUtf8(out, Long.toString(r.timestampMs()));
        if (r.groupId() != null) {
            out.write('|');
            writeUtf8(out, r.groupId());
        }
        long hash = 0xcbf29ce484222325L;
        for (byte b : out.toByteArray()) {
            hash ^= (b & 0xffL);
            hash *= 0x100000001b3L;
        }
        return hash;
    }

    private static void writeUtf8(ByteArrayOutputStream out, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.write(b, 0, b.length);
    }
}
