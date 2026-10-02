package com.github.h3yow.durablekvcachejava.persistence;

import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.PersistenceException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.RecoveryException;
import com.github.h3yow.durablekvcachejava.core.OperationRecord;
import com.github.h3yow.durablekvcachejava.core.OperationType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于单文件 + {@link FileChannel#force} 的 WAL 实现。
 *
 * <p>行格式（每条一行，UTF-8，\n 结尾），新版本为 8 段：
 * {@code SEQ|TYPE|keyB64|valueB64|ttl|timestamp|crc64|groupB64}，
 * 单键记录的 group 段为空串，整组提交记录携带 groupId。
 * 同时兼容读取历史 7 段行（无 group 段，按旧校验和验证）。</p>
 *
 * <p>整组提交在 WAL 中表现为 {@code TXBEG,(EVICT*,成员操作*),TXEND} 的连续帧；
 * 帧内每条记录都独立 force。末尾允许存在被截断的半行或未闭合的组帧（崩溃残留），
 * 恢复时安全忽略并截除；已完整落盘的行若内容非法则恢复必须失败。</p>
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
            try (FileChannel ch = FileChannel.open(file,
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
                + ((r.type() == OperationType.PUT || r.type() == OperationType.TXBEG)
                        ? encode(r.value()) : "") + "|"
                + r.ttlMillis() + "|" + r.timestampMs() + "|" + checksumV2(r) + "|"
                + encode(r.groupId()) + "\n";
        byte[] bytes = line.getBytes(StandardCharsets.UTF_8);
        synchronized (appendLock) {
            if (closed) {
                throw new PersistenceException("WAL 已关闭，无法追加 seq=" + r.seq(), null);
            }
            try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE,
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
    public List<OperationRecord> readAll() {
        List<WalEntry> entries = readEntries();
        List<OperationRecord> records = new ArrayList<>(entries.size());
        for (WalEntry e : entries) {
            records.add(e.record());
        }
        return records;
    }

    @Override
    public List<WalEntry> readAllWithOffsets() {
        return readEntries();
    }

    @Override
    public long size() {
        try {
            return Files.size(file);
        } catch (IOException e) {
            throw new RecoveryException("WAL 读取文件长度失败: " + file, e);
        }
    }

    @Override
    public void truncateTo(long size) {
        synchronized (appendLock) {
            try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
                ch.truncate(size);
                ch.force(true);
            } catch (IOException e) {
                throw new PersistenceException("WAL 截断失败 size=" + size, e);
            }
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
    public void close() {
        synchronized (appendLock) {
            closed = true;
        }
    }

    // ------------------------------------------------------------------ 解析

    private List<WalEntry> readEntries() {
        try {
            byte[] raw = Files.readAllBytes(file);
            List<WalEntry> entries = new ArrayList<>();
            int start = 0;
            int validLineEnd = 0;
            int lineNo = 0;
            for (int i = 0; i < raw.length; i++) {
                if (raw[i] == '\n') {
                    lineNo++;
                    String line = new String(raw, start, i - start, StandardCharsets.UTF_8);
                    OperationRecord r = parseLine(line, lineNo);
                    entries.add(new WalEntry(r, start, i + 1));
                    start = i + 1;
                    validLineEnd = i + 1;
                }
            }
            // 存在无换行结尾的残留：只可能是崩溃时的截断，截除并在下次写入前修复文件长度。
            if (validLineEnd < raw.length) {
                truncate(validLineEnd);
            }
            return entries;
        } catch (RecoveryException e) {
            throw e;
        } catch (IOException e) {
            throw new RecoveryException("WAL 读取失败: " + file, e);
        }
    }

    private void truncate(long size) {
        try (FileChannel ch = FileChannel.open(file, StandardOpenOption.WRITE)) {
            ch.truncate(size);
            ch.force(true);
        } catch (IOException e) {
            throw new RecoveryException("WAL 截断残留数据失败: " + file, e);
        }
    }

    private static OperationRecord parseLine(String line, int lineNo) {
        // 新版 8 段: seq|type|key|value|ttl|ts|crc|group；旧版 7 段: 无 group
        String[] parts = line.split("\\|", -1);
        if (parts.length != 7 && parts.length != 8) {
            throw new RecoveryException("WAL 第 " + lineNo + " 行字段数非法: " + parts.length, null);
        }
        try {
            long seq = Long.parseLong(parts[0]);
            OperationType type = OperationType.valueOf(parts[1]);
            String key = decode(parts[2]);
            String value = (parts[3].isEmpty()
                    || type == OperationType.DELETE || type == OperationType.EVICT
                    || type == OperationType.TXEND)
                    ? null : decode(parts[3]);
            long ttl = Long.parseLong(parts[4]);
            long ts = Long.parseLong(parts[5]);
            long crc = Long.parseLong(parts[6]);
            String groupId = parts.length == 8 ? decode(parts[7]) : null;
            OperationRecord r = new OperationRecord(seq, type, key, value, ttl, ts, groupId);
            long expected = parts.length == 8 ? checksumV2(r) : checksumV1(r);
            if (expected != crc) {
                throw new RecoveryException("WAL 第 " + lineNo + " 行校验和不匹配 (key=" + key + ")", null);
            }
            if (type == OperationType.PUT && (value == null || ttl <= 0)) {
                throw new RecoveryException("WAL 第 " + lineNo + " 行 PUT 数据不完整", null);
            }
            if ((type == OperationType.DELETE || type == OperationType.EVICT)
                    && (!parts[3].isEmpty() || ttl != 0L)) {
                throw new RecoveryException("WAL 第 " + lineNo + " 行 " + type + " 字段必须为空/0", null);
            }
            if (type == OperationType.TXBEG) {
                if (groupId == null || ttl < 0L) {
                    throw new RecoveryException(
                            "WAL 第 " + lineNo + " 行 TXBEG 组帧字段非法", null);
                }
            }
            if (type == OperationType.TXEND) {
                if (!parts[3].isEmpty() || ttl < 0L || groupId == null) {
                    throw new RecoveryException(
                            "WAL 第 " + lineNo + " 行 TXEND 组帧字段非法", null);
                }
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

    /** 旧版（7 段行）校验和：覆盖前 6 段内容。 */
    static long checksumV1(OperationRecord r) {
        return fnv(payload(r, false));
    }

    /** 新版（8 段行）校验和：覆盖前 6 段及 groupId。 */
    static long checksumV2(OperationRecord r) {
        return fnv(payload(r, true));
    }

    private static byte[] payload(OperationRecord r, boolean withGroup) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeUtf8(out, r.seq() + "|" + r.type().name() + "|");
        writeUtf8(out, r.key() == null ? "" : r.key());
        out.write('|');
        writeUtf8(out, r.value() == null ? "" : r.value());
        out.write('|');
        writeUtf8(out, Long.toString(r.ttlMillis()));
        out.write('|');
        writeUtf8(out, Long.toString(r.timestampMs()));
        if (withGroup) {
            out.write('|');
            writeUtf8(out, r.groupId() == null ? "" : r.groupId());
        }
        return out.toByteArray();
    }

    /** 与具体内容绑定的简单 FNV-1a 64 位校验和，用于检测行损坏/截断拼接。 */
    private static long fnv(byte[] data) {
        long hash = 0xcbf29ce484222325L;
        for (byte b : data) {
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
