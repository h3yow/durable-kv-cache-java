package com.github.h3yow.durablekvcachejava.model;

import com.github.h3yow.durablekvcachejava.exception.RecoveryException;

/**
 * 一条 WAL / 快照记录。
 *
 * <p>存储格式（一行一条，ASCII 头 + JSON 载荷，便于本地检查与复现）：
 * <pre>
 *   DKVC|1|<crc32>|<seq>|<PUT|DELETE>|<version>|<expireAt>|
 *   <json-base64-key>|<json-base64-value>
 * </pre>
 * 两个段都不得包含换行（JSON 字符串内的换行由转义处理，载荷段为 Base64，天然无换行）。
 * CRC32 覆盖除自身段以外的整行内容；解码时逐字段给出可区分的失败原因。
 */
public record WalRecord(
        long sequence,
        RecordType type,
        String key,
        long version,
        long expireAtEpochMs,
        String value
) {

    public static final String MAGIC = "DKVC";
    public static final int FORMAT_VERSION = 1;

    public WalRecord {
        if (type == null) {
            throw new IllegalArgumentException("record type is null");
        }
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("record key is null/empty");
        }
        if (type == RecordType.PUT && value == null) {
            throw new IllegalArgumentException("PUT record value is null for key=" + key);
        }
    }

    public static WalRecord put(long sequence, String key, long version,
                                long expireAtEpochMs, String value) {
        return new WalRecord(sequence, RecordType.PUT, key, version, expireAtEpochMs, value);
    }

    public static WalRecord delete(long sequence, String key, long version, long expireAtEpochMs) {
        return new WalRecord(sequence, RecordType.DELETE, key, version, expireAtEpochMs, null);
    }

    /**
     * 编码为单行文本（含尾部 '\n' 由调用方追加）。
     */
    public String encode() {
        String k = b64(key);
        String v = type == RecordType.PUT ? b64(value) : "";
        String bodyWithoutCrc = MAGIC + "|" + FORMAT_VERSION + "|%"
                + "|" + sequence
                + "|" + type.name()
                + "|" + version
                + "|" + expireAtEpochMs
                + "|" + k
                + "|" + v;
        long crc = crc32(stripCrcPlaceholder(bodyWithoutCrc));
        return bodyWithoutCrc.replace("%", Long.toUnsignedString(crc));
    }

    /**
     * 解码一行。
     *
     * @param lineNumber 从 1 开始的行号，用于错误定位
     * @throws RecoveryException 原因码：UNSUPPORTED_FORMAT / CHECKSUM_MISMATCH / TRUNCATED_RECORD
     */
    public static WalRecord decode(String line, long lineNumber) {
        String[] parts = line.split("\\|", -1);
        if (parts.length != 9) {
            throw new RecoveryException("TRUNCATED_RECORD",
                    "line " + lineNumber + ": expected 9 fields but got " + parts.length);
        }
        if (!MAGIC.equals(parts[0])) {
            throw new RecoveryException("UNSUPPORTED_FORMAT",
                    "line " + lineNumber + ": bad magic " + parts[0]);
        }
        int fmt;
        try {
            fmt = Integer.parseInt(parts[1]);
        } catch (NumberFormatException e) {
            throw new RecoveryException("UNSUPPORTED_FORMAT", "line " + lineNumber + ": bad format version", e);
        }
        if (fmt != FORMAT_VERSION) {
            throw new RecoveryException("UNSUPPORTED_FORMAT",
                    "line " + lineNumber + ": unsupported format version " + fmt);
        }
        long expectedCrc;
        try {
            expectedCrc = Long.parseUnsignedLong(parts[2]);
        } catch (NumberFormatException e) {
            throw new RecoveryException("TRUNCATED_RECORD", "line " + lineNumber + ": bad crc", e);
        }
        long actualCrc = crc32(rebuildForCrc(parts));
        if (expectedCrc != actualCrc) {
            throw new RecoveryException("CHECKSUM_MISMATCH",
                    "line " + lineNumber + ": crc expected=" + Long.toUnsignedString(expectedCrc)
                            + " actual=" + Long.toUnsignedString(actualCrc));
        }
        try {
            long sequence = Long.parseLong(parts[3]);
            RecordType type = RecordType.valueOf(parts[4]);
            long version = Long.parseLong(parts[5]);
            long expireAt = Long.parseLong(parts[6]);
            String key = unb64(parts[7], "key", lineNumber);
            String value = type == RecordType.PUT ? unb64(parts[8], "value", lineNumber) : null;
            return new WalRecord(sequence, type, key, version, expireAt, value);
        } catch (IllegalArgumentException e) {
            throw new RecoveryException("TRUNCATED_RECORD",
                    "line " + lineNumber + ": malformed field: " + e.getMessage(), e);
        }
    }

    private static String stripCrcPlaceholder(String encoded) {
        return encoded.replace("|%|", "||");
    }

    private static String rebuildForCrc(String[] parts) {
        // 与 stripCrcPlaceholder 的结果保持一致：crc 段留空
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) {
                sb.append('|');
            }
            sb.append(i == 2 ? "" : parts[i]);
        }
        return sb.toString();
    }

    private static String b64(String s) {
        return java.util.Base64.getEncoder().encodeToString(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static String unb64(String s, String field, long lineNumber) {
        try {
            return new String(java.util.Base64.getDecoder().decode(s), java.nio.charset.StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            throw new RecoveryException("TRUNCATED_RECORD",
                    "line " + lineNumber + ": bad base64 in field " + field, e);
        }
    }

    private static long crc32(String s) {
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return crc.getValue();
    }

}
