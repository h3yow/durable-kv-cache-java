package com.github.h3yow.durablekvcachejava.exception;

/**
 * 恢复失败。reason 为可区分的失败原因：
 * CHECKSUM_MISMATCH / TRUNCATED_RECORD / SNAPSHOT_CORRUPT /
 * UNSUPPORTED_FORMAT / IO_ERROR / INVALID_STATE。
 */
public class RecoveryException extends CacheException {
    private final String reason;

    public RecoveryException(String reason, String message) {
        super(message);
        this.reason = reason;
    }

    public RecoveryException(String reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public String reason() { return reason; }

    @Override public String errorCode() { return "RECOVERY_FAILURE:" + reason; }
}
