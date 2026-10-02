package com.github.h3yow.durablekvcachejava.cache;

/**
 * 缓存相关异常，通过不同子类给出可区分原因。
 */
public final class CacheExceptions {

    private CacheExceptions() {
    }

    /** 非法参数：键/值/TTL 不合法。 */
    public static final class InvalidRequestException extends RuntimeException {
        public InvalidRequestException(String message) {
            super(message);
        }
    }

    /** 容量超限且淘汰后仍无法容纳。 */
    public static final class CapacityExceededException extends RuntimeException {

        /** 机器可判定原因：单键默认 {@code SINGLE_KEY_FULL}；整组为 {@code GROUP_TOO_LARGE} 等。 */
        private final String reason;

        public CapacityExceededException(String message) {
            this("SINGLE_KEY_FULL", message);
        }

        public CapacityExceededException(String reason, String message) {
            super(message);
            this.reason = reason;
        }

        public String reason() {
            return reason;
        }
    }

    /** 状态冲突：例如恢复未完成时访问。 */
    public static final class StateConflictException extends RuntimeException {
        public StateConflictException(String message) {
            super(message);
        }
    }

    /** 持久化失败：写入 WAL 失败（写不可视为成功）。 */
    public static final class PersistenceException extends RuntimeException {
        public PersistenceException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** 恢复失败：WAL 损坏等，启动必须中止。 */
    public static final class RecoveryException extends RuntimeException {
        public RecoveryException(String message, Throwable cause) {
            super(message, cause);
        }
        public RecoveryException(String message) {
            super(message);
        }
    }
}
