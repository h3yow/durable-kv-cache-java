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
        public CapacityExceededException(String message) {
            super(message);
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
