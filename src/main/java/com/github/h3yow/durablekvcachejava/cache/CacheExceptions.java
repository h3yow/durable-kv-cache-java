package com.github.h3yow.durablekvcachejava.cache;

import java.util.List;

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

    /**
     * 整组提交前置条件不满足：组内 DELETE 的键在提交时刻已过期或已被删除。
     * 整组被拒绝，不产生任何副作用。
     */
    public static final class GroupStaleException extends RuntimeException {
        private final String groupId;
        private final transient List<StaleKey> staleKeys;

        public GroupStaleException(String groupId, List<StaleKey> staleKeys) {
            super("整组提交被拒绝: groupId=" + groupId + " 存在已过期或已删除的成员键: " + staleKeys);
            this.groupId = groupId;
            this.staleKeys = staleKeys;
        }

        public String groupId() {
            return groupId;
        }

        public List<StaleKey> staleKeys() {
            return staleKeys;
        }

        /** 单个失效成员键的判定依据。 */
        public record StaleKey(String key, String reason) {
            public static StaleKey expired(String key) {
                return new StaleKey(key, "KEY_EXPIRED");
            }

            public static StaleKey deleted(String key) {
                return new StaleKey(key, "KEY_NOT_FOUND");
            }
        }
    }

    /**
     * 整组容量不足：整组本身超过上限，或按现有 LRU 顺序仍腾不出足够位置。
     * 整组被拒绝，不允许先淘汰一部分再失败，因此不产生任何淘汰副作用。
     */
    public static final class GroupCapacityException extends RuntimeException {
        private final String groupId;
        private final String reasonCode;

        public GroupCapacityException(String groupId, String reasonCode, String message) {
            super(message);
            this.groupId = groupId;
            this.reasonCode = reasonCode;
        }

        public String groupId() {
            return groupId;
        }

        public String reasonCode() {
            return reasonCode;
        }
    }
}
