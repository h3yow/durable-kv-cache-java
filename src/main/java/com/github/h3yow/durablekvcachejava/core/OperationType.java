package com.github.h3yow.durablekvcachejava.core;

/**
 * WAL 操作类型。
 */
public enum OperationType {
    PUT, DELETE,
    /** 容量淘汰：内存淘汰结论必须持久化，否则重启后被淘汰键会复活。 */
    EVICT
}
