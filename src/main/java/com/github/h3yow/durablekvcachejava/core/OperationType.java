package com.github.h3yow.durablekvcachejava.core;

/**
 * WAL 操作类型。
 */
public enum OperationType {
    PUT, DELETE,
    /** 容量淘汰：内存淘汰结论必须持久化，否则重启后被淘汰键会复活。 */
    EVICT,
    /** 整组提交开始帧：承载 groupId 与成员操作数；与 GCOMMIT 之间的成员记录构成一个原子组。 */
    GBEGIN,
    /** 整组提交确认帧：恢复时只有读到与 GBEGIN 匹配的 GCOMMIT，该组才整体生效。 */
    GCOMMIT
}
