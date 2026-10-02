package com.github.h3yow.durablekvcachejava.core;

/**
 * WAL 操作类型。
 */
public enum OperationType {
    PUT, DELETE,
    /** 容量淘汰：内存淘汰结论必须持久化，否则重启后被淘汰键会复活。 */
    EVICT,
    /** 整组提交开始：记录组标识与成员数量，后续连续记录属于同一组。 */
    TXBEG,
    /** 整组提交结束：只有看到对应 TXEND 的组才是确认提交；此前崩溃即整组丢弃。 */
    TXEND
}
