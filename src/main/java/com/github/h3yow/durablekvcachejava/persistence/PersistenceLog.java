package com.github.h3yow.durablekvcachejava.persistence;

import com.github.h3yow.durablekvcachejava.core.OperationRecord;

/**
 * 仅追加持久化日志（WAL）。
 */
public interface PersistenceLog {

    /** 追加一条记录并强制刷盘；成功返回后进程崩溃也不得丢失。 */
    void append(OperationRecord record);

    /** 顺序读取全部记录；遇损坏抛出 RecoveryException。 */
    Iterable<OperationRecord> readAll();

    /** 当前已分配的下一个序号。 */
    long nextSeq();

    /** 关闭底层资源。 */
    void close();
}
