package com.github.h3yow.durablekvcachejava.persistence;

import com.github.h3yow.durablekvcachejava.core.OperationRecord;

import java.io.Closeable;

/**
 * 仅追加持久化日志（WAL）。
 */
public interface PersistenceLog extends Closeable {

    /** 追加一条记录并强制刷盘；成功返回后进程崩溃也不得丢失。 */
    void append(OperationRecord record);

    /** 顺序读取全部记录；遇损坏抛出 RecoveryException。 */
    Iterable<OperationRecord> readAll();

    /** 顺序读取全部记录并附带每行字节偏移；末尾无换行的半截行被截除。 */
    Iterable<WalEntry> readAllWithOffsets();

    /** 当前 WAL 文件长度（字节）。 */
    long size();

    /** 将文件截断到指定字节长度并强制刷盘（整组提交失败时回滚半组记录用）。 */
    void truncateTo(long size);

    /** 当前已分配的下一个序号。 */
    long nextSeq();

    /** 关闭底层资源。 */
    void close();
}
