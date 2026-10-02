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

    /**
     * 将日志截断到给定字节长度并强制刷盘，用于整组提交中途失败时回滚尚未提交的帧，
     * 使 WAL 立即回到"最后一个完整提交点"。
     */
    void truncateTo(long byteSize);

    /** 当前 WAL 文件字节数（整组提交前记录"提交点"用）。 */
    long sizeBytes();

    /** 关闭底层资源。 */
    void close();
}
