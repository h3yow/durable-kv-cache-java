package com.github.h3yow.durablekvcachejava.persistence;

import com.github.h3yow.durablekvcachejava.core.OperationRecord;

/**
 * WAL 中一条完整记录及其在文件中的字节区间。
 *
 * @param record     解析出的记录
 * @param startOffset 该行起始字节偏移（含）
 * @param endOffset   该行结束字节偏移（不含，即换行符之后的位置）
 */
public record WalEntry(OperationRecord record, long startOffset, long endOffset) {
}
