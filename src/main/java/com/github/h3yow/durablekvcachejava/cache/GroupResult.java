package com.github.h3yow.durablekvcachejava.cache;

import java.util.List;

/**
 * 整组提交的明确结论。
 *
 * @param groupId       组标识（调用方提供，用于幂等去重）
 * @param committed     true=本次真正生效（或为重复提交的幂等返回）；false 仅预留给扩展场景
 * @param duplicate     true=该 groupId 此前已提交成功，本次未执行任何变更
 * @param insertedKeys  实际新插入的键
 * @param updatedKeys   实际重写的键
 * @param deletedKeys   实际删除的键
 * @param evictedKeys   为容纳本组按 LRU 淘汰的键（确定顺序）
 * @param liveEntries   提交后的存活键数
 * @param beginSeq      组首记录序号（TXBEG）
 * @param endSeq        组尾记录序号（TXEND）
 */
public record GroupResult(String groupId, boolean committed, boolean duplicate,
                          List<String> insertedKeys, List<String> updatedKeys,
                          List<String> deletedKeys, List<String> evictedKeys,
                          int liveEntries, long beginSeq, long endSeq) {
}
