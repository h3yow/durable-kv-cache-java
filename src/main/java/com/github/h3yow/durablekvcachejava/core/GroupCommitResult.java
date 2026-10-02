package com.github.h3yow.durablekvcachejava.core;

import java.util.List;

/**
 * 整组提交的成功结果。
 *
 * <p>失败（参数错误 / 组成员过期或已删除 / 容量不足 / 持久化失败 / 状态冲突）一律抛出
 * 可区分的异常，绝不返回本结果——即"要么全部生效，要么全部不生效"。</p>
 *
 * @param groupId      组 ID（调用方提供或服务端生成），可用于重试去重与日志追踪
 * @param outcome      COMMITTED（本次整体生效）或 DUPLICATE（同 ID 同内容已生效，本次不重复执行）
 * @param memberCount  组内成员操作数
 * @param evictedKeys  为容纳本组按现有 LRU 顺序淘汰的键（空列表表示未淘汰任何键）
 * @param beginSeq     本组 GBEGIN 帧序号（DUPLICATE 时为历史提交的序号）
 * @param commitSeq    本组 GCOMMIT 确认帧序号
 * @param liveEntries  提交后存活键数
 */
public record GroupCommitResult(String groupId, Outcome outcome, int memberCount,
                                List<String> evictedKeys, long beginSeq, long commitSeq,
                                int liveEntries) {

    public enum Outcome { COMMITTED, DUPLICATE }

    public boolean duplicate() {
        return outcome == Outcome.DUPLICATE;
    }
}
