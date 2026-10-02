package com.github.h3yow.durablekvcachejava.cache;

import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.InvalidRequestException;

import java.util.List;

/**
 * 整组前置条件失败：组内某个键在提交时已经过期、已经被删除（UPDATE/DELETE 找不到存活键），
 * 或 INSERT 目标已存在。整组被拒绝，不产生任何副作用。
 *
 * <p>与参数错误（{@link InvalidRequestException}）通过独立类型/错误码区分，
 * 调用方可安全重试：重新读取当前状态后再次提交即可。</p>
 */
public final class GroupStaleException extends RuntimeException {

    private final String groupId;
    private final List<String> staleKeys;

    public GroupStaleException(String groupId, List<String> staleKeys, String message) {
        super(message);
        this.groupId = groupId;
        this.staleKeys = List.copyOf(staleKeys);
    }

    public String groupId() {
        return groupId;
    }

    public List<String> staleKeys() {
        return staleKeys;
    }
}
