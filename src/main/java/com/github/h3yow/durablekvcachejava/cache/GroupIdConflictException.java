package com.github.h3yow.durablekvcachejava.cache;

/**
 * 整组提交的 groupId 冲突：同一个 groupId 此前已成功提交过一组<b>不同内容</b>的操作。
 *
 * <p>内容完全相同的重复提交按幂等处理（返回 {@code duplicate=true}，不产生副作用）；
 * 仅当 groupId 相同而成员/动作不同时报此异常，防止调用方串组误提交。</p>
 */
public final class GroupIdConflictException extends RuntimeException {

    private final String groupId;

    public GroupIdConflictException(String groupId, String message) {
        super(message);
        this.groupId = groupId;
    }

    public String groupId() {
        return groupId;
    }
}
