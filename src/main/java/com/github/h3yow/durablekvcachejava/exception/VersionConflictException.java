package com.github.h3yow.durablekvcachejava.exception;

/** 状态冲突：版本前置条件不满足（CAS 冲突）。 */
public class VersionConflictException extends CacheException {
    public VersionConflictException(String message) { super(message); }
    @Override public String errorCode() { return "VERSION_CONFLICT"; }
}
