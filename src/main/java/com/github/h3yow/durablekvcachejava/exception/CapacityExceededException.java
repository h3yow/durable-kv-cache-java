package com.github.h3yow.durablekvcachejava.exception;

/** 容量上限超限且淘汰策略为 REJECT（或无可淘汰条目）。 */
public class CapacityExceededException extends CacheException {
    public CapacityExceededException(String message) { super(message); }
    @Override public String errorCode() { return "CAPACITY_EXCEEDED"; }
}
