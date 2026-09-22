package com.github.h3yow.durablekvcachejava.core;

/**
 * 缓存条目（内存不可变快照）。
 */
public record CacheEntry(String value, long expireAtMs) {
    // TODO 骨架
}
