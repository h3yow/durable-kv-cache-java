package com.github.h3yow.durablekvcachejava.dto;

/**
 * 写入请求。
 * ttlMillis / ttlSeconds 二选一（服务端过期时长，相对值）；
 * 都为空表示永不过期。expectedVersion 为可选 CAS 前置条件。
 */
public record PutRequest(String value, Long ttlMillis, Long ttlSeconds, Long expectedVersion) {
}
