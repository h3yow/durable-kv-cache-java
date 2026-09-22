package com.github.h3yow.durablekvcachejava.dto;

/** 读取响应。expired=true 时 value/version 为空，由 reason 给出判定依据。 */
public record GetResponse(
        String key,
        boolean found,
        boolean expired,
        String value,
        Long version,
        Long expireAtEpochMs,
        Long ttlRemainingMs,
        String reason
) {
}
