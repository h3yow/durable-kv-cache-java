package com.github.h3yow.durablekvcachejava.dto;

/** 写入确认：只有已 fsync 落盘后才返回。 */
public record PutResponse(
        String key,
        long version,
        long sequence,
        long expireAtEpochMs,
        boolean evicted,
        String evictionDetail
) {
}
