package com.github.h3yow.durablekvcachejava.model;

import jakarta.annotation.Nullable;

/** 对外返回的条目视图。 */
public record EntryView(
        String key,
        @Nullable String value,
        long version,
        long sequence,
        @Nullable Long expireAtEpochMs,
        @Nullable Long ttlRemainingMs
) {
}
