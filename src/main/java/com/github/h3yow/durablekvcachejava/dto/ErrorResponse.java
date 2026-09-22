package com.github.h3yow.durablekvcachejava.dto;

import java.time.Instant;

/** 统一错误响应：error 为可区分错误码，reason 为人类可读判定依据。 */
public record ErrorResponse(String error, String reason, String key, Instant at) {
}
