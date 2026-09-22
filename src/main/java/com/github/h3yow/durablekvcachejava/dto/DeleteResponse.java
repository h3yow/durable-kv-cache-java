package com.github.h3yow.durablekvcachejava.dto;

/** 删除响应。 */
public record DeleteResponse(String key, boolean deleted, long version, String reason) {
}
