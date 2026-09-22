package com.github.h3yow.durablekvcachejava.model;

/** 删除结果：deleted 表示是否有有效条目被移除，reason 为判定依据。 */
public record DeleteResult(String key, boolean deleted, long version, String reason) {
}
