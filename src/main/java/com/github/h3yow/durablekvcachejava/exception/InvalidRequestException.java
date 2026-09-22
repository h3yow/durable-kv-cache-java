package com.github.h3yow.durablekvcachejava.exception;

/** 非法参数（空键、TTL 非法、值超限等），错误码非法_* 可区分。 */
public class InvalidRequestException extends CacheException {
    public InvalidRequestException(String message) { super(message); }
    @Override public String errorCode() { return "INVALID_REQUEST"; }
}
