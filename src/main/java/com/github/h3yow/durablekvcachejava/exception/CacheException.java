package com.github.h3yow.durablekvcachejava.exception;

/** 所有可区分业务/状态错误的基类。 */
public abstract class CacheException extends RuntimeException {
    protected CacheException(String message) { super(message); }
    protected CacheException(String message, Throwable cause) { super(message, cause); }

    /** 机器可区分的错误码。 */
    public abstract String errorCode();
}
