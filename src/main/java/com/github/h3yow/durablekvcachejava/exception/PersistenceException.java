package com.github.h3yow.durablekvcachejava.exception;

/** 持久化 IO 失败（写入/ fsync 错误）。 */
public class PersistenceException extends CacheException {
    public PersistenceException(String message, Throwable cause) { super(message, cause); }
    @Override public String errorCode() { return "PERSISTENCE_FAILURE"; }
}
