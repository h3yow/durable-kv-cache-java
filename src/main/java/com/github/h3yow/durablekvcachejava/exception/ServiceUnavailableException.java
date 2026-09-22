package com.github.h3yow.durablekvcachejava.exception;

/** 服务尚未完成恢复或已失败，拒绝对外服务。 */
public class ServiceUnavailableException extends CacheException {
    public ServiceUnavailableException(String message) { super(message); }
    @Override public String errorCode() { return "SERVICE_UNAVAILABLE"; }
}
