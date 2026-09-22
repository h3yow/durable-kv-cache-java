package com.github.h3yow.durablekvcachejava.exception;

/** 启动时发现非法配置。 */
public class IllegalConfigurationException extends CacheException {
    public IllegalConfigurationException(String message) { super(message); }
    @Override public String errorCode() { return "ILLEGAL_CONFIGURATION"; }
}
