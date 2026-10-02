package com.github.h3yow.durablekvcachejava.web;

import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.CapacityExceededException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.InvalidRequestException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.PersistenceException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.RecoveryException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.StateConflictException;
import com.github.h3yow.durablekvcachejava.cache.GroupIdConflictException;
import com.github.h3yow.durablekvcachejava.cache.GroupStaleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 异常到 HTTP 状态码的可区分映射：
 * 400 非法请求 / 409 状态冲突（恢复中、重复恢复、groupId 串组）/
 * 412 整组前置不满足（键已过期/已删除）/ 507 容量超限 /
 * 503 恢复失败 / 500 持久化失败。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(InvalidRequestException.class)
    public ResponseEntity<Map<String, Object>> invalid(InvalidRequestException e) {
        return error(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", e.getMessage(), null);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<Map<String, Object>> badBody(HttpMessageNotReadableException e) {
        return error(HttpStatus.BAD_REQUEST, "INVALID_REQUEST",
                "请求体不是合法 JSON 或字段类型错误: " + e.getMostSpecificCause().getMessage(), null);
    }

    @ExceptionHandler(GroupStaleException.class)
    public ResponseEntity<Map<String, Object>> stale(GroupStaleException e) {
        Map<String, Object> extra = new LinkedHashMap<>();
        extra.put("groupId", e.groupId());
        extra.put("staleKeys", e.staleKeys());
        return error(HttpStatus.PRECONDITION_FAILED, "GROUP_MEMBER_STALE", e.getMessage(), extra);
    }

    @ExceptionHandler(GroupIdConflictException.class)
    public ResponseEntity<Map<String, Object>> groupConflict(GroupIdConflictException e) {
        Map<String, Object> extra = new LinkedHashMap<>();
        extra.put("groupId", e.groupId());
        return error(HttpStatus.CONFLICT, "GROUP_ID_CONFLICT", e.getMessage(), extra);
    }

    @ExceptionHandler(CapacityExceededException.class)
    public ResponseEntity<Map<String, Object>> capacity(CapacityExceededException e) {
        Map<String, Object> extra = new LinkedHashMap<>();
        extra.put("reason", e.reason());
        return error(HttpStatus.INSUFFICIENT_STORAGE, "CAPACITY_EXCEEDED", e.getMessage(), extra);
    }

    @ExceptionHandler(StateConflictException.class)
    public ResponseEntity<Map<String, Object>> conflict(StateConflictException e) {
        return error(HttpStatus.CONFLICT, "STATE_CONFLICT", e.getMessage(), null);
    }

    @ExceptionHandler(RecoveryException.class)
    public ResponseEntity<Map<String, Object>> recovery(RecoveryException e) {
        return error(HttpStatus.SERVICE_UNAVAILABLE, "RECOVERY_FAILED", e.getMessage(), null);
    }

    @ExceptionHandler(PersistenceException.class)
    public ResponseEntity<Map<String, Object>> persistence(PersistenceException e) {
        log.error("持久化失败", e);
        return error(HttpStatus.INTERNAL_SERVER_ERROR, "PERSISTENCE_FAILED", e.getMessage(), null);
    }

    private static ResponseEntity<Map<String, Object>> error(HttpStatus status, String code,
                                                             String message, Map<String, Object> extra) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "ERROR");
        body.put("errorCode", code);
        body.put("httpStatus", status.value());
        body.put("message", message);
        if (extra != null) {
            body.putAll(extra);
        }
        return ResponseEntity.status(status).body(body);
    }
}
