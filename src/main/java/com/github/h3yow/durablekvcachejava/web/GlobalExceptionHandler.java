package com.github.h3yow.durablekvcachejava.web;

import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.CapacityExceededException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.InvalidRequestException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.PersistenceException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.RecoveryException;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.StateConflictException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 异常到 HTTP 状态码的可区分映射：
 * 400 非法请求 / 409 状态冲突（恢复中、重复恢复）/ 507 容量超限 /
 * 503 恢复失败 / 500 持久化失败。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(InvalidRequestException.class)
    public ResponseEntity<Map<String, Object>> invalid(InvalidRequestException e) {
        return error(HttpStatus.BAD_REQUEST, "INVALID_REQUEST", e.getMessage());
    }

    @ExceptionHandler(CapacityExceededException.class)
    public ResponseEntity<Map<String, Object>> capacity(CapacityExceededException e) {
        return error(HttpStatus.INSUFFICIENT_STORAGE, "CAPACITY_EXCEEDED", e.getMessage());
    }

    @ExceptionHandler(StateConflictException.class)
    public ResponseEntity<Map<String, Object>> conflict(StateConflictException e) {
        return error(HttpStatus.CONFLICT, "STATE_CONFLICT", e.getMessage());
    }

    @ExceptionHandler(RecoveryException.class)
    public ResponseEntity<Map<String, Object>> recovery(RecoveryException e) {
        return error(HttpStatus.SERVICE_UNAVAILABLE, "RECOVERY_FAILED", e.getMessage());
    }

    @ExceptionHandler(PersistenceException.class)
    public ResponseEntity<Map<String, Object>> persistence(PersistenceException e) {
        log.error("持久化失败", e);
        return error(HttpStatus.INTERNAL_SERVER_ERROR, "PERSISTENCE_FAILED", e.getMessage());
    }

    private static ResponseEntity<Map<String, Object>> error(HttpStatus status, String code,
                                                             String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "ERROR");
        body.put("errorCode", code);
        body.put("httpStatus", status.value());
        body.put("message", message);
        return ResponseEntity.status(status).body(body);
    }
}
