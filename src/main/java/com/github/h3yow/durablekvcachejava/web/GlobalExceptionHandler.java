package com.github.h3yow.durablekvcachejava.web;

import com.github.h3yow.durablekvcachejava.dto.ErrorResponse;
import com.github.h3yow.durablekvcachejava.exception.CapacityExceededException;
import com.github.h3yow.durablekvcachejava.exception.IllegalConfigurationException;
import com.github.h3yow.durablekvcachejava.exception.InvalidRequestException;
import com.github.h3yow.durablekvcachejava.exception.PersistenceException;
import com.github.h3yow.durablekvcachejava.exception.RecoveryException;
import com.github.h3yow.durablekvcachejava.exception.ServiceUnavailableException;
import com.github.h3yow.durablekvcachejava.exception.VersionConflictException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.Instant;

/**
 * 错误映射（错误码可区分，状态码语义明确）：
 * <pre>
 *  400 INVALID_REQUEST / MALFORMED_BODY / ILLEGAL_CONFIGURATION
 *  409 VERSION_CONFLICT
 *  429 CAPACITY_EXCEEDED
 *  503 SERVICE_UNAVAILABLE（恢复中/失败）
 *  500 RECOVERY_FAILURE:<reason> / PERSISTENCE_FAILURE / INTERNAL
 * </pre>
 * 恢复失败携带具体原因码（CHECKSUM_MISMATCH / TRUNCATED_RECORD /
 * SNAPSHOT_CORRUPT / UNSUPPORTED_FORMAT / IO_ERROR）。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(InvalidRequestException.class)
    public ResponseEntity<ErrorResponse> invalid(InvalidRequestException ex) {
        return build(HttpStatus.BAD_REQUEST, ex, null);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> unreadable(HttpMessageNotReadableException ex) {
        ErrorResponse body = new ErrorResponse("MALFORMED_BODY",
                "request body is missing or not valid JSON", null, Instant.now());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(body);
    }

    @ExceptionHandler(IllegalConfigurationException.class)
    public ResponseEntity<ErrorResponse> badConfig(IllegalConfigurationException ex) {
        return build(HttpStatus.BAD_REQUEST, ex, null);
    }

    @ExceptionHandler(VersionConflictException.class)
    public ResponseEntity<ErrorResponse> conflict(VersionConflictException ex) {
        return build(HttpStatus.CONFLICT, ex, null);
    }

    @ExceptionHandler(CapacityExceededException.class)
    public ResponseEntity<ErrorResponse> capacity(CapacityExceededException ex) {
        return build(HttpStatus.TOO_MANY_REQUESTS, ex, null);
    }

    @ExceptionHandler(ServiceUnavailableException.class)
    public ResponseEntity<ErrorResponse> unavailable(ServiceUnavailableException ex) {
        return build(HttpStatus.SERVICE_UNAVAILABLE, ex, null);
    }

    @ExceptionHandler(RecoveryException.class)
    public ResponseEntity<ErrorResponse> recovery(RecoveryException ex) {
        return build(HttpStatus.INTERNAL_SERVER_ERROR, ex, null);
    }

    @ExceptionHandler(PersistenceException.class)
    public ResponseEntity<ErrorResponse> persistence(PersistenceException ex) {
        return build(HttpStatus.INTERNAL_SERVER_ERROR, ex, null);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> other(Exception ex) {
        ErrorResponse body = new ErrorResponse("INTERNAL",
                ex.getClass().getSimpleName() + ": " + ex.getMessage(), null, Instant.now());
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(body);
    }

    private ResponseEntity<ErrorResponse> build(HttpStatus status,
                                                com.github.h3yow.durablekvcachejava.exception.CacheException ex,
                                                String key) {
        return ResponseEntity.status(status)
                .body(new ErrorResponse(ex.errorCode(), ex.getMessage(), key, Instant.now()));
    }
}
