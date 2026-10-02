package com.github.h3yow.durablekvcachejava.web;

import com.github.h3yow.durablekvcachejava.cache.KeyValueCache;
import com.github.h3yow.durablekvcachejava.cache.KeyValueCache.PutResult;
import com.github.h3yow.durablekvcachejava.cache.CacheExceptions.InvalidRequestException;
import com.github.h3yow.durablekvcachejava.core.GroupCommitResult;
import com.github.h3yow.durablekvcachejava.core.GroupOperation;
import com.github.h3yow.durablekvcachejava.core.OperationType;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 缓存 HTTP 入口。所有语义判定均下沉到 {@link KeyValueCache} 的单一状态机，
 * HTTP 层只做协议转换，保证各入口结论一致。
 */
@RestController
@RequestMapping("/cache")
public class CacheController {

    private final KeyValueCache cache;

    public CacheController(KeyValueCache cache) {
        this.cache = cache;
    }

    /**
     * 写入：PUT /cache/keys/{key}?ttlMillis=5000，body 为原始 UTF-8 文本值。
     * 成功返回 200，body 为 JSON 字符串；非法请求 400，容量问题 507，持久化失败 500。
     */
    @PutMapping("/keys/{key}")
    public ResponseEntity<Map<String, Object>> put(@PathVariable String key,
                                                   HttpServletRequest request,
                                                   @RequestParam(name = "ttlMillis", required = false)
                                                   Long ttlMillis) throws IOException {
        // 直接读取原始请求体（UTF-8 文本值），不依赖 Content-Type，避免表单解析/消息转换器差异。
        String body;
        try (InputStream in = request.getInputStream()) {
            body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
        if (body.isEmpty()) {
            body = null; // 空 body 交由状态机按"value 不得为 null"明确拒绝
        }
        PutResult result = cache.put(key, body, ttlMillis);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("status", "OK");
        resp.put("key", result.key());
        resp.put("expireAtMs", result.expireAtMs());
        resp.put("evicted", result.evicted());
        resp.put("evictedKey", result.evictedKey());
        resp.put("liveEntries", result.liveEntries());
        return ResponseEntity.ok(resp);
    }

    /** 读取：命中 200 返回 JSON；未命中/过期 404，body 给出判定依据。 */
    @GetMapping("/keys/{key}")
    public ResponseEntity<Map<String, Object>> get(@PathVariable String key) {
        String value = cache.get(key);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("key", key);
        if (value == null) {
            resp.put("status", "NOT_FOUND");
            resp.put("reason", "键不存在或已过期（详见服务端判定日志）");
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(resp);
        }
        resp.put("status", "OK");
        resp.put("value", value);
        return ResponseEntity.ok(resp);
    }

    /** 删除：200 返回是否实际删除（幂等）。 */
    @DeleteMapping("/keys/{key}")
    public Map<String, Object> delete(@PathVariable String key) {
        boolean existed = cache.delete(key);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("status", "OK");
        resp.put("key", key);
        resp.put("existed", existed);
        resp.put("metrics", cache.metrics());
        return resp;
    }

    /** 容量占用与淘汰结果的本地观测端点。 */
    @GetMapping("/admin/metrics")
    public KeyValueCache.Metrics metrics() {
        return cache.metrics();
    }

    /**
     * 整组原子提交：POST /cache/groups?groupId=可选，body 为成员操作数组的 JSON。
     * 成功 200 返回 {@code outcome=COMMITTED/DUPLICATE} 及淘汰键、序号、存活键数；
     * 参数错误 400、成员已过期/已删除 412、整组容量不足 507、持久化失败 500。
     */
    @PostMapping("/groups")
    public ResponseEntity<Map<String, Object>> commitGroup(
            @RequestParam(name = "groupId", required = false) String groupId,
            @RequestBody GroupCommitRequest request) {
        if (request == null || request.getMembers() == null) {
            throw new InvalidRequestException("请求体必须包含非空 members 数组");
        }
        List<GroupOperation> members = new java.util.ArrayList<>(request.getMembers().size());
        for (GroupCommitRequest.Member m : request.getMembers()) {
            if (m == null) {
                throw new InvalidRequestException("members 中不得包含 null 元素");
            }
            if ("PUT".equalsIgnoreCase(m.getOp())) {
                members.add(GroupOperation.put(m.getKey(), m.getValue(),
                        m.getTtlMillis() == null ? 0L : m.getTtlMillis()));
            } else if ("DELETE".equalsIgnoreCase(m.getOp())) {
                members.add(GroupOperation.delete(m.getKey()));
            } else {
                throw new InvalidRequestException(
                        "不支持的成员操作 op=" + m.getOp() + "，仅支持 PUT / DELETE");
            }
        }
        GroupCommitResult result = cache.commitGroup(groupId, members);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("status", "OK");
        resp.put("groupId", result.groupId());
        resp.put("outcome", result.outcome().name());
        resp.put("duplicate", result.duplicate());
        resp.put("memberCount", result.memberCount());
        resp.put("evictedKeys", result.evictedKeys());
        resp.put("beginSeq", result.beginSeq());
        resp.put("commitSeq", result.commitSeq());
        resp.put("liveEntries", result.liveEntries());
        return ResponseEntity.ok(resp);
    }
}
