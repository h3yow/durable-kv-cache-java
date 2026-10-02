package com.github.h3yow.durablekvcachejava.web;

import com.github.h3yow.durablekvcachejava.cache.GroupAction;
import com.github.h3yow.durablekvcachejava.cache.GroupMember;
import com.github.h3yow.durablekvcachejava.cache.GroupResult;
import com.github.h3yow.durablekvcachejava.cache.KeyValueCache;
import com.github.h3yow.durablekvcachejava.cache.KeyValueCache.PutResult;
import com.github.h3yow.durablekvcachejava.web.dto.GroupCommitRequest;
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
import java.util.ArrayList;
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
     * 整组原子提交：POST /cache/groups/{groupId}/commit。
     * 组内成员全部生效或全部不生效；相同 groupId + 相同成员的重复提交幂等。
     * 成功 200；参数错误 400；前置不满足（键已过期/已删除/INSERT 已存在）412；
     * groupId 串组冲突 409；容量装不下 507；持久化失败 500。
     */
    @PostMapping("/groups/{groupId}/commit")
    public ResponseEntity<Map<String, Object>> commitGroup(@PathVariable String groupId,
                                                           @RequestBody GroupCommitRequest body) {
        List<GroupMember> members = new ArrayList<>();
        if (body == null || body.getMembers() == null) {
            members = List.of(); // 交给状态机按"members 不得为空"明确拒绝
        } else {
            for (GroupCommitRequest.Member m : body.getMembers()) {
                GroupAction action;
                try {
                    action = GroupAction.valueOf(m.getAction());
                } catch (Exception e) {
                    throw new com.github.h3yow.durablekvcachejava.cache.CacheExceptions
                            .InvalidRequestException("非法 action: " + m.getAction()
                            + "（允许 INSERT/UPDATE/DELETE，key=" + m.getKey() + "）");
                }
                members.add(new GroupMember(m.getKey(), action, m.getValue(), m.getTtlMillis()));
            }
        }
        GroupResult result = cache.commitGroup(groupId, members);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("status", result.duplicate() ? "OK_DUPLICATE" : "OK");
        resp.put("groupId", result.groupId());
        resp.put("duplicate", result.duplicate());
        resp.put("insertedKeys", result.insertedKeys());
        resp.put("updatedKeys", result.updatedKeys());
        resp.put("deletedKeys", result.deletedKeys());
        resp.put("evictedKeys", result.evictedKeys());
        resp.put("liveEntries", result.liveEntries());
        resp.put("seqRange", List.of(result.beginSeq(), result.endSeq()));
        return ResponseEntity.ok(resp);
    }

    /** 查询某个 groupId 的已确认提交结论；从未确认提交 404。 */
    @GetMapping("/groups/{groupId}")
    public ResponseEntity<Map<String, Object>> groupStatus(@PathVariable String groupId) {
        GroupResult result = cache.groupStatus(groupId);
        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("groupId", groupId);
        if (result == null) {
            resp.put("status", "NOT_FOUND");
            resp.put("reason", "该 groupId 从未确认提交成功（或提交中途失败被整组回滚）");
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(resp);
        }
        resp.put("status", "OK");
        resp.put("insertedKeys", result.insertedKeys());
        resp.put("updatedKeys", result.updatedKeys());
        resp.put("deletedKeys", result.deletedKeys());
        resp.put("evictedKeys", result.evictedKeys());
        resp.put("liveEntries", result.liveEntries());
        resp.put("seqRange", List.of(result.beginSeq(), result.endSeq()));
        return ResponseEntity.ok(resp);
    }
}
