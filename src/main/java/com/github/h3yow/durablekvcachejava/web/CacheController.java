package com.github.h3yow.durablekvcachejava.web;

import com.github.h3yow.durablekvcachejava.core.CacheStore;
import com.github.h3yow.durablekvcachejava.dto.DeleteResponse;
import com.github.h3yow.durablekvcachejava.dto.GetResponse;
import com.github.h3yow.durablekvcachejava.dto.PutRequest;
import com.github.h3yow.durablekvcachejava.dto.PutResponse;
import com.github.h3yow.durablekvcachejava.model.CacheEntry;
import com.github.h3yow.durablekvcachejava.model.DeleteResult;
import com.github.h3yow.durablekvcachejava.model.EntryView;
import org.springframework.web.bind.annotation.*;

/**
 * 键值缓存入口。所有过期/版本判定都由服务端 {@link CacheStore} 统一给出，
 * 响应中的 reason 字段给出判定依据，保证结论可解释。
 */
@RestController
@RequestMapping("/cache")
public class CacheController {

    private final CacheStore cacheStore;

    public CacheController(CacheStore cacheStore) {
        this.cacheStore = cacheStore;
    }

    @PutMapping("/{key}")
    public PutResponse put(@PathVariable String key, @RequestBody(required = false) PutRequest request) {
        PutRequest body = request == null ? new PutRequest(null, null, null, null) : request;
        Long ttl = body.ttlMillis();
        if (ttl == null && body.ttlSeconds() != null) {
            ttl = body.ttlSeconds() * 1000L;
        }
        CacheEntry saved = cacheStore.put(key, body.value(), ttl, body.expectedVersion());
        long expireAt = saved.expireAtEpochMs() == CacheEntry.NO_EXPIRY ? -1L : saved.expireAtEpochMs();
        String detail = "committed under write lock; sequence=" + saved.sequence()
                + " version=" + saved.version()
                + (saved.expireAtEpochMs() == CacheEntry.NO_EXPIRY ? "; no expiry" : "; server-side expiry");
        return new PutResponse(key, saved.version(), saved.sequence(), expireAt, false, detail);
    }

    @GetMapping("/{key}")
    public GetResponse get(@PathVariable String key) {
        EntryView view = cacheStore.get(key);
        if (view.value() != null) {
            return new GetResponse(key, true, false, view.value(), view.version(),
                    view.expireAtEpochMs(), view.ttlRemainingMs(),
                    "hit: live entry at server-side now");
        }
        // 区分两种不存在：EntryView.version() < 0 表示从未有过键；
        // 其余情况（刚过期、被惰性清理或已删除）能拿到 expireAt 时明确标记为过期
        boolean expired = view.ttlRemainingMs() != null
                || (view.expireAtEpochMs() != null && view.expireAtEpochMs() != CacheEntry.NO_EXPIRY);
        long version = view.version();
        String reason = expired
                ? "not found: expired at " + view.expireAtEpochMs()
                : "not found: no such key";
        return new GetResponse(key, false, expired, null, version < 0 ? null : version,
                view.expireAtEpochMs(), null, reason);
    }

    @DeleteMapping("/{key}")
    public DeleteResponse delete(@PathVariable String key,
                                 @RequestParam(required = false) Long expectedVersion) {
        DeleteResult result = cacheStore.delete(key, expectedVersion);
        return new DeleteResponse(result.key(), result.deleted(), result.version(), result.reason());
    }
}
