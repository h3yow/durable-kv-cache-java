package com.github.h3yow.durablekvcachejava.web;

import com.github.h3yow.durablekvcachejava.core.CacheStore;
import com.github.h3yow.durablekvcachejava.model.StatusSnapshot;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;

/**
 * 本地可观测与运维入口：
 * <ul>
 *   <li>GET  /admin/status     —— 生命周期、容量占用、WAL 体积、计数、最近淘汰明细、恢复结果；</li>
 *   <li>POST /admin/checkpoint —— 同步快照压缩；</li>
 *   <li>POST /admin/purge      —— 立即清理过期键。</li>
 * </ul>
 */
@RestController
@RequestMapping("/admin")
public class AdminController {

    private final CacheStore cacheStore;

    public AdminController(CacheStore cacheStore) {
        this.cacheStore = cacheStore;
    }

    @GetMapping("/status")
    public StatusSnapshot status() {
        return cacheStore.status();
    }

    @PostMapping("/checkpoint")
    public Map<String, Object> checkpoint() {
        long lastSeq = cacheStore.checkpoint();
        return Map.of("ok", true, "lastCheckpointSequence", lastSeq, "at", Instant.now().toEpochMilli());
    }

    @PostMapping("/purge")
    public Map<String, Object> purge() {
        int removed = cacheStore.purgeExpired();
        return Map.of("ok", true, "removed", removed, "at", Instant.now().toEpochMilli());
    }
}
