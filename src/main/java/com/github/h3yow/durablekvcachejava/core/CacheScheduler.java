package com.github.h3yow.durablekvcachejava.core;

import com.github.h3yow.durablekvcachejava.config.CacheProperties;
import com.github.h3yow.durablekvcachejava.model.LifecycleState;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 生命周期与后台调度：
 * <ol>
 *   <li>应用就绪前缓存处于 CREATED（数据入口一律 503），不暴露任何半初始化数据；</li>
 *   <li>收到 {@link ApplicationReadyEvent} 后执行一次性恢复：成功转 ACTIVE，
 *       失败转 FAILED 并保留可区分原因（不会启动后台调度，绝不静默继续）；</li>
 *   <li>ACTIVE 后按配置周期清理过期键。</li>
 * </ol>
 */
@Component
public class CacheScheduler {

    private static final Logger log = LoggerFactory.getLogger(CacheScheduler.class);

    private final CacheStore cacheStore;
    private final CacheProperties props;
    private final AtomicBoolean recovered = new AtomicBoolean(false);
    private ScheduledExecutorService executor;

    public CacheScheduler(CacheStore cacheStore, CacheProperties props) {
        this.cacheStore = cacheStore;
        this.props = props;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        startup();
    }

    /** 执行恢复并（成功时）启动后台调度。幂等：重复调用不会重复恢复。 */
    public synchronized void startup() {
        if (!recovered.compareAndSet(false, true)) {
            log.info("[startup] recovery already attempted, state={}", cacheStore.state());
            return;
        }
        // 对外语义：恢复开始
        try {
            cacheStore.recover();
        } catch (RuntimeException e) {
            // CacheStore 已置 FAILED；这里不再启动调度，错误可经 /admin/status 观测
            log.error("[startup] recovery failed, service stays FAILED: {}", e.getMessage());
            return;
        }
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cache-cleanup");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(this::safePurge,
                props.getCleanupIntervalMs(), props.getCleanupIntervalMs(), TimeUnit.MILLISECONDS);
        log.info("[startup] cache ACTIVE, cleanup interval={}ms", props.getCleanupIntervalMs());
    }

    private void safePurge() {
        try {
            cacheStore.purgeExpired();
        } catch (RuntimeException e) {
            log.warn("[cleanup] purge failed: {}", e.getMessage(), e);
        }
    }

    public boolean isActive() {
        return cacheStore.state() == LifecycleState.ACTIVE;
    }

    @PreDestroy
    public void stop() {
        if (executor != null) {
            executor.shutdownNow();
        }
        cacheStore.shutdown();
    }
}
