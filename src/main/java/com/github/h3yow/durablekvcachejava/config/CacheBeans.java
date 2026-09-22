package com.github.h3yow.durablekvcachejava.config;

import com.github.h3yow.durablekvcachejava.cache.KeyValueCache;
import com.github.h3yow.durablekvcachejava.core.DurableKvCache;
import com.github.h3yow.durablekvcachejava.eviction.LruEvictionTracker;
import com.github.h3yow.durablekvcachejava.persistence.FileWalLog;
import com.github.h3yow.durablekvcachejava.persistence.PersistenceLog;
import com.github.h3yow.durablekvcachejava.recovery.RecoveryRunner;
import com.github.h3yow.durablekvcachejava.recovery.RecoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 缓存组件装配。
 *
 * <p>装配顺序即恢复屏障：WAL 创建 → 空状态机构造（未就绪）→ 恢复重放 → markReady →
 * 暴露为 Bean。恢复过程中任意异常都会阻止 Bean 注册，Controller 无法获得缓存实例，
 * 因此不存在"半初始化数据对外可见"的窗口。</p>
 */
@Configuration
public class CacheBeans {

    private static final Logger log = LoggerFactory.getLogger(CacheBeans.class);

    @Bean(destroyMethod = "close")
    public PersistenceLog persistenceLog(CacheProperties properties) {
        log.info("初始化 WAL: dataDir={}", properties.getDataDir());
        return new FileWalLog(properties.getDataDir(), "cache.wal");
    }

    @Bean
    public KeyValueCache keyValueCache(PersistenceLog wal, CacheProperties properties) {
        return recovered(wal, properties).cache();
    }

    @Bean
    public RecoveryRunner recoveryRunner(PersistenceLog wal, CacheProperties properties) {
        // 恢复只执行一次；Runner 与缓存共享同一份真实恢复结果。
        return new RecoveryRunner(recovered(wal, properties).result());
    }

    private record Recovered(DurableKvCache cache, RecoveryService.RecoveryResult result) {
    }

    private volatile Recovered singleton;

    private synchronized Recovered recovered(PersistenceLog wal, CacheProperties properties) {
        if (singleton != null) {
            return singleton;
        }
        DurableKvCache cache = new DurableKvCache(wal, new LruEvictionTracker(), properties);
        RecoveryService.RecoveryResult result = new RecoveryService(wal, cache).recover();
        singleton = new Recovered(cache, result);
        return singleton;
    }
}
