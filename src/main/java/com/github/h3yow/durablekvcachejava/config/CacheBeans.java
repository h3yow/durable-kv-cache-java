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
 * <p>装配顺序即恢复屏障：WAL 创建 → 空状态机构造（未就绪）→ 恢复重放（整个进程仅执行一次）
 * → markReady → 把同一个已恢复实例同时暴露给业务 Bean 与启动确认 Runner。恢复过程中任意异常
 * 都会阻止 Bean 注册，Controller 无法获得缓存实例，因此不存在"半初始化数据对外可见"的窗口。</p>
 *
 * <p>注意：{@link KeyValueCache} 与 {@link RecoveryRunner} 必须共享<b>同一个</b>状态机实例和
 * 同一份恢复结果；绝不能各自重放同一 WAL，否则整组 groupId 去重表等内存状态会被重复构建、
 * 序号与计数也会错乱。</p>
 */
@Configuration
public class CacheBeans {

    private static final Logger log = LoggerFactory.getLogger(CacheBeans.class);

    /** 一次恢复的产物：状态机实例 + 恢复摘要。 */
    private record Recovered(DurableKvCache cache, RecoveryService.RecoveryResult result) {
    }

    @Bean(destroyMethod = "close")
    public PersistenceLog persistenceLog(CacheProperties properties) {
        log.info("初始化 WAL: dataDir={}", properties.getDataDir());
        return new FileWalLog(properties.getDataDir(), "cache.wal");
    }

    /** 单一恢复入口：整个进程只构造一个状态机、只重放一次 WAL。 */
    @Bean
    public Recovered recoveredCache(PersistenceLog wal, CacheProperties properties) {
        DurableKvCache cache = new DurableKvCache(wal, new LruEvictionTracker(), properties);
        RecoveryService.RecoveryResult result = new RecoveryService(wal, cache).recover();
        return new Recovered(cache, result);
    }

    @Bean
    public KeyValueCache keyValueCache(Recovered recovered) {
        return recovered.cache();
    }

    @Bean
    public RecoveryRunner recoveryRunner(Recovered recovered) {
        return new RecoveryRunner(recovered.result());
    }
}
