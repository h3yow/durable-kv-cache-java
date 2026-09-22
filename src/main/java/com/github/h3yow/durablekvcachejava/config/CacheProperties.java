package com.github.h3yow.durablekvcachejava.config;

import java.nio.file.Path;

import com.github.h3yow.durablekvcachejava.exception.IllegalConfigurationException;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * 缓存服务配置项（cache.* 前缀）。
 *
 * <ul>
 *   <li>{@code cache.data-dir}：持久化目录（默认 data/cache）；</li>
 *   <li>{@code cache.max-entries}：容量上限（键数量），必须 &gt;= 1；</li>
 *   <li>{@code cache.max-value-bytes}：单值 UTF-8 字节上限，必须 &gt;= 1；</li>
 *   <li>{@code cache.eviction}：EVICT_LRU（默认，超限先淘汰）或 REJECT（超限拒绝新键）；</li>
 *   <li>{@code cache.fsync-each-write}：每条确认写入是否 fsync（默认 true）；</li>
 *   <li>{@code cache.wal-compaction-bytes}：活动 WAL 超过该字节触发快照压缩，&lt;=0 关闭自动压缩；</li>
 *   <li>{@code cache.cleanup-interval-ms}：后台过期清理周期，必须 &gt; 0。</li>
 * </ul>
 */
@Configuration
@ConfigurationProperties(prefix = "cache")
public class CacheProperties {

    public enum EvictionMode {
        /** 容量超限时按确定顺序淘汰（先过期键，再最久未访问，最后按键名字典序）。 */
        EVICT_LRU,
        /** 容量超限时拒绝新键写入（CAPACITY_EXCEEDED）。 */
        REJECT
    }

    private Path dataDir = Path.of("data", "cache");
    private int maxEntries = 10_000;
    private long maxValueBytes = 1L << 20;
    private EvictionMode eviction = EvictionMode.EVICT_LRU;
    private boolean fsyncEachWrite = true;
    private long walCompactionBytes = 16L << 20;
    private long cleanupIntervalMs = 1_000L;

    /** 启动期一次性校验，非法配置直接抛出 ILLEGAL_CONFIGURATION，服务不进入恢复。 */
    public void validate() {
        if (dataDir == null) {
            throw new IllegalConfigurationException("cache.data-dir must not be null");
        }
        if (maxEntries < 1) {
            throw new IllegalConfigurationException(
                    "cache.max-entries must be >= 1 but got " + maxEntries);
        }
        if (maxValueBytes < 1) {
            throw new IllegalConfigurationException(
                    "cache.max-value-bytes must be >= 1 but got " + maxValueBytes);
        }
        if (eviction == null) {
            throw new IllegalConfigurationException("cache.eviction must be EVICT_LRU or REJECT");
        }
        if (cleanupIntervalMs <= 0) {
            throw new IllegalConfigurationException(
                    "cache.cleanup-interval-ms must be > 0 but got " + cleanupIntervalMs);
        }
        if (walCompactionBytes < 0) {
            throw new IllegalConfigurationException(
                    "cache.wal-compaction-bytes must be >= 0 (0 disables auto compaction) but got "
                            + walCompactionBytes);
        }
    }

    public Path getDataDir() { return dataDir; }
    public void setDataDir(Path dataDir) { this.dataDir = dataDir; }

    public int getMaxEntries() { return maxEntries; }
    public void setMaxEntries(int maxEntries) { this.maxEntries = maxEntries; }

    public long getMaxValueBytes() { return maxValueBytes; }
    public void setMaxValueBytes(long maxValueBytes) { this.maxValueBytes = maxValueBytes; }

    public EvictionMode getEviction() { return eviction; }
    public void setEviction(EvictionMode eviction) { this.eviction = eviction; }

    public boolean isFsyncEachWrite() { return fsyncEachWrite; }
    public void setFsyncEachWrite(boolean fsyncEachWrite) { this.fsyncEachWrite = fsyncEachWrite; }

    public long getWalCompactionBytes() { return walCompactionBytes; }
    public void setWalCompactionBytes(long walCompactionBytes) { this.walCompactionBytes = walCompactionBytes; }

    public long getCleanupIntervalMs() { return cleanupIntervalMs; }
    public void setCleanupIntervalMs(long cleanupIntervalMs) { this.cleanupIntervalMs = cleanupIntervalMs; }
}
