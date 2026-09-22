package com.github.h3yow.durablekvcachejava.config;

import jakarta.annotation.PostConstruct;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 缓存配置（前缀 kv-cache）。
 */
@ConfigurationProperties(prefix = "kv-cache")
public class CacheProperties {

    @PostConstruct
    void validate() {
        if (dataDir == null || dataDir.isBlank()) {
            throw new IllegalStateException("非法配置 kv-cache.data-dir: 不得为空");
        }
        if (maxValueBytes <= 0) {
            throw new IllegalStateException("非法配置 kv-cache.max-value-bytes: 必须为正数: " + maxValueBytes);
        }
        if (maxKeyLength <= 0) {
            throw new IllegalStateException("非法配置 kv-cache.max-key-length: 必须为正数: " + maxKeyLength);
        }
    }

    /** WAL 文件目录。 */
    private String dataDir = "data";

    /** 最大键数量，<=0 表示不限制。 */
    private int maxEntries = 1000;

    /** 单次写入值的最大字节数（按 UTF-8 计）。 */
    private int maxValueBytes = 64 * 1024;

    /** 键最大长度。 */
    private int maxKeyLength = 256;

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public int getMaxEntries() {
        return maxEntries;
    }

    public void setMaxEntries(int maxEntries) {
        this.maxEntries = maxEntries;
    }

    public int getMaxValueBytes() {
        return maxValueBytes;
    }

    public void setMaxValueBytes(int maxValueBytes) {
        this.maxValueBytes = maxValueBytes;
    }

    public int getMaxKeyLength() {
        return maxKeyLength;
    }

    public void setMaxKeyLength(int maxKeyLength) {
        this.maxKeyLength = maxKeyLength;
    }
}
