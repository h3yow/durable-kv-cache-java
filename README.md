# Durable KV Cache (Java / Spring Boot)

一个**本地持久化、可承受并发访问与进程异常重启**的键值缓存服务。所有结论（过期、淘汰、
删除、恢复）都有单一确定顺序、可在日志中解释，并通过本地可复现的测试覆盖正常、边界与
失败路径。不依赖任何外部服务或真实凭据。

## 1. 构建与运行

要求 JDK 21+、Maven 3.9+（仅需本地 Maven 仓库）。

```bash
mvn package -DskipTests
java -jar target/durable-kv-cache-java-1.0-SNAPSHOT.jar \
     --server.port=8080 \
     --kv-cache.data-dir=./data \
     --kv-cache.max-entries=1000
```

或使用开发模式：`mvn spring-boot:run`。

## 2. HTTP 接口

| 方法 | 路径 | 说明 |
|---|---|---|
| PUT | `/cache/keys/{key}?ttlMillis=<毫秒>` | 写入/重写，body 为**原始 UTF-8 文本值**（任意 Content-Type 均可） |
| GET | `/cache/keys/{key}` | 读取；命中 200，不存在/已过期 404 |
| DELETE | `/cache/keys/{key}` | 删除（幂等），`existed` 表示是否删除了现存键 |
| GET | `/cache/admin/metrics` | 容量占用、淘汰数、过期清理数等可观测指标 |

```bash
curl -X PUT "http://localhost:8080/cache/keys/user:1?ttlMillis=5000" --data-binary "alice"
curl "http://localhost:8080/cache/keys/user:1"
curl -X DELETE "http://localhost:8080/cache/keys/user:1"
curl "http://localhost:8080/cache/admin/metrics"
```

### 错误响应（原因可区分）

| HTTP | errorCode | 触发条件 |
|---|---|---|
| 400 | `INVALID_REQUEST` | 空键、null 值、缺 ttl、ttl<=0、值超长、键超长 |
| 409 | `STATE_CONFLICT` | 恢复未完成时访问、对同一实例重复恢复 |
| 507 | `CAPACITY_EXCEEDED` | 容量满且无确定可淘汰键 |
| 500 | `PERSISTENCE_FAILED` | WAL 追加/刷盘失败（本次写入整体失败，内存回滚） |
| 503 | `RECOVERY_FAILED` | WAL 校验失败等恢复错误（正常流程中该错误发生在启动期，应用直接中止） |

## 3. 过期语义

- 每次 PUT 必须带正整数 `ttlMillis`，到期绝对时间 `expireAt = 写入时刻 + ttl`，
  随记录一起返回。
- 判定规则统一为 `now >= expireAt` 即过期，**三个入口完全一致**：
  - GET：惰性判定，命中过期时在同一把锁内原子移除并返回 404；
  - PUT/DELETE：操作开始时顺带扫描清理已过期键。
- 过期键的重写立即生效，旧值不会复活；过期判定与删除/重写在同一监视器锁内串行，
  因此「过期、重写、删除并发发生」时只有**一个确定的全序结果**，绝不返回旧值。
- 过期依据（`now`、`expireAt`）逐条打印到服务端日志。
- 注意：这是单机墙钟语义；进程重启后，恢复重放同样用「当前时刻 vs 记录到期时间」判定，
  重启前已过期的键不会进入存活集。

## 4. 并发与一致性模型

- `DurableKvCache` 用**单一监视器锁**把「判定 → 变更内存 → 追加 WAL」串行化，
  HTTP 层不做任何状态判定。因此所有入口对同一键观察到同一全序。
- 写入次序：先淘汰（如有）后写入；淘汰与写入各自获得单调递增序号并先落盘，
  内存变更只在 WAL `force` 成功后提交。
- WAL 失败时本次操作内存整体回滚（含被淘汰键恢复），调用方得到 500，绝不会出现
  「响应失败但数据已变」或「响应成功但未落盘」。

## 5. 持久化与恢复保证

### WAL 格式

文件 `data/cache.wal`，每条一行（UTF-8，`\n` 结尾）：

```
seq|TYPE|key(base64)|value(base64)|ttl|timestamp|crc64
```

- `TYPE` ∈ `PUT / DELETE / EVICT`；
- `seq` 从 1 开始、严格连续递增；
- 行尾附带 FNV-1a 64 位校验和，覆盖除校验字段外的全部内容。

每次追加后执行 `FileChannel.force(true)`：**返回 200 的写入在 kill -9 后也不丢**。

### 恢复流程（启动期，在 Web 服务对外可用之前完成）

1. 顺序读取全部记录；末尾**无换行的半截行**被视为崩溃残留，可安全忽略并截除；
2. 完整行必须通过字段数与 CRC 校验，否则抛 `RecoveryException`，Spring Bean 创建失败、
   **启动中止（退出码非 0），绝不静默继续**；
3. 校验序号严格连续（缺号/重复同样中止，错误信息可区分）；
4. 按全序重放：`PUT` 覆盖（重复提交不叠加计数）、`DELETE`/`EVICT` 移除；
   恢复时刻已过期的 PUT 记为 `EXPIRED_SKIP` 且不进入存活集；
5. 全部成功后才 `markReady`；此前任何业务请求都得到 `409 STATE_CONFLICT`，
   **不暴露半初始化数据**；
6. 对同一缓存实例重复恢复会被拒绝（`STATE_CONFLICT`），防止内存叠加。

`EVICT` 也要持久化：容量淘汰是状态机全序的一部分，否则重启后被淘汰键会因历史 PUT
「复活」。WAL 追加失败导致淘汰已落盘而新 PUT 回滚时，会追加一条幂等 EVICT 补偿记录，
使 WAL 与内存重新一致。

恢复摘要（重放条数、PUT/DELETE/EVICT 计数、过期跳过数、最终 seq、存活键数）
打印在启动日志中。

## 6. 容量与淘汰规则

- 配置 `kv-cache.max-entries`（>0 生效，<=0 不限）。容量按**存活键数**计。
- 淘汰算法为**确定的 LRU**（access-order `LinkedHashMap`）：
  - PUT（新增键）与命中的 GET 都会把该键标记为最近使用；
  - 容量满时，链表头（最久未被触碰的键）被淘汰；
  - PUT 到已存在键（重写）不触发淘汰。
- 淘汰键、判定依据、淘汰前后容量均写日志；淘汰计入 `/cache/admin/metrics` 的
  `evictionCount`，重启恢复后该计数保持连续。
- 指标字段：`maxEntries, liveEntries, totalPutCount, totalDeleteCount, evictionCount,
  expiredScanCount, lastExpireAtMs, state(READY/RECOVERING)`。

### 配置项（均可通过同名环境变量覆盖）

| 属性 | 环境变量 | 默认 | 非法值 |
|---|---|---|---|
| `kv-cache.data-dir` | `KV_CACHE_DATA_DIR` | `data` | 空串 → 启动失败 |
| `kv-cache.max-entries` | `KV_CACHE_MAX_ENTRIES` | `1000` | 任意整数；<=0 表示不限 |
| `kv-cache.max-value-bytes` | `KV_CACHE_MAX_VALUE_BYTES` | `65536` | <=0 → 启动失败 |
| `kv-cache.max-key-length` | `KV_CACHE_MAX_KEY_LENGTH` | `256` | <=0 → 启动失败 |

## 7. 本地验证方法

### 一键测试

```bash
mvn test
```

22 个测试全部本地运行（WAL 均落在 JUnit `@TempDir`），不依赖外部服务：

| 测试类 | 覆盖内容 |
|---|---|
| `FileWalLogTest` | WAL 往返、崩溃半行截断、CRC 损坏可区分失败、序号递增 |
| `DurableKvCacheTest` | 读写删、TTL 过期、确定 LRU 淘汰、非法请求 4 类原因、未就绪访问拒绝、16 线程并发读重写、过期/重写/删除竞争（8 组）、指标 |
| `RecoveryTest` | kill 后重放（重写/删除/过期/删后重写）、淘汰结论跨重启保持、重复恢复拒绝、WAL 损坏中止、崩溃残留容忍 |
| `CacheControllerIntegrationTest` | HTTP 成功/404/400 错误码、metrics 端点 |
| `ProcessRestartSimulationTest` | 真实关闭并重建整个 Spring 上下文，验证已确认写入与删除跨「进程」保持 |

单测日志中打印了**键、判定依据（now/expireAt、LRU 位置、序号、错误原因）与恢复结果**，
可在 `target/surefire-reports/` 或控制台查看。

### 手工复现崩溃恢复

```bash
mvn package -DskipTests
rm -rf /tmp/kv && java -jar target/durable-kv-cache-java-1.0-SNAPSHOT.jar \
    --server.port=8085 --kv-cache.data-dir=/tmp/kv --kv-cache.max-entries=2 &
B=http://localhost:8085/cache
curl -s -X PUT "$B/keys/a?ttlMillis=600000" --data-binary alpha
curl -s -X PUT "$B/keys/b?ttlMillis=600000" --data-binary bravo
curl -s "$B/keys/a" >/dev/null                                   # 刷新 a，使 b 成为 LRU 最旧
curl -s -X PUT "$B/keys/c?ttlMillis=600000" --data-binary charlie # 淘汰 b（EVICT 落盘）
kill -9 $(ss -ltnp | grep 8085 | grep -oP 'pid=\K[0-9]+')        # 模拟异常终止
java -jar target/durable-kv-cache-java-1.0-SNAPSHOT.jar \
    --server.port=8085 --kv-cache.data-dir=/tmp/kv --kv-cache.max-entries=2 &
curl "$B/keys/a"   # 200 alpha；curl "$B/keys/b" # 404：淘汰结论跨重启保持
```

## 8. 代码结构

```
core/        CacheEntry / OperationRecord / OperationType / DurableKvCache(状态机)
persistence/ PersistenceLog 接口、FileWalLog(force + CRC + 截断修复)
eviction/    EvictionTracker 接口、LruEvictionTracker(access-order LRU)
cache/       KeyValueCache 接口、CacheExceptions(可区分异常层次)
recovery/    RecoveryService(全序重放)、RecoveryRunner(启动确认)
config/      CacheProperties(配置校验)、CacheBeans(恢复屏障式装配)、CachedBodyFilter
web/         CacheController、GlobalExceptionHandler、CachedBodyFilter
```
