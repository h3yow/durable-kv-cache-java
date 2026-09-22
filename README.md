# Durable KV Cache（本地持久化键值缓存）

一个基于 Spring Boot 3 / Java 21+ 的**本地持久化、可承受并发与进程异常重启**的键值缓存服务。
不依赖任何外部服务（无需数据库/Redis/真实凭据），数据落在本地目录的 WAL + 快照文件中。

---

## 1. 功能概览

| 能力 | 说明 |
|---|---|
| 写入 `PUT` | 支持服务端过期时间（相对 TTL），也可永不过期；支持可选 CAS 版本前置条件 |
| 读取 `GET` | 过期视同不存在，响应明确区分 `found / expired / 不存在` 及判定依据 |
| 删除 `DELETE` | 显式删除写墓碑；同样支持 CAS；过期键删除返回 `deleted=false` 与依据 |
| 持久化 | 每条确认写入顺序追加 WAL，默认逐条 `fsync(force(true))` |
| 崩溃恢复 | 启动时重放「快照 + WAL」，去重折叠、按恢复时点丢弃过期键；恢复期间拒绝服务 |
| 容量与淘汰 | 可配置键数量上限；`EVICT_LRU`（默认，确定顺序淘汰）或 `REJECT`（超限拒绝） |
| 可观测 | `GET /admin/status` 给出生命周期、占用、计数、最近淘汰明细与恢复报告 |
| 快照压缩 | WAL 超阈值自动压缩，或 `POST /admin/checkpoint` 手动触发 |

---

## 2. HTTP 接口

### 2.1 写入 `PUT /cache/{key}`

请求体（JSON）：

```json
{
  "value": "字符串值（必填，UTF-8 字节数受 cache.max-value-bytes 限制）",
  "ttlMillis": 5000,
  "ttlSeconds": null,
  "expectedVersion": null
}
```

- `ttlMillis` 与 `ttlSeconds` 二选一（相对服务端的存活时长）；都为空表示**永不过期**；必须 > 0。
- `expectedVersion` 可选：要求当前存活版本等于该值；传 `-1` 表示要求键当前不存在或已失效（过期）。

成功响应 `200`：

```json
{
  "key": "k", "version": 1, "sequence": 1,
  "expireAtEpochMs": 1790000005000,
  "evicted": false,
  "evictionDetail": "committed under write lock; sequence=1 version=1; server-side expiry"
}
```

`expireAtEpochMs = -1` 表示永不过期。

### 2.2 读取 `GET /cache/{key}`

- 存活：`{"found": true, "expired": false, "value": ..., "version": ..., "expireAtEpochMs": ..., "ttlRemainingMs": ..., "reason": "..."}`
- 已过期（过期点之后、惰性清理之前）：`found=false, expired=true, expireAtEpochMs=<过期点>, reason="not found: expired at ..."`
- 不存在或已被后台清理/删除：`found=false, expired=false, reason="not found: no such key"`

### 2.3 删除 `DELETE /cache/{key}?expectedVersion=`

```json
{ "key": "k", "deleted": true, "version": 2, "reason": "deleted live entry version=1" }
```

### 2.4 运维

| 方法与路径 | 作用 |
|---|---|
| `GET  /admin/status` | 状态快照：state、entries/maxEntries、currentWalBytes、lastCheckpointSequence、totalPuts/Deletes/Evictions/ExpiredPurges、recentEvictions（最近 50 条淘汰明细）、recoveryReport、lastError |
| `POST /admin/checkpoint` | 同步快照压缩 |
| `POST /admin/purge` | 立即清理过期键 |

### 2.5 错误码与 HTTP 状态

所有错误返回 `{"error": <可区分错误码>, "reason": <判定依据>, "key": ..., "at": ...}`：

| HTTP | error | 触发场景 |
|---|---|---|
| 400 | `INVALID_REQUEST` | 空键、null 值、TTL ≤ 0、值字节超限、键过长 |
| 400 | `MALFORMED_BODY` | 请求体缺失/不是合法 JSON |
| 400 | `ILLEGAL_CONFIGURATION` | 启动配置非法（max-entries<1 等），服务不进入恢复 |
| 409 | `VERSION_CONFLICT` | CAS 前置版本与当前存活版本不一致 |
| 429 | `CAPACITY_EXCEEDED` | REJECT 模式下新键超限，或无任何可淘汰条目 |
| 503 | `SERVICE_UNAVAILABLE` | 恢复中（RECOVERING）/恢复失败（FAILED）/重复恢复 |
| 500 | `RECOVERY_FAILURE:<reason>` | 恢复失败，reason 见下 |
| 500 | `PERSISTENCE_FAILURE` | WAL 写入/fsync/快照 IO 失败 |

恢复失败的可区分 `reason`：`CHECKSUM_MISMATCH`、`TRUNCATED_RECORD`、`SNAPSHOT_CORRUPT`、
`UNSUPPORTED_FORMAT`（格式版本/魔数/数据目录出现未识别文件）、`IO_ERROR`。

---

## 3. 过期语义（各入口一致、可解释）

- 过期时间**完全由服务端**在写入时确定：`expireAt = 服务端 now + ttl`，写入后不再受客户端时钟影响。
- 唯一判定规则（对 GET / 重写 / 删除 / 淘汰 / 后台清理 / 重启恢复完全相同）：

  ```
  isExpired(now) := expireAt != NEVER && now >= expireAt
  ```

  即**到期点当刻即过期（含等号）**；永不过期使用哨兵 `Long.MAX_VALUE`。
- 过期结论不依赖后台线程是否及时清理：GET 当场判定；后台清理与写前清扫只负责回收占用。
- 对已过期键的写入：旧存活期结束，按**全新存活期**处理，version 从 1 重新计；
  CAS `-1` 可显式表达「仅当键不存在/已失效时写入」。
- 对已过期键的删除：不产生新的提交序号，返回 `deleted=false` 并移除残条目，reason 注明已过期。
- 恢复时以**恢复时点**统一判定过期：恢复时点已过期的键不装入内存（报告里计 `expiredAtRecovery`），
  因此重启后不会短暂暴露“启动即过期”的数据。

## 4. 并发与单一确定顺序

- 所有变更操作（PUT/DELETE/淘汰/清扫/checkpoint）在同一把写锁下**串行**；读操作无锁，
  读到的 `CacheEntry` 是不可变对象。
- 两个单调序列：
  - `sequence`：全局严格单调，分配顺序 = WAL 物理追加顺序；
  - `version`：每键单调，覆盖 +1；删除墓碑版本 = 被删版本 +1；过期后重写从 1 重新计。
- 因此同一键的「过期 / 重写 / 删除」无论怎样并发，只有一个获胜提交顺序，
  **不可能读到已被覆盖的旧值**；WAL 重放与内存状态给出相同的最终结论。
- GET 的 LRU 访问时间用 `computeIfPresent` + 引用比对做 CAS 刷新，不会把并发重写的新条目改旧。

## 5. 持久化与恢复保证

### 5.1 WAL 格式（可直接用文本工具检查）

每行一条记录：

```
DKVC|1|<crc32>|<seq>|<PUT|DELETE>|<version>|<expireAt>|<base64 key>|<base64 value>\n
```

- CRC32 覆盖除自身字段外的整行；键/值经 Base64 编码，可包含任意 Unicode/换行。
- `fsync-each-write=true`（默认）时，写入在 `FileChannel.force(true)` 成功后才返回 200。

### 5.2 进程异常终止（kill -9 / 断电）的边界

- 已返回成功的写入：在 WAL 中且已 fsync，**重启必然可恢复**。
- 未完成的最后一次写入：可能缺失整行，或产生「没有结尾 `\n` 的残尾行」。
  恢复时该残尾一律视为**未提交**而忽略（恢复报告计 `danglingTailLines`），不会读到半条记录。
- 已提交行的任何字节损坏都会触发 CRC 错误（`CHECKSUM_MISMATCH`），**不会被当作残尾忽略**。

### 5.3 恢复流程（幂等、可重复解释）

1. 读取 `snapshot.db`（不存在视为空）；快照头/体不一致、序号倒退等一律 `SNAPSHOT_CORRUPT`。
2. 按文件名起始 seq 排好序读取全部 `wal-<seq>-<ts>.log` 及 `.log.archived`；
   数据目录出现任何其他常规文件 → `UNSUPPORTED_FORMAT`。
3. 合并后按 sequence 去重：同序号内容完全相同（重复提交/快照与WAL重叠）→ 跳过并计
   `skippedDuplicates`；同序号内容不一致 → `TRUNCATED_RECORD`。序号必须单调。
4. 每键折叠到最大 sequence 的记录；DELETE 生效为“不存在”。
5. 以恢复时点统一判定过期，丢弃已过期键。产物仅为应进入内存的活动记录（纯读，不修改磁盘）。

- 恢复期间状态为 `RECOVERING`，所有 `/cache/**` 返回 503，**不暴露半初始化数据**。
- 恢复是一次性状态迁移 `CREATED -> RECOVERING -> ACTIVE|FAILED`：对同一实例重复 `recover()`
  会得到明确的 503，而不是第二次叠加；重复恢复/重复提交都不会造成数据叠加或丢失。
- 恢复失败 → `FAILED`：保留原因码与消息（可从 `/admin/status` 的 `lastError` 看到），
  所有数据请求继续 503，**绝不静默继续**。

### 5.4 快照压缩（checkpoint）的崩溃安全

协议：持写锁得到一致视图 → 写 `snapshot.tmp` 并 force → `ATOMIC_MOVE` 为 `snapshot.db`
并 force 目录 → 关闭旧 WAL、打开新 WAL（从下一 seq 起步）→ 旧 WAL 原子改名为
`*.log.archived` 并 force 目录。

- rename 前崩溃：仍是旧快照 + 完整旧 WAL；rename 后崩溃：新快照 +（可能已归档的）旧 WAL，
  归档 WAL 仍参与重放，与快照按 sequence 去重。任何崩溃点都不会丢已确认数据、不会重复叠加。
- 归档失败（如文件系统不支持原子改名）不影响正确性：旧 WAL 原样保留，恢复照常去重；
  该错误记录在 `/admin/status` 的 `lastError` 中可观测。
- 活动 WAL 字节数超过 `cache.wal-compaction-bytes` 后在写入成功时自动触发（0 关闭）；
  自动压缩失败不影响已确认写入（WAL 完好）。

## 6. 容量与淘汰规则

- 容量口径为**当前未过期的键数量**。写新键前先回收过期残条目，再判定容量，
  因此“过期但还没被后台清理”的键不会虚占容量。
- 覆盖已有键（含 CAS 覆盖）永远不触发容量错误/淘汰。
- `EVICT_LRU`（默认）需要腾位时，淘汰目标按以下**全序**选取，唯一确定：
  1. 已过期键优先（`expireAt` 升序；相同按键名字典序）；
  2. 其余按“最近访问时间”升序（LRU；写入与命中 GET 都会刷新访问时间）；
  3. 访问时间完全相同再按键名字典序升序。
  淘汰同样向 WAL 追加一条 DELETE，重启后被淘汰键不会复活。
- `REJECT`：容量满时新键写入直接 `429 CAPACITY_EXCEEDED`，已有键覆盖仍允许。
- 观测：`/admin/status` 的 `entries / maxEntries / totalEvictions / totalExpiredPurges /
  recentEvictions`（含被淘汰键、版本、访问时间、过期点、原因、当时容量）。

## 7. 配置项（`src/main/resources/application.properties` 或 -D 参数）

| 键 | 默认 | 约束/含义 |
|---|---|---|
| `cache.data-dir` | `/tmp/durable-kv-cache-demo` | WAL 与 snapshot 所在目录，启动自动创建 |
| `cache.max-entries` | `10000` | 容量上限（键数），必须 ≥ 1 |
| `cache.max-value-bytes` | `1048576` | 单值 UTF-8 字节上限，必须 ≥ 1 |
| `cache.eviction` | `EVICT_LRU` | `EVICT_LRU` / `REJECT` |
| `cache.fsync-each-write` | `true` | 每条确认写入是否 fsync |
| `cache.wal-compaction-bytes` | `16777216` | 自动压缩阈值（字节），0 关闭 |
| `cache.cleanup-interval-ms` | `1000` | 后台过期清理周期，必须 > 0 |

非法配置在恢复开始**之前**校验，直接失败且不触碰数据（`ILLEGAL_CONFIGURATION`）。

## 8. 本地验证方法（无需外部服务/凭据）

前置：JDK 21+、Maven 3.9+（离线即可，依赖已在本地仓库）。

```bash
# 1) 全量测试（29 个用例，含并发、过期竞争、重启恢复、损坏失败、淘汰、快照、全栈 HTTP）
mvn test

# 仅跑某一组
mvn test -Dtest=CacheStoreConcurrencyTest     # 并发读写 / CAS / 过期边界 / REJECT
mvn test -Dtest=RecoveryPersistenceTest       # 重启恢复 / 删除与覆盖顺序 / 重复恢复
mvn test -Dtest=RecoveryFailureTest           # CRC/截断/快照损坏/陌生文件/非法配置
mvn test -Dtest=EvictionTest                  # 确定淘汰顺序、重启不复活、REJECT
mvn test -Dtest=CheckpointCompactionTest      # 快照压缩、归档去重、多次压缩稳定
mvn test -Dtest=WalRecordCodecTest            # 编解码与各可区分损坏原因
mvn test -Dtest=WebApiIntegrationTest         # 全栈 HTTP（200/400/409/429/503 与可观测）

# 2) 启动服务手工验证
mvn spring-boot:run
curl -s -XPUT  localhost:8080/cache/hello -H 'Content-Type: application/json' \
  -d '{"value":"world","ttlSeconds":10}'
curl -s localhost:8080/cache/hello
curl -s -XDELETE localhost:8080/cache/hello
curl -s localhost:8080/admin/status | head -c 1200; echo
```

测试日志中每个关键判定都会打印**键、判定依据与恢复结果**，例如：
`[get-expired] key=... expireAt=... now=... basis=server-side expiry`、
`[eviction] evict key=... reason=lru-then-key-asc ... liveSize=2/3`、
`[recovery] recover@... snapshotRecords=... duplicates=... expiredAtRecovery=... live=...`。
数据文件可直接用文本工具打开检查（`data/cache/wal-*.log`、`snapshot.db`）。
