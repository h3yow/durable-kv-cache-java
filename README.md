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
| POST | `/cache/groups?groupId=<可选>` | **整组原子提交**，body 为成员操作 JSON（见 §2.1） |
| GET | `/cache/admin/metrics` | 容量占用、淘汰数、过期清理数、整组成败等可观测指标 |

```bash
curl -X PUT "http://localhost:8080/cache/keys/user:1?ttlMillis=5000" --data-binary "alice"
curl "http://localhost:8080/cache/keys/user:1"
curl -X DELETE "http://localhost:8080/cache/keys/user:1"
curl "http://localhost:8080/cache/admin/metrics"
```

### 2.1 整组提交（POST /cache/groups）

一次提交里的多个 PUT/DELETE **要么全部生效，要么全部不生效**，用于"会话资料/权限/计数
一起更新""一批键一起失效"等需要原子性的关联操作。

请求体（`groupId` 通过查询参数传入，可省略；省略时服务端生成 UUID）：

```json
{
  "members": [
    {"op": "PUT",    "key": "sess:profile", "value": "alice", "ttlMillis": 600000},
    {"op": "PUT",    "key": "sess:perm",    "value": "admin", "ttlMillis": 600000},
    {"op": "DELETE", "key": "sess:oldCount"}
  ]
}
```

成功响应（200）：

```json
{
  "status": "OK",
  "groupId": "sess-42",
  "outcome": "COMMITTED",
  "duplicate": false,
  "memberCount": 3,
  "evictedKeys": ["some:lru:oldest"],
  "beginSeq": 12,
  "commitSeq": 17,
  "liveEntries": 3
}
```

```bash
curl -X POST "http://localhost:8080/cache/groups?groupId=sess-42" \
     -H 'Content-Type: application/json' \
     --data '{"members":[{"op":"PUT","key":"a","value":"1","ttlMillis":600000},
                          {"op":"DELETE","key":"b"}]}'
```

#### 语义与边界（务必先读）

- **原子可见性**：整组与单键入口共用同一把监视器锁，且整组在 GCOMMIT 落盘前**完全不改内存**。
  因此锁内任何一个瞬间，所有键都处于"某个完整提交之前/之后"的一致状态，绝不存在只改了一部分
  的内存中间态——从任何入口都读不到半新半旧。多组并发且键重叠时，整组按获取锁的顺序形成单一
  确定全序，最终状态等价于某一种串行执行，不会互相覆盖到一半。注意：单个 GET 是单键读，
  连续多个 GET 分别属于不同的锁内瞬间；需要"多键同一视图"时，应在同一个整组提交里更新它们、
  并以版本号等方式在业务侧关联读取。
- **幂等去重（强烈建议显式传 groupId）**：相同 `groupId` + 完全相同的成员重复提交，返回
  `outcome=DUPLICATE` 与首次提交的结论，**不重复执行、不叠加计数**；相同 `groupId` 但成员
  内容不同，返回 400 `INVALID_REQUEST`（拒绝幂等键复用）。不传 groupId 时每次都是新组。
  groupId 的去重表随 WAL 一起恢复，重启后重试同样安全。
- **组内 DELETE 的前置条件**：组内 DELETE 的键在**提交时刻必须仍然存活**。已过期或已删除
  都会使**整组**被拒绝（412 `GROUP_STALE`），响应逐个键给出可区分原因
  （`KEY_EXPIRED` / `KEY_NOT_FOUND`），且不产生任何副作用。组内 PUT 沿用单键 PUT 语义：
  可以新建键，也可以重写（覆盖）一个已过期键——过期键在提交时按"不存在"处理。
- **同一组内键不得重复**（无论 PUT/DELETE），否则 400。
- **容量规则（与单键完全一致，先预演后执行）**：容量按存活键数计，淘汰顺序仍为确定 LRU。
  - 整组自身最终占用的键数就大于 `max-entries`：直接拒绝，错误码 `GROUP_TOO_LARGE`；
  - 否则按现有 LRU 顺序预演淘汰，只要能在容量上限内为整组腾出位置就**整体成功**，
    响应的 `evictedKeys` 明确列出淘汰了哪些键（淘汰记录也在同一原子组内落盘）；
  - 预演腾不出足够空间：整组拒绝，错误码 `GROUP_INSUFFICIENT_SPACE`。
  - **拒绝时不允许先淘汰一部分再失败**：容量预演全程只读，不淘汰、不移动 LRU、不占序号。
- **TTL**：组内 PUT 仍必须带正整数 `ttlMillis`，过期判定规则与单键完全相同；提交开始时会
  顺手清理与本组无关的已过期键。
- **持久化失败**：GBEGIN/成员/EVICT/GCOMMIT 写入途中若 WAL 失败，内存尚未改动，文件立即
  截断回本组开始前的提交点并归还序号，调用方得到 500；即使截断本身失败，重启恢复也会把
  未闭合的整组整体丢弃。
- **重启恢复**：只有读到与 GBEGIN 匹配的 GCOMMIT，整组才整体重放；末尾的半行、或只有
  GBEGIN 没有 GCOMMIT 的整段内容一律视为强杀残留并物理截除——**不会恢复出半组**。
  WAL 中出现两个相同 groupId 的已闭合整组、孤儿 GCOMMIT、帧字段/CRC 错误时，恢复失败、
  应用拒绝对外服务（退出码非 0），绝不带半套数据继续跑。
- **失败时调用方怎么处理**：
  - 400 `INVALID_REQUEST`：请求本身有问题，修正参数后用**新的 groupId**重试；
  - 412 `GROUP_STALE`：组成员的前置状态已变化（被别的请求改/删/过期），调用方应重新读取
    关联键、按最新状态重组操作后用**新的 groupId**重试；不要原样重试；
  - 507 `GROUP_TOO_LARGE`/`GROUP_INSUFFICIENT_SPACE`：拆小整组或调整容量后重试；
  - 500 `PERSISTENCE_FAILED`：结果未知，**用同一个 groupId 重试**即可——若此前已落盘会得到
    `DUPLICATE`，未落盘则正常 `COMMITTED`，两种情况都安全。

### 错误响应（原因可区分）

| HTTP | errorCode | 触发条件 |
|---|---|---|
| 400 | `INVALID_REQUEST` | 空键、null 值、缺 ttl、ttl<=0、值超长、键超长、组为空、组内键重复、未知 op、groupId 复用但内容不同 |
| 409 | `STATE_CONFLICT` | 恢复未完成时访问、对同一实例重复恢复 |
| 412 | `GROUP_STALE` | 整组内 DELETE 的键在提交时已过期（`KEY_EXPIRED`）或已删除（`KEY_NOT_FOUND`），整组拒绝 |
| 507 | `CAPACITY_EXCEEDED` | 单键写入时容量满且无确定可淘汰键 |
| 507 | `GROUP_TOO_LARGE` / `GROUP_INSUFFICIENT_SPACE` | 整组自身超过上限 / 按 LRU 也腾不出足够空间；整组拒绝且无任何淘汰副作用 |
| 500 | `PERSISTENCE_FAILED` | WAL 追加/刷盘失败（本次操作整体失败，内存回滚；整组会把文件截回提交点） |
| 503 | `RECOVERY_FAILED` | WAL 校验/帧结构失败等恢复错误（正常流程中该错误发生在启动期，应用直接中止） |

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
  HTTP 层不做任何状态判定。因此所有入口（单键与整组）对任意一组键都观察到同一全序。
- 单键写入次序：先淘汰（如有）后写入；淘汰与写入各自获得单调递增序号并先落盘，
  内存变更只在 WAL `force` 成功后提交。
- 整组写入次序：容量预演（只读）→ 连续追加 `GBEGIN, EVICT*, (PUT|DELETE)*, GCOMMIT`
  （每帧/每条都占连续序号并逐条 force）→ **GCOMMIT 成功后**才一次性变更内存。
  因此任何读都不可能碰到"组内一部分键已改、其余没改"的时刻；键有重叠的并发整组，
  最终状态必为某一种确定的串行顺序，不会互相覆盖到一半。
- WAL 失败时本次操作内存整体回滚（单键含被淘汰键恢复；整组则把文件截回本组开始前的
  提交点并归还序号），调用方得到 500，绝不会出现「响应失败但数据已变」或「响应成功但未落盘」。

## 5. 持久化与恢复保证

### WAL 格式

文件 `data/cache.wal`，每条一行（UTF-8，`\n` 结尾）。历史单键记录为 7 段，
整组帧/成员为 8 段（末段 CRC 之前多一个 groupId）：

```
seq|TYPE|key(base64)|value(base64)|ttl|timestamp[|groupId(base64)]|crc64
```

- `TYPE` ∈ `PUT / DELETE / EVICT / GBEGIN / GCOMMIT`；
- 一个确认的整组在 WAL 中形如
  `GBEGIN, EVICT*(为该组淘汰的键), PUT/DELETE*(组成员), GCOMMIT`，
  帧与成员都带相同 groupId、各自占用严格连续的 seq；
- `GBEGIN` 的 ttl 字段为"GBEGIN 与 GCOMMIT 之间的记录总数"（含 EVICT），
  `GCOMMIT` 的 ttl 字段为 0、key 字段承载 groupId；
- `seq` 从 1 开始、严格连续递增；
- 行尾附带 FNV-1a 64 位校验和，覆盖除校验字段外的全部内容（含 groupId）；
- 无 groupId 的 7 段旧格式仍可正常读取（向前兼容）。

每次追加后执行 `FileChannel.force(true)`：**返回成功的写入/整组在 kill -9 后也不丢**。

### 恢复流程（启动期，在 Web 服务对外可用之前完成）

1. 顺序读取全部记录，只保留到**最后一个完整提交点**为止：完整换行结尾的普通记录，或
   以 GCOMMIT 闭合的整组；
2. 末尾**无换行的半截行**、或**只有 GBEGIN 没有匹配 GCOMMIT 的整段内容**，一律视为
   强杀残留，整体忽略并物理截除——因此"整组写了一半被杀"绝不会恢复出半组；
3. 完整行必须通过字段数与 CRC 校验；整组必须满足：GBEGIN 与 GCOMMIT 的 groupId 一致、
   帧内记录数与 GBEGIN 声明一致、不允许嵌套 GBEGIN、不允许孤儿 GCOMMIT。任一不满足都抛
   `RecoveryException`，Spring Bean 创建失败、**启动中止（退出码非 0），绝不静默继续**；
4. 校验序号严格连续（缺号/重复同样中止，错误信息可区分）；
5. 按全序重放：`PUT` 覆盖（重复提交不叠加计数）、`DELETE`/`EVICT` 移除；整组在遇到
   GCOMMIT 时一次性应用所有成员；恢复时刻已过期的 PUT 记为 `EXPIRED_SKIP` 且不进入存活集；
6. 同一个 groupId 的两个**已闭合**整组属于幂等键冲突，恢复中止，防止结果叠加或丢失；
7. 全部成功后才 `markReady`；此前任何业务请求都得到 `409 STATE_CONFLICT`，
   **不暴露半初始化数据**；
8. 对同一缓存实例重复恢复会被拒绝（`STATE_CONFLICT`），防止内存叠加。

单键路径的 `EVICT` 也要持久化：容量淘汰是状态机全序的一部分，否则重启后被淘汰键会因历史
PUT「复活」。单键 WAL 追加失败导致淘汰已落盘而新 PUT 回滚时，会追加一条幂等 EVICT 补偿记录，
使 WAL 与内存重新一致。整组路径的 EVICT 天然属于整组帧，随整组一起成功或一起被截除。

恢复摘要（重放条数、PUT/DELETE/EVICT 计数、**整组数 groupsApplied**、过期跳过数、最终 seq、
存活键数）打印在启动日志中。

## 6. 容量与淘汰规则

- 配置 `kv-cache.max-entries`（>0 生效，<=0 不限）。容量按**存活键数**计。
- 淘汰算法为**确定的 LRU**（access-order `LinkedHashMap`）：
  - PUT（新增键）与命中的 GET 都会把该键标记为最近使用；
  - 容量满时，链表头（最久未被触碰的键）被淘汰；
  - PUT 到已存在键（重写）不触发淘汰。
- **整组的容量判定复用同一套规则但先预演后执行**：按 LRU 从旧到新挑选"不会被本组删除或
  重写"的存活键作为淘汰候选，只做只读预演；整组成功时淘汰才真正发生（EVICT 记录并入同一
  原子组），整组失败时一个键都不会被淘汰，LRU 顺序也保持原样（详见 §2.1）。
- 淘汰键、判定依据、淘汰前后容量均写日志；淘汰计入 `/cache/admin/metrics` 的
  `evictionCount`，重启恢复后该计数保持连续（整组内淘汰同样计入）。
- 指标字段：`maxEntries, liveEntries, totalPutCount, totalDeleteCount, evictionCount,
  expiredScanCount, lastExpireAtMs, state(READY/RECOVERING), groupCommitCount,
  groupSuccessCount(含COMMITTED，不含DUPLICATE), groupDuplicateCount,
  groupRejectedStaleCount, groupRejectedCapacityCount`。
  容量占用（`liveEntries/maxEntries`）与整组操作成败结论都可在该端点直接看到。

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

42 个测试全部本地运行（WAL 均落在 JUnit `@TempDir` 或每次运行唯一的目录/键前缀下，
用例之间互不污染），不依赖外部服务：

| 测试类 | 覆盖内容 |
|---|---|
| `FileWalLogTest` | WAL 往返、崩溃半行截断、CRC 损坏可区分失败、序号递增 |
| `DurableKvCacheTest` | 读写删、TTL 过期、确定 LRU 淘汰、非法请求 4 类原因、未就绪访问拒绝、16 线程并发读重写、过期/重写/删除竞争（8 组）、指标 |
| `GroupCommitTest` | **整组**：整组原子生效、PUT+DELETE 混合、同 ID 重试 DUPLICATE/异内容拒绝、组内含已过期/已删除键（KEY_EXPIRED 与 KEY_NOT_FOUND 可区分）、容量边界（刚好放下 / 差一个按 LRU 淘汰 / 整组大于上限 / 被拒后 LRU 不受影响）、8 线程×100 轮重叠键并发整组的全序一致性、失败不占序号 |
| `GroupRecoveryTest` | **整组恢复**：整组（含淘汰）跨重启完整恢复且多次重启幂等、强杀半组（GBEGIN+成员无 GCOMMIT，含半行）被整体截除不恢复半组、孤儿 GCOMMIT 中止、WAL 中重复 groupId 中止、对同一实例重复恢复拒绝 |
| `RecoveryTest` | kill 后重放（重写/删除/过期/删后重写）、淘汰结论跨重启保持、重复恢复拒绝、WAL 损坏中止、崩溃残留容忍 |
| `CacheControllerIntegrationTest` | HTTP 单键成功/404/400 错误码、metrics 端点 |
| `GroupControllerIntegrationTest` | HTTP 整组 COMMITTED/DUPLICATE、412 GROUP_STALE（含逐键原因）、507 GROUP_TOO_LARGE、400 非法 op、metrics 整组字段 |
| `ProcessRestartSimulationTest` | 真实关闭并重建整个 Spring 上下文，验证已确认单键写入/删除与**已确认整组**跨「进程」完整保持、重启后同 ID 重试 DUPLICATE |

单测日志中打印了**组 ID、涉及的键、判定依据（now/expireAt、LRU 位置、序号、错误原因码）
与恢复结果（groupsApplied 等）**，可在 `target/surefire-reports/` 或控制台查看。

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

### 手工复现整组提交与强杀恢复

```bash
mvn package -DskipTests
rm -rf /tmp/kvg && java -jar target/durable-kv-cache-java-1.0-SNAPSHOT.jar \
    --server.port=8086 --kv-cache.data-dir=/tmp/kvg --kv-cache.max-entries=3 &
G=http://localhost:8086/cache

# 1) 两个单键占满容量
curl -s -X PUT "$G/keys/a?ttlMillis=600000" --data-binary alpha >/dev/null
curl -s -X PUT "$G/keys/b?ttlMillis=600000" --data-binary bravo >/dev/null
curl -s "$G/keys/a" >/dev/null   # 刷新 a，使 b 成为 LRU 最旧

# 2) 整组新增 p、q：容量 3 放不下 4 个键，按 LRU 整组淘汰 b（响应 evictedKeys=["b"]）
curl -s -X POST "$G/groups?groupId=sess-42" -H 'Content-Type: application/json' --data '{
  "members":[
    {"op":"PUT","key":"p","value":"alice","ttlMillis":600000},
    {"op":"PUT","key":"q","value":"admin","ttlMillis":600000}
  ]}'

# 3) 相同 groupId 重试 -> outcome=DUPLICATE，不叠加
curl -s -X POST "$G/groups?groupId=sess-42" -H 'Content-Type: application/json' --data '{
  "members":[
    {"op":"PUT","key":"p","value":"alice","ttlMillis":600000},
    {"op":"PUT","key":"q","value":"admin","ttlMillis":600000}
  ]}'

# 4) 组内删除已不存在的 b -> 412 GROUP_STALE，逐键原因 KEY_NOT_FOUND，且无副作用
curl -s -X POST "$G/groups?groupId=bad-1" -H 'Content-Type: application/json' \
     --data '{"members":[{"op":"DELETE","key":"b"}]}'

# 5) 整组大于上限 -> 507 GROUP_TOO_LARGE，不淘汰任何键
curl -s -X POST "$G/groups?groupId=big-1" -H 'Content-Type: application/json' --data '{
  "members":[
    {"op":"PUT","key":"x","value":"1","ttlMillis":600000},
    {"op":"PUT","key":"y","value":"2","ttlMillis":600000},
    {"op":"PUT","key":"z","value":"3","ttlMillis":600000},
    {"op":"PUT","key":"w","value":"4","ttlMillis":600000}
  ]}'

# 6) 容量占用与整组成败结论本地可见
curl -s "$G/admin/metrics"

# 7) kill -9 后重启：p、q 整组完整恢复，b 仍 404；同 ID 重试仍 DUPLICATE
kill -9 $(ss -ltnp | grep 8086 | grep -oP 'pid=\K[0-9]+')
java -jar target/durable-kv-cache-java-1.0-SNAPSHOT.jar \
    --server.port=8086 --kv-cache.data-dir=/tmp/kvg --kv-cache.max-entries=3 &
curl "$G/keys/p"   # 200 alice（整组完整恢复，不是半组）
curl -o /dev/null -w '%{http_code}\n' "$G/keys/b"   # 404
```

## 8. 代码结构

```
core/        CacheEntry / OperationRecord / OperationType(含GBEGIN/GCOMMIT)
             GroupOperation / GroupCommitResult / DurableKvCache(状态机: 单键+整组)
persistence/ PersistenceLog 接口(append/readAll/truncateTo/sizeBytes)、
             FileWalLog(force + CRC + 组帧校验 + 截到最后提交点)
eviction/    EvictionTracker 接口(含 LRU 只读快照)、LruEvictionTracker(access-order LRU)
cache/       KeyValueCache 接口(commitGroup + 扩展指标)、CacheExceptions(可区分异常层次,
             含 GroupStaleException / GroupCapacityException)
recovery/    RecoveryService(单键全序重放 + 整组缓冲到GCOMMIT一次性应用)、RecoveryRunner
config/      CacheProperties(配置校验)、CacheBeans(单一恢复实例屏障式装配)
web/         CacheController(单键+POST /groups)、GlobalExceptionHandler、CachedBodyFilter、
             GroupCommitRequest(JSON 请求体)
```
