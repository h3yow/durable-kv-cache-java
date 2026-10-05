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
| POST | `/cache/groups/{groupId}/commit` | **整组原子提交**，body 为 JSON（见 §2.1） |
| GET | `/cache/groups/{groupId}` | 查询某组的已确认提交结论；从未确认提交 404 |
| GET | `/cache/admin/metrics` | 容量占用、淘汰数、过期清理数等可观测指标 |

```bash
curl -X PUT "http://localhost:8080/cache/keys/user:1?ttlMillis=5000" --data-binary "alice"
curl "http://localhost:8080/cache/keys/user:1"
curl -X DELETE "http://localhost:8080/cache/keys/user:1"
curl "http://localhost:8080/cache/admin/metrics"
```

### 2.1 整组提交（一组关联键要么全成、要么全不成）

`POST /cache/groups/{groupId}/commit`，请求体：

```json
{
  "members": [
    {"key": "session:1:profile", "action": "INSERT", "value": "alice", "ttlMillis": 600000},
    {"key": "session:1:perms",   "action": "UPDATE", "value": "admin", "ttlMillis": 600000},
    {"key": "session:1:count",   "action": "DELETE"}
  ]
}
```

- `action` 三种，语义显式、判定结论可区分：
  - `INSERT`：要求键当前**不存在**（未过期）。目标已存在 → 整组前置失败；
  - `UPDATE`：要求键当前**存活**。已过期/已删除 → 整组前置失败；
  - `DELETE`：要求键当前**存活**（整组失效场景）。已不存在 → 整组前置失败。
- `groupId` 为调用方提供的非空幂等键（如业务流水号）：
  - 相同 `groupId` + **完全相同**的成员（动作/键/值/TTL）重复提交 → 幂等返回
    `status=OK_DUPLICATE`、`duplicate=true`，结果不叠加、不覆盖，重启后仍然识别；
  - 相同 `groupId` 但成员不同 → `409 GROUP_ID_CONFLICT`，拒绝串组覆盖。
- 成功响应明确列出每一类键与序号区间，便于本地核对：

```json
{
  "status": "OK",
  "groupId": "session-commit-0001",
  "duplicate": false,
  "insertedKeys": ["session:1:profile"],
  "updatedKeys":  ["session:1:perms"],
  "deletedKeys":  ["session:1:count"],
  "evictedKeys":  ["old:key"],
  "liveEntries": 3,
  "seqRange": [12, 17]
}
```

```bash
curl -X POST "http://localhost:8080/cache/groups/session-commit-0001/commit" \
     -H 'Content-Type: application/json' \
     --data '{"members":[{"key":"a","action":"INSERT","value":"1","ttlMillis":600000},
                         {"key":"b","action":"INSERT","value":"2","ttlMillis":600000}]}'
curl "http://localhost:8080/cache/groups/session-commit-0001"
```

### 错误响应（原因可区分）

| HTTP | errorCode | 触发条件 |
|---|---|---|
| 400 | `INVALID_REQUEST` | 空键、null 值、缺 ttl、ttl<=0、值超长、键超长；整组：空 groupId、空成员、组内键重复、非法 action |
| 412 | `GROUP_MEMBER_STALE` | **仅整组**：提交时某个成员键已过期/已删除（UPDATE/DELETE 不命中）或 INSERT 目标已存在；响应带 `staleKeys`，**整组零副作用**，调用方可重读后重试 |
| 409 | `STATE_CONFLICT` | 恢复未完成时访问、对同一实例重复恢复 |
| 409 | `GROUP_ID_CONFLICT` | **仅整组**：相同 groupId 提交了内容不同的成员 |
| 507 | `CAPACITY_EXCEEDED` | 单键容量满且无确定可淘汰键；整组装不下（响应带 `reason=GROUP_TOO_LARGE`） |
| 500 | `PERSISTENCE_FAILED` | WAL 追加/刷盘失败（本次操作整体失败，内存回滚） |
| 503 | `RECOVERY_FAILED` | WAL 校验失败等恢复错误（正常流程中该错误发生在启动期，应用直接中止） |

**调用方处理建议**：
- `400`：请求本身有错，修正参数后重试，不要原样重放；
- `412 GROUP_MEMBER_STALE`：组的前置假设已不成立（并发中被别人改/过期/删除），应**重新读取相关键的当前状态**，用新的 `groupId` 重新组包提交；本次调用没有留下任何副作用，可安全重试；
- `409 GROUP_ID_CONFLICT`：`groupId` 被复用给了不同内容，属于调用方编程序号错误，必须更换 `groupId`；
- `507`：容量问题。整组场景下**淘汰一个都不会发生**（规划阶段即拒绝），可在扩容或清理无关键后，用**同一个** `groupId` 原样重试（该组从未确认提交）；
- `500`：本次写入未落盘成功，状态回滚到提交前；可用同一个 `groupId` 安全重试（服务端没有该组的确认记录）；
- 只有拿到 `200`（含 `OK_DUPLICATE`）才代表这一组已经原子生效。

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
- 整组提交复用**完全相同**的过期规则：提交开始先统一扫描清理已过期键（过期键自然释放
  容量占用），再对成员做前置判定。恢复时组成员 PUT 与单键 PUT 使用同一过期判定：
  确认整组中恢复时刻已过期的键不进入存活集（组的"原子性"针对的是提交那一刻的成败，
  到期后的键按 TTL 失效是既有语义的一部分，不会因为属于某个组而被续命）。

## 4. 并发与一致性模型

- `DurableKvCache` 用**单一监视器锁**把「判定 → 变更内存 → 追加 WAL」串行化，
  HTTP 层不做任何状态判定。因此所有入口对同一键观察到同一全序。
- 写入次序：先淘汰（如有）后写入；淘汰与写入各自获得单调递增序号并先落盘，
  内存变更只在 WAL `force` 成功后提交。
- WAL 失败时本次操作内存整体回滚（含被淘汰键恢复），调用方得到 500，绝不会出现
  「响应失败但数据已变」或「响应成功但未落盘」。

### 4.1 整组提交的原子性与可见性

- 一次整组提交在同一把监视器锁内完成「统一过期清理 → 前置校验 → **只读容量规划** →
  整帧 WAL 落盘 → 内存一次提交」。锁不释放，因此提交期间任何其它入口（单键 GET/PUT/DELETE
  或另一组提交）只能看到**这组操作之前的完整状态或之后的完整状态**，不可能读到改了一半。
- 多组提交并发且键重叠时，它们按抢锁顺序形成**单一确定全序**：后提交的组整体覆盖先提交组
  在重叠键上的值，不会出现互相覆盖到一半。整组的私有键与共享键的归属在任一时刻都自洽。
- 容量规划是**纯只读**的：先确定需要淘汰哪些非成员键（严格按现有 LRU 顺序），
  全部可行性确认后才进入落盘；规划本身不淘汰任何键、不消耗序号。
- WAL 失败回滚：整帧记录被物理截断（`truncate` 回提交前长度），序号回退、内存零改动；
  即使截断本身失败，重启恢复也会按未闭合帧丢弃同一区间，结论收敛一致。

## 5. 持久化与恢复保证

### WAL 格式

文件 `data/cache.wal`，每条一行（UTF-8，`\n` 结尾）。当前为 **8 段**格式：

```
seq|TYPE|key(base64)|value(base64)|ttl|timestamp|crc64|group(base64)
```

- `TYPE` ∈ `PUT / DELETE / EVICT / TXBEG / TXEND`；
- `seq` 从 1 开始、在同一 WAL 上严格单调递增（崩溃截除未闭合整组后，被丢弃帧的序号
  **永久跳过、不复用**，保证重放与新追加之间绝不撞号）；
- 第 8 段 `group` 为整组提交的 `groupId`，单键记录为空；
- **历史 7 段 WAL 行保持兼容**：旧文件按旧 FNV 校验和读取，重放语义不变；
  新写入一律使用 8 段格式与扩展校验和（覆盖 group 段）；
- 行尾附带 FNV-1a 64 位校验和，覆盖除校验字段外的全部内容。

每次追加后执行 `FileChannel.force(true)`：**返回 200 的写入在 kill -9 后也不丢**。

### 整组提交的 WAL 帧

```
TXBEG(groupId, 成员数N, 动作清单)   ← value 字段携带 INSERT/UPDATE/DELETE 动作清单
EVICT ...                          ← 为容纳本组确定淘汰的键（0..M 条，先于成员落盘）
PUT|DELETE ...  (N 条成员，按请求顺序，均带同一 groupId)
TXEND(groupId, 淘汰数M)
```

- 只有读到匹配的 `TXEND` 的帧才是**确认提交**；
- 恢复两遍扫描：第一遍定位并校验全部帧，末尾未闭合帧（`TXEND` 前崩溃，包括只写了半行的
  页撕裂）判定为从未确认，**从 `TXBEG` 起始偏移整段截除并丢弃**——崩溃后第一次重启即
  回到该组之前的完整状态，内存绝不重放半组；
- 已闭合帧内的任何结构问题（groupId 不一致、动作清单/成员数/淘汰数不符、DELETE 成员
  恢复时未命中存活键、CRC 失败）都作为 WAL 损坏抛 `RecoveryException`，**启动中止、拒绝对外
  服务**，不会带着半套数据继续跑；
- `TXBEG` 中持久化动作清单，使恢复即使遇到「提交时 UPDATE、恢复时键刚好过期」也能准确
  重建幂等签名，不依赖提交瞬间的键是否存在来推断动作。

### 恢复流程（启动期，在 Web 服务对外可用之前完成）

1. 顺序读取全部记录；末尾**无换行的半截行**被视为崩溃残留，可安全忽略并截除；
2. 完整行必须通过字段数与 CRC 校验（8 段新格式/7 段旧格式分别校验），否则抛
   `RecoveryException`，Spring Bean 创建失败、**启动中止（退出码非 0），绝不静默继续**；
3. 校验序号单调递增、组帧结构合法（见上）；未闭合整组整段截除；
4. 按全序重放：`PUT` 覆盖（重复提交不叠加计数）、`DELETE`/`EVICT` 移除；
   恢复时刻已过期的 PUT 记为 `EXPIRED_SKIP` 且不进入存活集；整组以闭合帧为单位原子应用；
5. 全部成功后才 `markReady`；此前任何业务请求都得到 `409 STATE_CONFLICT`，
   **不暴露半初始化数据**；
6. 对同一缓存实例重复恢复会被拒绝（`STATE_CONFLICT`），防止内存叠加；
7. 已确认整组的 `groupId → (成员签名, 结论)` 在恢复后重建登记：重复提交相同组仍幂等，
   不同内容仍冲突，结论不因重启丢失或叠加。

`EVICT` 也要持久化：容量淘汰是状态机全序的一部分，否则重启后被淘汰键会因历史 PUT
「复活」。组帧内的 EVICT 与单键 EVICT 共用同一恢复规则（淘汰计数跨重启连续）。

恢复摘要（重放条数、PUT/DELETE/EVICT 计数、过期跳过数、最终 seq、存活键数、
**恢复整组数、丢弃未闭合整组数**）打印在启动日志中。

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

### 6.1 整组提交的容量规则（先规划、后淘汰；不允许先淘汰再失败）

设组内写入成员数（INSERT+UPDATE）为 `puts`，删除成员数为 `deletes`，新增数为
`inserts`（仅 INSERT），容量上限为 `M`：

1. **整组自身即超限**：`puts > M` 且没有任何非成员键可腾时，直接拒绝
   （消息中明确「没有任何其它键可供淘汰」）；
2. **腾出全部位置仍不够**：`puts > M` 且存在非成员键时，消息明确「即使淘汰全部 N 个
   非成员键，仍差 X 个位置」；
3. 否则计算提交后存活数 `liveAfter = 当前存活 - deletes + inserts`：
   - `liveAfter <= M`：不需要淘汰，直接成功；
   - 否则需要 `need = liveAfter - M` 个空位，按当前 LRU 从旧到新选取**非成员存活键**
     （组成员键无论将被写入还是删除，都绝不允许作为腾位对象被淘汰）。
4. 以上任一拒绝结论都发生在落盘之前：**不写任何 WAL、不淘汰任何键、不消耗序号**，
   响应 507 且 `reason=GROUP_TOO_LARGE`，响应消息区分上述两种原因。
5. 成功时响应的 `evictedKeys` 明确给出被淘汰的键（确定顺序）；EVICT 记录先于成员
   操作在同一组帧内落盘，淘汰结论随整组一起跨重启保持。

边界示例（`M=3`，当前已满 `old1,old2,old3`）：
- 组 = `DELETE old3` + `INSERT n1`：删除释放 1 位，刚好放下，无淘汰；
- 组 = `UPDATE old1` + `INSERT n1,n2,n3`：组写入 4 键 > 3，即使淘汰非成员
  `old2,old3` 仍差 1 位 → 整组拒绝，三个旧键一个都不会被先淘汰。

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

49 个测试全部本地运行，不依赖外部服务。**用例之间的数据完全隔离**：

- 纯状态机用例（`DurableKvCacheTest` / `GroupCommitTest` / `GroupRecoveryTest` 等）
  的 WAL 落在 JUnit `@TempDir`，每个用例一个独立目录；
- Spring 上下文用例（`ApplicationTests` / `CacheControllerIntegrationTest` /
  `GroupControllerIntegrationTest`）通过 `IsolatedTestStorage.freshDir(...)`
  在**类加载时**（先于 Spring 上下文创建）分配本轮独有的全新目录，形如
  `target/test-data/<用例名>-<时间戳>-<随机串>`，再由 `@DynamicPropertySource`
  注入 `kv-cache.data-dir`；创建时会把绝对路径打进日志；
- `ProcessRestartSimulationTest` 每个用例独立子目录、每个上下文唯一标记。

因此**同一工作区连续执行任意多次 `mvn test`（不清构建产物）结论都一致**：
每一轮、每个用例都从干净状态开始，上一轮（哪怕被强杀）留在磁盘上的 WAL
不会被本轮复用，本轮的第一次整组提交永远被判定为全新的第一次提交（`OK`），
只有调用方真的用相同 `groupId` + 相同成员重复提交才得到 `OK_DUPLICATE`。
`mvn clean` 可一并清掉所有历史 `target/test-data/*` 目录。

| 测试类 | 覆盖内容 |
|---|---|
| `FileWalLogTest` | WAL 往返（8 段新格式/7 段旧格式兼容）、崩溃半行截断、CRC 损坏可区分失败、序号递增 |
| `DurableKvCacheTest` | 读写删、TTL 过期、确定 LRU 淘汰、非法请求 4 类原因、未就绪访问拒绝、16 线程并发读重写、过期/重写/删除竞争（8 组）、指标 |
| `GroupCommitTest` | 整组原子生效、UPDATE/DELETE 混合、前置失败零副作用（已过期/已删除/INSERT 冲突）、重启前后幂等、groupId 串组冲突、参数错误、容量刚好放下/差一个/整组大于上限/删除释放容量且不误淘汰成员 |
| `GroupConcurrencyTest` | 两组（键在 c 上重叠）12 写线程 + 4 快照读线程高并发：半组可见态为 0，最终为单一确定全序 |
| `GroupRecoveryTest` | 已确认整组完整恢复、TXEND 前崩溃首次重启即整段丢弃（无半组）、半行撕裂同样丢弃、损坏闭合帧 CRC 中止、组淘汰结论跨重启、连续 3 次冷启动不叠加 |
| `GroupControllerIntegrationTest` | HTTP 200/OK_DUPLICATE/412/409/507/400 错误码可区分、组状态 404（每轮独立目录 `target/test-data/group-http-*`） |
| `RecoveryTest` | kill 后重放（重写/删除/过期/删后重写）、淘汰结论跨重启保持、重复恢复拒绝、WAL 损坏中止、崩溃残留容忍 |
| `CacheControllerIntegrationTest` | HTTP 成功/404/400 错误码、metrics 端点（每轮独立目录 `target/test-data/cache-http-*`） |
| `ProcessRestartSimulationTest` | 真实关闭并重建整个 Spring 上下文（每用例独立子目录、每上下文唯一）：单键与整组跨「进程」保持、整组不半组、跨进程重复提交幂等 |
| `RepeatRunStabilityTest` | **反复执行稳定性**：同一套整组用例连续 3 轮（每轮全新目录）结论逐位一致；上一轮被强杀留下的未闭合半组既不泄漏进下一轮，重启后也被整帧丢弃、同内容全新提交仍判 `OK_FIRST` 而非误判重复；同一组操作两种执行顺序结论与恢复后快照一致 |

单测日志中打印了**每个用例操作的存储目录、哪一组（groupId）、涉及哪些键、
判定依据（now/expireAt、LRU 位置、动作清单、序号区间、错误原因）与恢复结果
（整组恢复数/未闭合帧丢弃数）**，可在 `target/surefire-reports/` 或控制台查看。

### 反复执行与顺序变化

```bash
mvn test                                   # 第 1 遍
mvn test                                   # 第 2 遍（不清产物），结论必须与第 1 遍一致
mvn test -Dsurefire.runOrder=random        # 随机顺序执行，结论仍然一致
mvn clean test                             # 清掉全部历史测试数据后执行
```

预期结论：任意重复执行、任意执行顺序下 49 个测试全部通过；整组 HTTP 用例中
`http-grp-1` 的首次提交永远返回 `status=OK`、`duplicate=false`，同请求体重放
才返回 `OK_DUPLICATE`；`RepeatRunStabilityTest` 日志可直接看到三轮独立目录的
判定结论逐位相同。

### 手工复现整组原子性与崩溃恢复

```bash
mvn package -DskipTests
rm -rf /tmp/kvg && java -jar target/durable-kv-cache-java-1.0-SNAPSHOT.jar \
    --server.port=8086 --kv-cache.data-dir=/tmp/kvg --kv-cache.max-entries=3 &
B=http://localhost:8086/cache

# 1) 三个旧键填满容量；GET 刷新 old3，使 old1 成为 LRU 最旧
for k in old1 old2 old3; do
  curl -s -X PUT "$B/keys/$k?ttlMillis=600000" --data-binary "$k" >/dev/null
done
curl -s "$B/keys/old3" >/dev/null

# 2) 整组提交：UPDATE old3 + 两个 INSERT。puts=3 恰好 <= 上限 3，
#    liveAfter=3-0+2=5 -> need=2，LRU 淘汰非成员 old1,old2 后刚好 live=3
curl -s -X POST "$B/groups/demo-session-1/commit" -H 'Content-Type: application/json' --data '{
  "members":[
    {"key":"old3","action":"UPDATE","value":"old3v2","ttlMillis":600000},
    {"key":"new1","action":"INSERT","value":"n1","ttlMillis":600000},
    {"key":"new2","action":"INSERT","value":"n2","ttlMillis":600000}
  ]}'
# -> evictedKeys=["old1","old2"], liveEntries=3, seqRange 可见

# 3) 前置失败演示：UPDATE 一个不存在的键 -> 412，且组内其它键不受影响
curl -i -X POST "$B/groups/demo-stale/commit" -H 'Content-Type: application/json' --data '{
  "members":[{"key":"ghost","action":"UPDATE","value":"x","ttlMillis":600000}]}'

# 4) 幂等：相同 groupId + 相同成员再提一次 -> 200 status=OK_DUPLICATE
curl -s -X POST "$B/groups/demo-session-1/commit" -H 'Content-Type: application/json' --data '{
  "members":[
    {"key":"old3","action":"UPDATE","value":"old3v2","ttlMillis":600000},
    {"key":"new1","action":"INSERT","value":"n1","ttlMillis":600000},
    {"key":"new2","action":"INSERT","value":"n2","ttlMillis":600000}
  ]}' | head -c 300

# 5) kill -9 后重启：整组结论完整恢复（old1/old2 不复活，old3/new1/new2 存活）
kill -9 $(ss -ltnp | grep 8086 | grep -oP 'pid=\K[0-9]+')
java -jar target/durable-kv-cache-java-1.0-SNAPSHOT.jar \
    --server.port=8086 --kv-cache.data-dir=/tmp/kvg --kv-cache.max-entries=3 &
sleep 2
curl -s "$B/groups/demo-session-1"          # 组结论可查
curl -s "$B/keys/old1"                      # 404（组淘汰结论跨重启保持）
curl -s "$B/keys/old3"                      # 200 old3v2
curl -s "$B/admin/metrics"                  # evictionCount=2, liveEntries=3 接得上
```

模拟「提交中被强杀」（无 TXEND 的半组）：停服后手工在 `cache.wal` 末尾追加带某个
groupId 的 `TXBEG`+部分成员行（**不要**加 TXEND，也可最后一行不加换行模拟页撕裂），
再启动；观察启动/恢复日志中「恢复丢弃未闭合整组」——WAL 从该帧 TXBEG 偏移整段截除，
对外服务恢复到该组之前的完整状态，不出现半组。该场景的自动化版本见
`GroupRecoveryTest.crashBeforeTxEndDiscardsWholeGroupOnFirstRestart`。

### 手工复现崩溃恢复（单键）

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
core/        CacheEntry / OperationRecord / OperationType(TXBEG/TXEND) / DurableKvCache(状态机,含整组提交)
cache/       KeyValueCache 接口、GroupAction/GroupMember/GroupResult、CacheExceptions、
             GroupStaleException(412) / GroupIdConflictException(409)
persistence/ PersistenceLog 接口、WalEntry(偏移)、FileWalLog(8段格式+7段兼容+截断)
eviction/    EvictionTracker 接口、LruEvictionTracker(access-order LRU, 只读快照)
recovery/    RecoveryService(两遍帧感知重放/未闭合帧截除)、RecoveryRunner(启动确认)
config/      CacheProperties(配置校验)、CacheBeans(恢复屏障式装配)、CachedBodyFilter
web/         CacheController(单键+整组端点)、GlobalExceptionHandler、dto/GroupCommitRequest、CachedBodyFilter
tests/       IsolatedTestStorage(每轮/每用例独立数据目录分配) + 各测试类
```
