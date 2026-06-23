# tidb2snowflake 优化空间梳理（Snowflake 链路）

> 本文汇总 tidb2snowflake（TiDB → Snowflake 链路）当前实现中**有优化空间的点**，分两大块：**增量扫描** 与 **快照装载**，并附优先级与落地建议。所有结论以代码为准，关键处标注了源码位置。

## 背景：两个值得优化的环节

tidb2snowflake 的核心是「TSO 对齐的快照（Dumpling → `COPY INTO`）+ TiCDC Changefeed 增量（CSV → `MERGE INTO`）」，以 S3 为中转。数据搬运由 Snowflake 直读 S3 完成，tidb2snowflake 只做编排。两条链路里各有一处明显的成本/性能瓶颈：

- **增量端**：每轮轮询都对整张表子树做全量 `WalkDir` + 逐文件 `HEAD`，随增量文件数线性增长。
- **快照端**：整表 all-or-nothing 装载，无文件级断点续传；逐文件 `COPY`、5GiB 大文件、不压缩，大表时墙钟时间长、失败重灌代价大。

---

## 一、增量扫描优化

### 现状与问题

增量循环按 `--cdc.flush-interval / 5`（默认 60s/5 = **12 秒**）一轮，每轮 `getNewFiles()` 对表子树 `increment/<db>/<table>/` 做：

- **LIST**：`WalkDir(SubDir=<db>/<table>)` 列出整棵子树**所有** key（含所有日期、所有老文件）—— `replicate/increment.go:200-201`
- **HEAD**：对扫到的**每个** `.csv` 调一次 `CheckpointExists`（= `FileExists`，一次 HEAD）—— `replicate/increment.go:214`、`:240`

两者都随文件数 **O(N) 线性增长**。同时 tidb2snowflake **不删处理过的 CSV**（只删过期旧版本 `schema.json`，`replicate/increment.go:311`），文件无限堆积，导致：

- **S3 请求费（LIST/HEAD）随文件数线性上涨**，长跑后往往超过存储费；
- **扫描延迟持续上升**。

注意：「老文件不会被重复 `MERGE`」是真的（靠内存 `tableDMLIdxMap` 算增量区间，`replicate/increment.go:43/227`），但「老文件不被扫描」是假的——发现新文件本身就要把子树全 LIST 一遍。

### 现成抓手：TiCDC 已写了索引文件

TiCDC 在每个日期目录写了 `meta/CDC.index`，内容是该目录**最新数据文件名**。TiCDC 自己重启也靠读它恢复（`getNextFileIdxFromIndexFile`）。消费端完全可以用它，避免逐文件枚举。

### 三档优化方案（从零风险到完整）

#### 档位 0 — 干掉 HEAD 风暴（零风险，强烈推荐先做）

`WalkDir` 本来就已经把 `.checkpoint` 文件名一起列出来了（落进忽略分支）。把这一轮 walk 列到的 `.checkpoint` 路径收进一个**内存 set**，`CheckpointExists` 改成查 set，而不是发网络 HEAD。

- **收益**：每轮省掉 **O(csv 数量) 个 HEAD 请求**，LIST 不变。
- **风险**：几乎为零，语义不变，不引入新文件。
- **改法**：`getNewFiles` 里先把 walk 结果缓存成 `[]path`，walk 结束后再分类处理（checkpoint set + csv 处理）。

#### 档位 1 — 只看「活跃日期」（中等）

`date-separator=day` 已按天分目录，但现在递归列所有日期。优化：

1. 用 **delimiter 列举**（S3 `Delimiter=/`）只拿 `<date>/` 这一层前缀，不展开目录内的文件 —— LIST 量从 O(总文件) 降到 O(日期数)。
2. 维护「最早未完成日期」水位：某 `<date>` 一旦 ①游标到达该目录 max ②changefeed `checkpoint-ts`（读 `increment/metadata`）越过当天 → **永久关闭，不再扫描**。

稳态下「活跃日期」基本就是今天 1 个目录，扫描量压到接近常数。

#### 档位 2 — 持久化游标 + 读 index（完整，稳态 O(1)）

内存里的 `tableDMLIdxMap` 本身就是「(version/partition/date) → 已处理最大序号」的游标，只是没落盘、重启即丢。把它持久化即可：

- **记在哪 / 记什么**：表自己的命名空间下放一个小 JSON，例如 `increment/<db>/<table>/_consumer/progress.json`：
  ```json
  {
    "dml_idx": { "<version>/<partition>/<date>": 42 },   // = tableDMLIdxMap
    "known_versions": [V0, V1],                           // = tableDefMap 版本，省得重读 schema
    "closed_dates": ["2026-06-13", "2026-06-14"]          // 已永久关闭日期
  }
  ```
- **什么时候读**：`NewIncrementReplicateSession`（`replicate/increment.go:55`）启动时先读它，恢复游标，**重启不再全量扫描**。
- **每轮怎么扫**：delimiter 列出 `> 最大已关闭日期` 的目录（通常 1~2 个）→ 每个活跃日期 GET 一次 `meta/CDC.index` 拿 maxIndex → 处理 `游标+1 .. maxIndex`，**不 LIST、不 HEAD 单个 csv**。
- **什么时候写**：每批 `MERGE` 成功后覆盖写一次 `progress.json`（1 次 PUT）。

稳态每轮成本 ≈ `1 次 delimiter LIST + 每活跃日期 1 个 GET + 1 个 PUT` —— 从 O(总文件) 降到 **O(活跃日期) ≈ O(1)**，`.checkpoint` 可彻底不写。这正是 TiCDC 自带 storage consumer 的消费模型。

##### 必须处理的 3 个正确性坑

1. **游标只能在 `MERGE` 持久化成功之后再前移**（与现在写 `.checkpoint` 的时序一致）。崩在中间 → 重启从游标重放最后一批，因 `MERGE` 按主键幂等，**at-least-once 安全**。顺序绝不能反。
2. **`CDC.index` 是「先写 index 再写 data」**，index 指到的顶部 csv 可能还没落完。消费端要容忍 GET 顶部文件 `NoSuchKey`/不完整 → 跳过、下一轮再处理（或保守只处理到 `maxIndex-1`）。
3. **跨天 index 归 0**（每个新日期序号从 1 开始），游标必须按 `(version, partition, date)` 三元组记 —— `tableDMLIdxMap` 的 key（`DmlPathKey`）已满足，直接序列化即可。

### 不改代码也能缓解（运维侧）

| 手段 | 说明 |
|---|---|
| **S3 Lifecycle 清理老分区** | 对 `increment/` 下老日期目录设过期规则。安全前提：只删「复制延迟天数」之前的对象（已消费文件都有 `.checkpoint`，且 `checkpoint-ts` 已越过） |
| **调大 `--cdc.flush-interval`** | 轮询频率 = flush-interval/5，从 60s 调到 300s，直接把扫描/请求费砍到 1/5（代价是延迟变大） |
| **调大 `--cdc.file-size`** | 单文件更大 → 文件总数更少 → LIST/HEAD 更少 |
| **TiCDC 端文件回收** | cloud storage sink 支持 `file-expiration-days`，但 tidb2snowflake 自动建 changefeed 时未设置（`pkg/cdc/connector.go` 只设了 flush-interval/file-size/output-column-id），需改源码或事后 `cdc cli changefeed update` 补 |

---

## 二、快照装载优化（尤其大表）

### 前提：同步阻塞不是问题，不要动

`COPY INTO` 和 `MERGE INTO` 都走 Go `database/sql` 的 `db.Exec`，**同步阻塞到 Snowflake 真正落表并提交才返回**（`pkg/snowflake/sql.go:64`、`pkg/snowflake/connector.go:106`；全仓库无 async 模式）。tidb2snowflake **只在 `Exec` 成功返回后才写 `loadinfo`/`.checkpoint` 标记**，正是这个同步语义让「标记 = 已落表」成立，从而支撑幂等与断点续传。**这是正确性地基，不能改成异步。** 优化空间在装载**策略**，不在同步性。

### 现状与问题（大表时尤其痛）

| 问题 | 说明 | 大表影响 |
|---|---|---|
| **整表 all-or-nothing，无文件级续传** | 装到一半崩溃 → 没写 loadinfo → 重启 `CREATE OR REPLACE TABLE`（`pkg/snowflake/sql.go:108`，经 `CopyTableSchema` 触发，`replicate/snapshot.go:91`）清空重建 → **全部重灌** | 几 TB 表装到 99% 崩了也从零再来，致命 |
| **逐文件一条 COPY（反模式）** | 默认 `parrallelLoad=true`（`cmd/snowflake.go:112`），对每个 CSV 发一条 `COPY INTO @stage/<单文件>`，Go 侧 16 并发（`DataWarehouseLoadConcurrency=16`，`replicate/snapshot.go:24/116`） | 没用上 Snowflake 单条 COPY 内部跨 warehouse 线程并行的能力，被 16 卡住，statement 开销 ×N |
| **dump 与 load 串行，不流水线** | `Export()`（Dumpling 全量导出）整表导完才进 load（`cmd/core.go`） | 大表先等 dump 全写完 load 才开始，墙钟 ≈ dump + load |
| **5GiB 大文件，COPY 并行度差** | Dumpling `FileSize=5GiB`（`pkg/dumpling/dump.go:47`） | Snowflake 按文件分配线程，单个超大文件只能少数线程处理，远超官方推荐的 100–250MB |
| **CSV 不压缩** | `FileType=csv`、无 gzip（`pkg/dumpling/dump.go:34`） | S3 存储/传输字节翻几倍，COPY 读 I/O 更多 |
| **空表可见窗口** | load 前先 `CREATE OR REPLACE` 清空，整个装载期表是空的；重启又清一次 | 大表装载几十分钟内，下游查到空表/半成品 |
| **warehouse 不自适应** | COPY 吞吐 ∝ warehouse 大小，但 tidb2snowflake 不调 | 大初始装载时小 warehouse 是瓶颈 |

### 优化方案（按性价比排序）

#### ① 让快照可断点续传（最该做）

Snowflake `COPY INTO` **自带 load history**：同一张表 64 天内不会重复加载已装过的文件（除非 `FORCE=TRUE`）。现在是 `CREATE OR REPLACE TABLE` 把这个历史一起重置了，才导致全量重灌。

- **改法**：重试时不要 REPLACE（改成 `CREATE TABLE IF NOT EXISTS`），重跑同一条 COPY 时 Snowflake 自动跳过已装文件；或像增量那样给每个文件写 `.checkpoint`，重启只补未完成的。
- **收益**：大表崩溃后**只补差量**，而不是从零再来。

#### ② 用「一条/少数几条 COPY」替代逐文件 COPY

`COPY INTO table FROM @stage/<db>.<table>.` 一条语句覆盖所有文件，让 Snowflake 在 warehouse 内部自动跨线程并行——这才是推荐的批量装载姿势（代码里**非并行分支** `replicate/snapshot.go:152` 其实就是这么写的）。Go 侧 16 并发反而把它拆碎、压住了并行度。

#### ③ 调小 dump 文件 + 压缩（改参数，立竿见影）

- 把 Dumpling `FileSize` 从 5GiB 降到 **~128–256MB**，COPY 并行度大幅提升。
- 开 **gzip 压缩** CSV，省 S3 字节、加快 COPY 读取。

#### ④ dump 与 load 流水线化

不等整表导完，**文件一落 S3 就开始 COPY**（消费端边扫边装）。大表墙钟时间可近似砍半。需改 `Export`/`Replicate` 的串行结构。

#### ⑤ 装载期临时放大 warehouse

初始大快照前 `ALTER WAREHOUSE SET WAREHOUSE_SIZE='XLARGE'`（或多集群），装完缩回。COPY 吞吐基本线性涨，更快装完 = 更短计费时长，成本不一定增加。

#### ⑥ staging 表 + 原子 SWAP，消除空表窗口

COPY 装进 `table_staging`，完成后 `ALTER TABLE … SWAP WITH`（原子切换），下游永远看不到半成品，重启也不暴露空表。

#### ⑦ 超大 / 持续场景考虑 Snowpipe

serverless、异步、按文件计费、S3 事件自动触发，把装载从客户端进程解耦。属于较大的架构改动，适合「初始 TB 级 + 长期高频增量」场景。

---

## 三、优先级与落地建议

| 优先级 | 优化项 | 改动量 | 主要收益 |
|---|---|---|---|
| P0 | 增量档位 0：内存 set 替 HEAD | 极小 | 去掉每轮 O(N) 个 HEAD 请求 |
| P0 | 快照 ③：调小 dump 文件 + gzip 压缩 | 极小（改参数） | COPY 并行度↑、S3 成本↓ |
| P0（运维） | S3 Lifecycle 清理老分区 + 调大 flush-interval/file-size | 配置 | 增量文件堆积/请求费失控的止血 |
| P1 | 快照 ①：断点续传（去掉 retry 的 REPLACE / 加 per-file checkpoint） | 中 | 大表失败不再整表重灌 |
| P1 | 快照 ②：单条大 COPY 让 Snowflake 自己并行 | 小~中 | 大表装载吞吐↑ |
| P1 | 增量档位 1：delimiter + 关闭日期 | 中 | LIST 降到 O(日期) |
| P2 | 增量档位 2：progress.json + 读 CDC.index | 较大 | 稳态 O(1) + 秒级重启 |
| P2 | 快照 ④/⑤/⑥：流水线 / 放大 warehouse / staging+SWAP | 中~大 | 大表墙钟时间↓、消除空表窗口 |
| P3 | 快照 ⑦：Snowpipe | 大（架构） | 超大/持续场景成本与解耦 |

### 一句话总结

- **增量端**最该补 **扫描优化**：先零风险干掉 HEAD（档位 0），再用持久化游标 + `CDC.index` 把每轮成本压到 O(1)（档位 2）；运维侧配 Lifecycle 清理止血。
- **快照端**最该补 **断点续传**（利用 COPY 自带去重 / per-file checkpoint）；最容易拿分的是**调小 dump 文件 + 压缩 + 单条大 COPY 让 Snowflake 自己并行**；进一步可做流水线、放大 warehouse、staging+SWAP。
- 所有优化都**不改变「数据由 Snowflake 直读 S3 装入内部表」的本质**，只是把大表的墙钟时间、失败重灌代价、S3 成本显著降下来；**同步阻塞语义保持不变**（正确性地基）。
