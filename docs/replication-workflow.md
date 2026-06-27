# Replication Workflow

当前代码库的主流程可以分成三部分：

1. source 侧数据生产和任务管理
2. 同步 snapshot 数据到 Snowflake
3. 同步 incremental 数据到 Snowflake

入口在 `cmd/core.go` 的 `Replicate`。它负责初始化 storage、state 和 source runner，然后进入 Snowflake load 流程。Snowflake load 内部先处理 snapshot，再处理 incremental。

## 1. Source 侧数据生产和任务管理

这一部分负责让上游把数据写到对象存储。

TiDB Cloud 模式下，代码会通过 OpenAPI 创建或等待 export 和 changefeed 任务。OP 模式下，代码会调用 Dumpling 产出 snapshot 文件，并通过 TiCDC 产出 incremental 文件。

对象存储目录约定：

- `snapshot/`：snapshot 数据、schema 文件和 metadata
- `increment/`：incremental 数据、schema 文件、`.index` 文件和 metadata

snapshot TSO 的最终来源是 `snapshot/metadata` 里的 `Pos`。代码会读取该值并写入 state，作为 incremental 阶段的起点。

这部分不负责写 Snowflake，只负责准备可消费的上游文件。

## 2. Snapshot 数据同步到 Snowflake

入口是 `snapshot.Load`。

snapshot 阶段处理的是静态历史数据。当前实现会：

1. 读取每张表的 snapshot schema 文件。
2. 生成 `table.Meta`，其中 `Schema` 和 `Table` 保留源端原始值。
3. 基于 `table.Meta.SnowflakeTableName()` 生成 Snowflake 目标表名。
4. 在 Snowflake 创建目标表。
5. 扫描 `snapshot/` 下匹配配置表的 CSV 文件。
6. 用固定 8 个 worker 并发执行 `COPY INTO`。

当本次 snapshot 目录里需要同步的数据文件全部成功加载后，state 里的 `snapshot.finished` 会被设置为 `true`。下一次启动时，如果看到 `snapshot.finished=true`，snapshot 阶段会直接跳过。

如果复用已有 state 后新增 `--table`，新增表不会触发 snapshot backfill。这个行为是当前语义的一部分：`snapshot.finished=true` 表示该任务的 snapshot 阶段已经结束。

## 3. Incremental 数据同步到 Snowflake

入口是 `incremental.Load`。

incremental 阶段处理的是变更数据，不能像 snapshot 一样随意并发。当前实现的核心流程是：

1. 周期性读取 `increment/metadata`，拿到 source 侧已经可见的 checkpoint。
2. 在 state 中记录本轮 scan 的 high watermark。
3. 通过 `.index` 文件和 cloudstorage 路径规则，找出本轮严格可见的 schema 和 DML 文件。
4. 按 table version、partition、date、file index 的顺序处理文件。
5. 先处理 schema/DDL，再处理 DML。
6. DML 通过 Snowflake `MERGE` 写入目标表。

DML 文件内部用 `$4` 作为 commit-ts。MERGE 时会加上 `TO_NUMBER($4) <= highWatermark`，只消费本轮 scan 范围内的行。

如果一个 DML 文件里还存在 `commit-ts > highWatermark` 的行，当前文件不会推进 state 里的 `dml_file_watermarks`，本轮也不会继续越过它处理后续文件。这样可以避免 SQL 层截断了文件内容，但 state 却错误地认为整个文件已经完成。

## 支撑模块

除了三段主流程，代码里还有几类支撑模块：

- `pkg/state`：持久化 snapshot 和 incremental 的恢复进度。
- `pkg/snowflake`：封装 Snowflake 连接、建表、stage、COPY、MERGE 和 DDL。
- `pkg/table`：保存源端表结构，并提供派生 Snowflake 目标表名的方法。
- `pkg/tidbcloud`、`pkg/ticdc`、`pkg/dumpling`：封装 source 侧任务创建和等待。
- `pkg/metrics`、`pkg/utils`：通用辅助能力。

整体链路可以概括为：

```text
source 侧产出对象存储文件
  -> snapshot 静态全量加载
  -> incremental 按 checkpoint 范围持续消费
```
