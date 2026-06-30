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
6. 用默认 16 个 worker 并发执行 `COPY INTO`。

当本次 snapshot 目录里需要同步的数据文件全部成功加载后，state 里的 `snapshot_finished` 会被设置为 `true`。下一次启动时，如果看到 `snapshot_finished=true`，snapshot 阶段会直接跳过。

如果复用已有 state 后新增 `--table`，新增表不会触发 snapshot backfill。这个行为是当前语义的一部分：`snapshot_finished=true` 表示该任务的 snapshot 阶段已经结束。

## 3. Incremental 数据同步到 Snowflake

入口是 `incremental.Load`。

incremental 阶段处理的是变更数据，不能像 snapshot 一样随意并发。当前实现的核心流程是：

1. 周期性读取 `increment/metadata`，拿到 TiCDC 已确认 flush 到 storage 的 checkpoint。
2. 通过实际存在的 `.index` 文件和 cloudstorage 路径规则，找出可消费的 schema 和 DML 文件。
3. 通过 state 中每个 DML stream 的 cursor 计算需要消费的 file range。
4. 按 table version、partition、date、file index 的顺序处理文件。
5. 先处理 schema/DDL，再处理 DML。
6. DML 通过 Snowflake `MERGE` 写入目标表。

DML 文件内部用 `$4` 作为 commit-ts。MERGE 时会加上 `TO_NUMBER($4) > checkpoint_ts`。`increment/metadata` 里的 checkpoint 是确认下界，不是消费上界；`.index` 指向的 DML 文件会完整消费。

loader 会在 state 里记录每张表每个 DML stream 已经完整消费到的 date 和 file index；相邻两轮 scan 会从这个 cursor 之后继续。如果 cursor 丢失或落后，代码会重新生成当前 checkpoint 之后的候选文件范围，并依赖 MERGE 的 commit-ts 过滤和主键幂等性收敛。

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
