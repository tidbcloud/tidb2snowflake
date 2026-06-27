# Source Job Creation

本文整理当前代码里 source 侧任务是怎么创建的，重点是 OP 模式下的
Dumpling snapshot dump 和 changefeed 创建。TiDB Cloud 模式也放在一起，
方便对比。

主入口是 `cmd/core.go` 的 `Replicate`。它会先生成对象存储路径和 state manager，
然后调用 `source.Prepare`。`source.Prepare` 根据 `--source.mode` 选择 runner：

- `tidbcloud`：使用 `tidbCloudRunner`，通过 TiDB Cloud OpenAPI 创建 export 和
  changefeed。
- `op`：使用 `opRunner`，通过 Dumpling dump snapshot，通过 TiCDC OpenAPI 创建
  changefeed。

对象存储固定使用两个目录：

- `snapshot/`：snapshot 数据、schema 文件和 `snapshot/metadata`。
- `increment/`：changefeed 写出的 incremental 文件、schema、`.index` 和
  `increment/metadata`。

## 通用创建逻辑

`pkg/source/source.go` 里的 `ensureManagedJob` 封装了 export/changefeed 的通用逻辑。

执行顺序是：

1. 先看 `replication-state.json` 里是否已有 job id。
2. 如果已有 id，就调用 runner 的 `waitJob`，继续等待同一个 source job。
3. 如果没有 id，再检查对应对象存储目录是否已有文件。
4. 如果目录里已有数据，就跳过 source job 创建。
5. 如果没有 id、目录也没有数据，才调用 runner 的 `createJob`。
6. 创建成功后，把返回的 job id 写入 state。
7. 随后等待该 job ready，再把等待结果同步回 state。

这个逻辑用于 TiDB Cloud export、TiDB Cloud changefeed 和 OP TiCDC changefeed。
Dumpling 不走 `ensureManagedJob`，因为当前实现没有 Dumpling job id；它是在本进程里
直接创建 dumper 并执行 dump。

## OP 模式：整体顺序

OP 模式入口是 `pkg/source/op.go` 的 `opRunner.prepare`。

在 `--mode=full` 下，顺序是：

1. 检查 `snapshot/` 是否已有数据。
2. 如果已有 snapshot 数据，读取 `snapshot/metadata` 里的 `Pos`，写入 state 的
   `snapshot.tso`。
3. 如果没有 snapshot 数据，且 state 里还没有 `snapshot.tso`，连接 TiDB 执行
   `SELECT @@tidb_current_ts`，把当前 TSO 写入 state。
4. 创建或等待 TiCDC changefeed。changefeed 从 state 里的 `snapshot.tso` 开始。
5. 如果 snapshot 还没完成、对象存储里也没有 snapshot 数据，就运行 Dumpling dump。
6. Dumpling 完成后，再读取 `snapshot/metadata` 里的 `Pos`，并校验它和 state 里的
   `snapshot.tso` 一致。

在 `--mode=snapshot-only` 下，只处理 Dumpling snapshot，不创建 TiCDC changefeed。

在 `--mode=incremental-only` 下，跳过 Dumpling，只创建或等待 changefeed。

## OP 模式：Dumpling dump

Dumpling 的调用点在 `opRunner.prepare`：

```go
dumpling.Run(ctx, cfg.TiDB, dumpling.Config{...})
```

传入的核心配置来自 CLI 和运行期 state：

| 字段 | 来源 | 用途 |
|---|---|---|
| `Concurrency` | `--snapshot.concurrency`，默认 8 | Dumpling dump 并发数。 |
| `StorageURI` | `snapshot/` S3 URI，带 AWS 凭据 query | Dumpling 输出目录。 |
| `SnapshotTSO` | state 里的 `snapshot.tso` | 固定 Dumpling 读取的 TiDB snapshot。 |
| `Tables` | `--table` | 只 dump 指定表。 |
| `Compression` | `--snapshot.compression` | snapshot 文件压缩方式。 |
| `CSVNullValue` | 固定 `\N` | CSV 里的 NULL 表示。 |
| `OnProgress` | 代码内回调 | 每 10 秒记录一次 dump 进度。 |

`pkg/dumpling/dump.go` 的 `BuildConfig` 会把这些字段转换成 TiDB Dumpling 的
`export.Config`：

- TiDB 连接使用 `--tidb.host`、`--tidb.port`、`--tidb.user`、`--tidb.pass`。
- 如果设置了 `--tidb.ssl-ca`，会写到 `conf.Security.CAPath`。
- 输出格式固定为 CSV。
- `NoHeader=true`，不输出 CSV header。
- 分隔符是 `,`，引号是 `"`，NULL 是 `\N`。
- `EscapeBackslash=false`。
- `TransactionalConsistency=true`。
- `CsvOutputDialect=SNOWFLAKE`。
- `OutputDirPath` 是 `snapshot/` S3 URI。
- `FileSize` 默认 `5GiB`。
- `Compression=none` 会转换成 Dumpling 的 `no-compression`。
- `Tables` 通过 `export.GetConfTables(cfg.Tables)` 解析。
- `ExtStorage` 通过 `GetExternalStorageWithDefaultTimeout` 打开。

`dumpling.Run` 的实际执行步骤：

1. 调用 `BuildConfig` 生成 Dumpling 配置。
2. 用 `tidb.OpenDB` 打开 TiDB 连接，确保源端可连。
3. 调用 `export.NewDumper(ctx, dumpConfig)` 创建 dumper。
4. 如果配置了 `OnProgress`，启动一个 goroutine，每 10 秒读一次
   `dumper.GetStatus()` 并写日志。
5. 调用 `dumper.Dump()` 执行 dump。
6. dump 成功后记录最终 `dumper.GetStatus()`。

当前实现不会把 Dumpling dump 记录成一个可恢复的远端 job。恢复主要依赖
`snapshot/` 目录是否已有数据，以及 `snapshot/metadata` 里的 `Pos`。

## OP 模式：TiCDC changefeed

OP changefeed 的创建点是 `opRunner.createJob`。

创建前先构造 TiCDC client：

```go
ticdc.NewClient(cfg.OP.TiCDCAddress)
```

如果 `--ticdc.address` 没有 scheme，代码会自动补 `http://`。请求路径会补成
`/api/v2/...`。

然后调用 `ticdc.BuildChangefeedConfig` 构造 TiCDC OpenAPI 请求体。核心配置是：

| 字段 | 值 |
|---|---|
| `Tables` | `--table` 列表。 |
| `StorageURI` | `increment/` S3 URI，带 AWS 凭据 query。 |
| `StartTSO` | state 里的 `snapshot.tso`，解析成 `uint64`；空值为 0。 |
| `FlushInterval` | `--changefeed.flush-interval`，默认 60s。 |
| `FileSizeMiB` | `--changefeed.file-size`，默认 64 MiB。 |

`BuildChangefeedConfig` 会把 sink URI query 改成：

- `protocol=csv`
- `flush-interval=<duration>`
- `file-size=<bytes>`

同时设置 replica config：

- `Filter.Rules = --table` 列表。
- `Sink.Protocol = csv`。
- `Sink.CSVConfig.IncludeCommitTs = true`。
- `Sink.CSVConfig.BinaryEncodingMethod = hex`。
- `Sink.DateSeparator = day`。
- `Sink.CloudStorageConfig.FlushInterval = <duration>`。
- `Sink.CloudStorageConfig.FileSize = <bytes>`。
- `Sink.CloudStorageConfig.OutputColumnID = true`。

创建请求是：

```text
POST /api/v2/changefeeds
```

创建成功后，返回的 changefeed id 会写入 state 的
`task_info.changefeed_id`。

等待逻辑是：

```text
GET /api/v2/changefeeds/{changefeed_id}?namespace=default
```

当 TiCDC 返回 `normal` 或 `warning` 时，认为 changefeed ready。返回
`failed`、`stopped`、`removed`、`finished` 时，本次运行失败。

## TiDB Cloud 模式：export 和 changefeed

TiDB Cloud 模式入口是 `pkg/source/tidbcloud.go` 的 `tidbCloudRunner.prepare`。

Snapshot 不是用本地 Dumpling 创建，而是通过 TiDB Cloud Export OpenAPI 创建：

```text
POST /v1beta1/clusters/{clusterID}/exports
```

Export 请求体由 `buildExportRequest` 生成：

- `DisplayName = tidb2snowflake-snapshot`
- `FileType = CSV`
- `Compression = NONE` 或 `GZIP`
- `EscapeBackslash = false`
- `Filter.Table.Patterns = --table` 列表
- CSV 格式使用 `,`、`"`、`\N`、`SkipHeader=true`
- `Dialect = SNOWFLAKE`
- 目标是 S3 `snapshot/` URI
- 认证方式是 S3 access key
- 如果 state 或参数里已有 snapshot TSO，会写入 `SnapshotTSO`

TiDB Cloud export 成功后会返回 `exportId` 和有效的 `snapshotTso`。代码会把
`exportId` 写入 `task_info.export_id`，并把 `snapshotTso` 写入 state。之后还会读
`snapshot/metadata` 里的 `Pos`，作为 snapshot TSO 的最终来源。

TiDB Cloud changefeed 也是通过 OpenAPI 创建：

```text
POST /v1beta1/clusters/{clusterID}/changefeeds
```

请求体由 `buildChangefeedRequest` 生成：

- `DisplayName = tidb2snowflake-incremental`
- sink 类型是 `CLOUD_STORAGE`
- 存储类型是 `S3`
- S3 URI 是不带凭据 query 的 `increment/` URI
- S3 认证方式是 access key，凭据放在 request body 里
- 数据格式是 `CSV`
- `IncludeCommitTs = true`
- 二进制编码是 `HEX`
- `DateSeparator = DAY`
- `IntervalInSeconds = --changefeed.flush-interval`
- `SizeInMiB = --changefeed.file-size`
- `OutputColumnID = true`
- `Filter.FilterRule = --table` 列表
- `Filter.Mode = FORCE_SYNC`

Start position 的规则：

- 如果 state 里有 `snapshot.tso`，使用 `FROM_TSO`，并把该 TSO 写入请求体。
- 如果没有 `snapshot.tso`，使用 `FROM_NOW`。

在 `--mode=full` 下，代码要求创建 changefeed 前必须已经拿到 `snapshot.tso`。
在 `incremental-only` 场景下，如果没有 snapshot TSO，TiDB Cloud changefeed 会从
`FROM_NOW` 开始。

等待逻辑是：

```text
GET /v1beta1/clusters/{clusterID}/changefeeds/{changefeedID}
```

状态为 `RUNNING` 或 `WARNING` 时认为 ready。状态为 `CREATE_FAILED`、
`RUNNING_FAILED`、`DELETING`、`DELETED` 时，本次运行失败。

## State 和恢复语义

`replication-state.json` 里和 source job 创建相关的字段是：

- `task_info.export_id`
- `task_info.changefeed_id`
- `snapshot.tso`
- `snapshot.finished`

恢复时的行为：

- 如果 state 里已有 export/changefeed id，继续等待同一个 job。
- 如果没有 id，但对象存储里已有 `snapshot/` 或 `increment/` 数据，跳过对应 source
  job 创建。
- 如果 `snapshot.finished=true`，OP 模式会跳过 Dumpling dump；Snowflake load 阶段也会跳过 snapshot load。
- 如果 state 里的 `snapshot.tso` 和 source job 或 `snapshot/metadata` 里的 TSO 不一致，
  运行会失败。

这套语义保证同一个 storage prefix 下重复运行时，优先复用已有 source job 和已有对象存储数据。
