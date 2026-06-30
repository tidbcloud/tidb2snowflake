# Snapshot Sync

本文说明 snapshot 阶段如何把对象存储里的全量数据同步到 Snowflake。

入口在 `cmd/core.go` 的 `loadIntoSnowflake`。只要 `--mode` 不是
`incremental-only`，代码就会进入 snapshot 阶段。真正执行加载的是
`snapshot.Load`。

## 进入条件

Snapshot 阶段会先看 state：

- 如果 `snapshot.finished=true`，直接跳过 snapshot load。
- 如果 `snapshot.finished=false`，调用 `snapshot.Load`。
- `snapshot.Load` 全部成功后，`markSnapshotFinished` 会把 `snapshot.finished`
  写成 `true`。
- 如果 `incremental.checkpoint_ts` 还是 0，`markSnapshotFinished` 会把它初始化成
  `snapshot.tso`。

`snapshot.tso` 必须已经存在。它来自 source 准备阶段：TiDB Cloud export 返回值、
OP 模式记录的当前 TSO，或 `snapshot/metadata` 里的 `Pos`。

## 输入文件

Snapshot 阶段读取对象存储里的 `snapshot/` 目录。

每张表至少需要：

- schema 文件：`snapshot/<db>.<table>-schema.sql`
- 数据文件：文件名以 `<db>.<table>.` 开头，并且包含 `.csv`

数据文件可以是普通 CSV，也可以是 gzip 压缩后的 CSV。是否按 gzip 读取由
`--snapshot.compression` 决定。

## Snowflake 连接和 Stage

`snapshot.Load` 会先创建 Snowflake connector：

```go
snowflake.NewConnector(
    cfg.Snowflake,
    snowflake.SnapshotStageName,
    cfg.StorageURI,
    cfg.Credential,
    cfg.Compression,
)
```

这个 connector 会：

1. 打开 Snowflake 连接。
2. 执行 `CREATE DATABASE IF NOT EXISTS`。
3. 执行 `CREATE SCHEMA IF NOT EXISTS`。
4. 创建 external stage，stage 名为 `snapshot_external`。
5. Stage URL 指向配置的 S3 根路径。

Stage 的 file format 固定为 CSV，`NULL_IF=('\\N')`，字段可用双引号包裹，
反斜杠作为 escape，binary format 为 `HEX`。

Snapshot load 自己执行 `COPY INTO` 时会再指定 file format。这里会按
`--snapshot.compression` 设置 `COMPRESSION = 'NONE'` 或 `COMPRESSION = 'GZIP'`。

## 建表流程

`snapshot.Load` 先调用 `createTables`。

对每个 `--table db.table`：

1. 拆出 source database 和 source table。
2. 读取 `snapshot/<db>.<table>-schema.sql`。
3. 用 TiDB parser 解析 `CREATE TABLE`。
4. 生成 `table.Meta`。
5. 检查表必须有 primary key。
6. 调用 `conn.CreateTable` 在 Snowflake 建表。

Snowflake 目标表名来自 `table.Meta.SnowflakeTableName()`，当前实现是
`<source_schema>.<source_table>`。

建表 SQL 使用：

```sql
CREATE OR REPLACE TABLE "<schema>.<table>" (...)
```

这意味着 snapshot 阶段重新执行时，会重建目标表。state 写入失败后再次运行，
snapshot load 可以重新加载整张表。

## 数据文件扫描

建表完成后，`loadSnapshotFiles` 开始加载数据文件。

流程是：

1. 创建一个任务 channel。
2. 启动固定 8 个 worker。
3. 调用 `store.WalkDir` 扫描 `snapshot/`。
4. 对每个对象路径调用 `snapshotTaskForFile`。
5. 如果文件名能解析出 `<db>.<table>`，把任务发给 worker。

`snapshotTaskForFile` 的匹配规则很简单：

- 取文件 basename。
- 文件名必须是 `.csv` 或 `.csv.gz`。
- 文件名前两段必须是 `<db>.<table>`。
- 目标 Snowflake 表名直接使用这两段拼出的 `<db>.<table>`。

不匹配的文件会被忽略，比如说明文件，或不是 CSV 的文件。

## 数据加载

每个 worker 调用：

```go
conn.LoadSnapshot(targetTable, filePath)
```

最终执行的是：

```sql
COPY INTO "<target_table>"
FROM @snapshot_external
FILES = ('<filePath>')
FILE_FORMAT = (...);
```

每个 snapshot 文件独立执行一次 `COPY INTO`。如果任意文件加载失败：

- 当前 worker 返回错误。
- `loadSnapshotFiles` 返回错误。
- `snapshot.Load` 返回错误。
- `snapshot.finished` 不会写成 `true`。

全部文件加载成功后，`snapshot.Load` 返回成功，`loadIntoSnowflake` 再调用
`markSnapshotFinished` 更新 state。

## State 更新

Snapshot 阶段只在全部文件加载成功后推进 state：

```json
{
  "snapshot": {
    "tso": "...",
    "finished": true
  },
  "incremental": {
    "checkpoint_ts": "<snapshot tso>"
  }
}
```

`checkpoint_ts` 只在原值为 0 时初始化。这样 incremental 阶段会从 snapshot TSO
之后开始消费。

## 失败和重跑

Snapshot 阶段的恢复边界是 phase 级别：

- `snapshot.finished=false`：下一次运行会重新执行 snapshot load。
- `snapshot.finished=true`：下一次运行会跳过 snapshot load。
- Snowflake 建表使用 `CREATE OR REPLACE TABLE`，所以 snapshot 阶段重复执行会重建表再加载。
- 如果某个文件 COPY 成功，但 state 还没写入 `snapshot.finished=true`，下一次运行会重跑整个 snapshot 阶段。

当前实现没有记录单个 snapshot 文件的加载水位。
