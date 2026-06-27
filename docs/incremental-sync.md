# Incremental Sync

本文说明 incremental 阶段如何把 TiCDC 或 TiDB Cloud changefeed 写入对象存储的
增量文件同步到 Snowflake。

入口在 `cmd/core.go` 的 `loadIntoSnowflake`。只要 `--mode` 不是
`snapshot-only`，代码就会调用 `incremental.Load`。这个阶段是持续运行的：它按固定
间隔扫描 `increment/`，把可见范围内的 DDL 和 DML 写入 Snowflake。

## 输入目录和文件

Incremental 阶段读取对象存储里的 `increment/` 目录。

主要文件有三类：

- `increment/metadata`：source 侧写出的 checkpoint，字段是 `checkpoint-ts`。
- schema 文件：记录每个 table version 的表结构和 DDL 信息。
- `.index` 文件：TiCDC cloud storage sink 的可见性标记。只有 `.index` 指向的
  DML 文件才会被消费。

代码不会直接按目录里出现的 CSV 文件消费 DML。它先读 `.index`，再由 index 内容
推导出 DML 文件名。这一点保证只消费 source 侧已经标记可见的文件。

## Snowflake 连接和 Stage

`incremental.Load` 会创建 Snowflake connector：

```go
snowflake.NewConnector(
    cfg.Snowflake,
    "increment_external",
    cfg.StorageURI,
    cfg.Credential,
)
```

它会打开 Snowflake 连接，并创建名为 `increment_external` 的 external stage。
Stage URL 指向 S3 根路径，所以后续加载文件时会使用完整对象路径，例如
`increment/<db>/<table>/...`。

## Loader 初始化

`newLoader` 会从 `replication-state.json` 恢复已有进度：

- `incremental.checkpoint_ts`：上一次完成扫描的 checkpoint。
- `incremental.scan.high_watermark`：如果上次 scan 没完成，继续使用同一个
  high watermark。
- `incremental.tables.<table>.dml_file_watermarks`：每张表每个 DML scope 已完成的
  文件序号。
- `incremental.tables.<table>.ddl_table_version_watermark`：每张表已应用的 DDL table version。

每个 `--table` 会生成一个 `tableState`，里面保存：

- source database 和 source table。
- 已发现的 schema file。
- DML index 水位。
- 当前表结构 `currentMeta`。
- DDL table version 水位。

## 主循环

`loader.run` 使用 ticker 周期性运行。扫描间隔来自：

```go
ScanInterval: cfg.ChangefeedFlushInterval / 5
```

也就是 `--changefeed.flush-interval` 的五分之一。

每轮循环按这个顺序执行：

1. `beginScan`
2. `scanVisibleFiles`
3. `consumeVisibleFiles`
4. `finishScan`

如果 context 被取消，循环退出。

## 冻结 High Watermark

`beginScan` 决定本轮要消费到哪个 TSO。

流程是：

1. 从 state 读取 `incremental.checkpoint_ts`。
2. 如果 state 里已有 `incremental.scan.high_watermark`，说明上次 scan 没完成，
   本轮继续使用这个 high watermark。
3. 如果没有 active scan，读取 `increment/metadata`。
4. 如果 `metadata.checkpoint-ts <= checkpoint_ts`，说明没有新数据，本轮跳过。
5. 如果 `metadata.checkpoint-ts > checkpoint_ts`，把它写入
   `incremental.scan.high_watermark`，本轮只消费到这个 TSO。

这样做的目的很直接：一轮 scan 开始后，即使 source 侧继续写入更高 TSO 的数据，
本轮也只处理 high watermark 以内的内容。

## 扫描可见文件

`scanVisibleFiles` 会逐表调用 `getNewFiles`。

对每张表，`getNewFiles` 会扫描：

```text
increment/<db>/<table>/
```

扫描时处理两类文件：

### Schema 文件

如果路径是 schema file，调用 `parseSchemaFilePath`：

- 解析出 schema、table 和 table version。
- 跳过 `tableVersion > highWatermark` 的 schema。
- 只接受当前配置表对应的 schema。
- 读取 schema 文件内容并反序列化成 `cloudstorage.SchemaFile`。
- 校验路径里的 table version、schema、table 和文件内容一致。
- 保存到 `table.tableDefMap`。
- 同时为这个 schema version 添加一个特殊 DML key，用于后续按顺序执行 DDL。

### `.index` 文件

如果路径以 `.index` 结尾，调用 `parseDMLIndexFile`：

- 解析 index 文件路径，得到 DML path key。
- 跳过不属于当前表的 index。
- 跳过 `tableVersion > highWatermark` 的 index。
- 读取 index 文件内容，解析出实际 DML 文件名和文件序号。
- 校验 index 路径和 index 内容匹配。
- 当前代码不支持 table-across-nodes index 文件。
- 更新内存里的 DML 文件水位。

扫描结束后，`diffDMLMaps` 会比较本轮扫描出的 index 水位和 state 里已有的水位，
得出本轮需要处理的文件范围。范围形式是：

```text
start = 已完成文件序号 + 1
end   = index 文件指向的最新文件序号
```

## 并发模型

`consumeVisibleFiles` 以表为单位并发处理。

- 最大并发数是固定的 `tableConcurrency = 8`。
- 不同表可以并发。
- 同一张表内部保持顺序。

同表顺序由 `handleNewFiles` 保证：

1. 先按 `cloudstorage.CompareDMLPathKey` 排序 DML path key。
2. 对每个 table version，先处理 schema/DDL key。
3. 再处理该 version 下的 DML 文件。
4. DML 再按 dispatcher scope 排序。
5. 同一个 scope 内按文件序号从小到大处理。

如果某个 DML 文件没有被完整消费，当前表本轮处理会停止，不会越过这个文件继续处理后续文件。

## DDL 同步

DDL 处理入口是 `syncExecDDLEvents`。

每个 schema file 会转换成 `table.Meta`。处理规则：

- schema file 里必须有 primary key，否则报错。
- 如果 `tableDef.Query` 为空，表示初始化表结构，只更新内存里的 `currentMeta`，不执行 DDL。
- 如果 `tableVersion <= ddl_table_version_watermark`，表示 DDL 已应用，跳过执行。
- 否则调用 `snowflake.GenDDLViaMetaDiff`，基于前后两版表结构生成 Snowflake DDL。
- 逐条执行生成的 DDL。
- DDL 成功后，把 `ddl_table_version_watermark` 更新到当前 table version。

支持的 DDL 由 `pkg/snowflake/ddl.go` 决定。常见列变更会转成
`ALTER TABLE ADD COLUMN`、`DROP COLUMN`、`MODIFY`、`RENAME COLUMN`。部分 DDL
会直接返回错误，例如 create table、rename table、create schema 等。

## DML 同步

DML 处理入口是 `syncExecDMLEvents`。

它先根据 DML path key 和文件序号生成 DML 文件路径，然后调用：

```go
loader.conn.LoadIncrement(table.FromSchemaFile(tableDef), objectPath, loader.highWatermark)
```

Snowflake 侧执行的是 `MERGE INTO`：

- 从 stage 文件读取数据。
- `$1` 是 DML flag，作为 `METADATA$FLAG`。
- `$4` 是 commit-ts。
- 表字段从 `$5` 开始读取。
- `WHERE TO_NUMBER($4) <= highWatermark`，只消费本轮 high watermark 以内的行。
- 对同一 primary key，用 `QUALIFY row_number() ... order by $4 desc = 1` 取最新一行。
- `METADATA$FLAG = 'D'` 时删除。
- 非删除事件匹配到主键时 update。
- 非删除事件没匹配到主键时 insert。

MERGE 执行后，代码还会检查这个 DML 文件里是否存在
`commit-ts > highWatermark` 的行：

```sql
SELECT COUNT(*) FROM (
    SELECT 1
    FROM '<stage file>'
    WHERE TO_NUMBER($4) > <highWatermark>
    LIMIT 1
);
```

如果还有更高 TSO 的行，说明这个文件只被部分消费。代码不会推进该文件的
`dml_file_watermarks`，并且当前表本轮停止继续处理后续文件。

如果文件已完整消费，就更新 state：

```json
{
  "incremental": {
    "tables": {
      "db.table": {
        "dml_file_watermarks": {
          "<tableVersion>/<partition>/<date>/<dispatcherID>": <fileIndex>
        }
      }
    }
  }
}
```

## 完成一轮 Scan

当 `consumeVisibleFiles` 成功返回后，`finishScan` 会更新全局 checkpoint：

```json
{
  "incremental": {
    "checkpoint_ts": "<highWatermark>",
    "scan": null
  }
}
```

这表示本轮 high watermark 内的所有表都处理完成。下一轮 scan 会从新的
`checkpoint_ts` 继续。

## 失败和重跑

Incremental 阶段的恢复依赖 state：

- 如果进程在一轮 scan 中退出，`incremental.scan.high_watermark` 会保留下来。
  下次启动继续使用同一个 high watermark。
- 每个完整消费的 DML 文件会更新 `dml_file_watermarks`。重启后不会再从已完成文件之前开始。
- 如果 MERGE 成功但更新 state 失败，下次可能重放同一个文件。MERGE 按 primary key 写入，目标是保持结果幂等。
- 如果 DDL 已经在 Snowflake 执行成功但 state 没更新，重启后可能再次执行同一条 DDL。
  这是当前实现需要特别注意的恢复风险。
- 如果某个 DML 文件含有 `commit-ts > highWatermark` 的行，该文件不会推进水位，
  下一轮会继续从该文件开始。

当前实现不消费 table-across-nodes 的 index 文件；遇到这类 index 会返回错误。
