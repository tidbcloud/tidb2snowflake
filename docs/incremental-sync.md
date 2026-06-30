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
    snowflake.IncrementStageName,
    cfg.StorageURI,
    cfg.Credential,
    "",
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
2. `processTables`
3. `FinishIncrementalScan`

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

`processTables` 会先发现 rename table schema file，再逐表调用 `getNewFiles`。

对每张表，`getNewFiles` 会扫描：

```text
increment/<db>/<table>/
```

有一个例外：`RENAME TABLE old TO new` 的 schema file 会落在新表目录下。
例如 `--table db.old` 时，TiCDC 可能写出：

```text
increment/db/new/meta/schema_<table-version>_<checksum>.json
```

所以每轮 scan 开始时，loader 会先列出同一个 database 下的表目录，只读取这些
目录里的 `meta/schema_*`，找出 `RENAME TABLE old TO new` 这类 schema file。
如果 rename 的 source table 是当前配置表 `db.old`，这个 schema file 会被挂回
`db.old` 的处理队列，按 `db.old` 的 table version 顺序执行 DDL。

这个逻辑只用于发现 rename table 的 schema file。DML 仍然只按配置表路径扫描：

```text
increment/db/old/<table-version>/<date>/meta/CDC.index
```

也就是说，当前语义是：精确配置 `--table db.old` 时，rename DDL 会同步到
Snowflake，把目标表从 `db.old` 改名为 `db.new`；但 loader 不会自动切换到
`db.new` 继续消费 DML。这个行为和 TiCDC 精确 table filter 下的输出一致：rename
DDL 会出现，rename 后新表名下的后续 DML 不会继续写出给这条同步任务。

扫描时处理两类文件：

### Schema 文件

如果路径是 schema file，调用 `parseSchemaFilePath`：

- 解析出 schema、table 和 table version。
- 跳过 `tableVersion > highWatermark` 的 schema。
- 普通 schema file 必须属于当前配置表；rename table schema file 可以来自新表目录，
  但它的 source table 必须是当前配置表。
- 保存 schema file 路径；真正执行 DDL 或加载 DML 前再读取文件内容并反序列化成
  `cloudstorage.SchemaFile`。
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

`processTables` 以表为单位并发处理。

- 最大并发数是固定的 `tableConcurrency = 8`。
- 不同表可以并发。
- 同一张表内部保持顺序。

同表顺序由 `handleNewFiles` 保证：

1. 先按 `cloudstorage.CompareDMLPathKey` 排序 DML path key。
2. 对每个 table version，先处理 schema/DDL key。
3. 再处理该 version 下的 DML 文件。
4. DML 再按 file index 顺序执行。
5. 同一个 scope 内按文件序号从小到大处理。

如果某个 DML 文件没有被完整消费，当前表本轮处理会停止，不会越过这个文件继续处理后续文件。

## DDL 同步

DDL 处理入口是 `execDDL`。

每个 schema file 会转换成 `table.Meta`。处理规则：

- schema file 里必须有 primary key，否则报错。
- 如果 `tableDef.Query` 为空，表示初始化表结构，只更新内存里的 `currentMeta`，不执行 DDL。
- 如果 `tableVersion <= ddl_table_version_watermark`，表示 DDL 已应用，跳过执行。
- 否则调用 `snowflake.GenDDLViaTiDBDDL`，基于 schema file 里的 `query` 生成 Snowflake DDL。
- 逐条执行生成的 DDL。
- DDL 成功后，把 `ddl_table_version_watermark` 更新到当前 table version。

支持的 DDL 由 `pkg/snowflake/ddl.go` 决定。常见列变更会转成
`ALTER TABLE ADD COLUMN`、`DROP COLUMN`、`MODIFY`、`RENAME COLUMN`。`RENAME TABLE`
会转成 Snowflake 的 `ALTER TABLE ... RENAME TO ...`。部分 DDL 会直接返回错误，
例如 create table、create schema 等。

## DDL Query 驱动的实现

TiCDC 写出的 schema file 里同时有两类信息：

- `query`：上游实际执行的 DDL 语句，表达这次变更的意图。
- `columns`：DDL 执行后的表结构，表达变更后的结果。

调整前的实现会把 schema file 转成表结构，再基于前后两版表结构生成 Snowflake DDL。
这条路径的问题不在于某个具体场景无法判断，而在于 schema file 已经带了 `query`：
每个 schema file 本来就对应一次 DDL 事件，没有必要再维护一套 column diff 翻译逻辑。
`table.Column.ID` 也不应该为了这条路径留在通用的 `table.Column` 里。

现在的处理规则是：

- `query` 用来判断这一个 schema file 对应的 DDL 动作。
- `columns` 只用来提供 DDL 执行后的标准列定义，例如类型、nullable、default。
- `snowflake.GenDDLViaTiDBDDL(prevMeta, nextMeta, actionType, query)` 是唯一的
  Snowflake DDL 生成入口。
- `tableDef.Query == ""` 表示初始化 schema file，不执行 Snowflake DDL；处理成功后
  更新内存里的 `currentMeta`，并推进 `ddl_table_version_watermark`。
- 列级 `ALTER TABLE` 用 TiDB parser 解析 AST，支持 `ADD COLUMN`、`DROP COLUMN`、
  `RENAME COLUMN`、`MODIFY COLUMN`、`CHANGE COLUMN`、`ALTER COLUMN ... DROP DEFAULT`。
- `ADD`、`MODIFY`、`CHANGE` 需要列定义时，从 `nextMeta` 查找变更后的列；`DROP` 和
  `RENAME` 直接使用 DDL AST 里的列名。
- `RENAME TABLE` 支持执行 Snowflake 表改名；它不改变当前任务的配置表名，也不让
  DML 扫描路径从旧表名切换到新表名。
- 非列级变更保持保守策略：不需要同步到 Snowflake 的 index、constraint、table option
  变更会跳过；需要但暂不支持的表级 DDL 继续返回明确错误。
- `table.Column.ID`、旧的 column diff 路径，以及 source 侧请求 column ID 的配置都已经删除。

调整后的接口关系会更直接：incremental 只负责按 table version 顺序拿到 schema
file；Snowflake DDL 生成模块负责把 TiDB DDL 语义翻译成 Snowflake DDL；schema file
里的 `columns` 作为变更后的表结构事实存在，不承担“推断这次做了什么”的职责。

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
          "<tableVersion>/<partition>/<date>": <fileIndex>
        }
      }
    }
  }
}
```

## 完成一轮 Scan

当 `processTables` 成功返回后，`FinishIncrementalScan` 会更新全局 checkpoint：

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
