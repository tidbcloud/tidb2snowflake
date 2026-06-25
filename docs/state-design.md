# State Design

本文记录 `tidb2snowflake` 的新 state 方案。

state 只能保存恢复和幂等必须依赖的字段。没有恢复语义的内容不进入 state。

旧方案里的 `loadinfo`、`.checkpoint`、`_consumer/progress.json` 和旧结构的 `tidb2snowflake.state.json` 不再参与恢复判断。

## Goals

新 state 解决三件事：

1. 记录 source job 状态，避免重启后重复创建 export 或 changefeed。
2. 记录每张表的 snapshot 是否已经完成，决定 bulk snapshot 是否可以跳过。
3. 记录每张表的 incremental 游标，保证 CDC 文件和 DDL 只在成功应用后推进。

## Storage Layout

只有一个 state 文件：

```text
tidb2snowflake.state.json
```

所有全局状态和每表状态都写入这个文件。调用方不能直接读写这个 JSON；只能通过 `pkg/state.Manager` 修改 state。

因为当前 loader 会按表并发运行，单文件 state 必须满足一个约束：同一个进程内只能有一个 `state.Manager` 持有并修改 state。manager 内部用锁串行化所有 read-modify-write。这个方案不支持多个 `tidb2snowflake` 进程同时使用同一个 storage prefix。

## Schema

完整 schema 如下：

```json
{
  "version": 1,
  "source": {
    "snapshot_tso": "449...",
    "export_id": "exp-xxx",
    "changefeed_id": "cf-xxx"
  },
  "tables": {
    "db.tbl": {
      "snapshot_done": false,
      "applied_dml": {
        "42/0/2026-06-25": 9
      },
      "ddl_watermark": 42
    }
  }
}
```

字段必须始终存在。暂时没有值时使用空值：

- string 字段用 `""`
- bool 字段用 `false`
- map 字段用 `{}`
- number 字段用 `0`

这样做可以减少兼容分支。读取 state 时只需要校验字段类型，不需要猜测缺字段表示什么。

## Field Analysis

### Root

| Field | Why it is required |
|---|---|
| `version` | state 格式版本。没有它，新代码无法可靠拒绝未知格式。 |
| `source` | source job 状态属于整个复制任务，不属于某一张表。 |
| `tables` | 每表状态的集合。state 只有一个文件，因此每表进度必须在这里。 |

### Source

| Field | Why it is required |
|---|---|
| `source.snapshot_tso` | snapshot 和 changefeed 的一致性锚点。changefeed 必须从同一个 TSO 开始，否则 snapshot 和 incremental 会断层或重叠。 |
| `source.export_id` | export 创建成功后必须记录。重启时用它等待或恢复同一个 export，避免创建重复 export。 |
| `source.changefeed_id` | changefeed 创建成功后必须记录。重启时用它等待或恢复同一个 changefeed，避免创建重复 changefeed。 |

`snapshot_tso` 写入后不能被不同值覆盖。`export_id` 和 `changefeed_id` 在对应模式不使用时保持空字符串，但字段仍然存在。

### Table

`tables` 的 key 是 table FQN，例如 `db.tbl`。value 是该表的同步状态。

| Field | Why it is required |
|---|---|
| `snapshot_done` | bulk snapshot 的唯一恢复门控。`true` 表示该表 snapshot 已经成功装载，可以跳过；`false` 表示下次从头执行 bulk snapshot。 |
| `applied_dml` | incremental 的 DML 文件级游标。每个 DML path key 记录已经成功应用的最大 file index。 |
| `ddl_watermark` | incremental 的 DDL 游标。删除旧 schema file mutation 后，必须用它判断某个 table version 的 DDL 是否已经应用。 |

表名不再作为 value 内字段重复保存。单文件 state 中，`tables` 的 key 已经是表身份；重复保存会制造两个表名来源。

## Snapshot: Bulk Only

snapshot 只保留 bulk 模式。

流程：

1. 如果 `tables[table].snapshot_done == true`，跳过 snapshot。
2. 如果为 `false`，执行 `CREATE OR REPLACE TABLE`。
3. 执行一次 `COPY INTO ... PATTERN`，把该表所有 snapshot 文件装入 Snowflake。
4. `COPY` 成功后，把 `snapshot_done` 写成 `true` 并保存 state。

失败恢复：

- 如果 `CREATE OR REPLACE TABLE` 或 `COPY` 失败，`snapshot_done` 仍然是 `false`。
- 下次运行重新建表并重新执行整表 `COPY`。
- 如果 `COPY` 成功但保存 state 失败，下次仍然会重新建表并重新 COPY。因为会先 `CREATE OR REPLACE TABLE`，不会产生重复数据，只会增加重跑成本。

## Why There Is No `load_mode`

`load_mode` 字段不需要存在。

之前讨论过 `bulk|per_file`，但现在 snapshot 只保留 bulk。只有一个模式时，`load_mode` 没有恢复语义。保留它只会让 state 多一个需要校验的字段。

bulk snapshot 的恢复判断只需要一个布尔值：

```json
{"snapshot_done": true}
```

它表达的含义足够明确：已经完成就跳过，没有完成就从头重跑。

## Incremental

incremental 以 CDC 文件为原子单位推进。

`applied_dml` 的 key 格式：

```text
<table_version>/<partition_num>/<date>
```

value 是该 key 下已经成功应用的最大 `file_index`。

例子：

```json
{
  "applied_dml": {
    "42/0/2026-06-25": 9,
    "42/1/2026-06-25": 4
  },
  "ddl_watermark": 42
}
```

字段含义：

| Part | Why it is required |
|---|---|
| `table_version` | TiCDC 路径按 table version 分组。不同 table version 的 DML 不能共用一个 file index。 |
| `partition_num` | 分区表会有多个 partition path。同一个 table version/date 下，不同 partition 的 file index 不能混用。 |
| `date` | 当前 cloud-storage sink 路径按日期分目录。不同日期下的 file index 不能混用。 |
| `file_index` | DML 文件级恢复的实际水位。只有对应文件 `MERGE` 成功后才能推进。 |
| `ddl_watermark` | 已经成功应用的最大 DDL table version。后续 schema file 的 table version 小于等于它时跳过 DDL。 |

DML 推进顺序：

```text
LoadIncrement succeeds
  -> update applied_dml
  -> save state
  -> continue next file
```

DDL 推进顺序：

```text
ExecDDL succeeds
  -> update ddl_watermark
  -> save state
  -> continue following DML
```

如果进程在 Snowflake 成功后、state 写入前失败，下次会重放最后一个 DML 文件或 DDL。DML 的 `MERGE` 按主键应保持幂等。DDL 是否能安全重放要用测试确认；如果某些 DDL 不能安全重放，需要在 DDL 执行层做幂等处理。

## State Initialization

启动时，`state.Manager` 负责把 state 归一化为完整 schema：

1. state 文件不存在时，创建空 state。
2. `version` 固定为 `1`。
3. `source` 下三个字段都存在，默认 `""`。
4. `tables` 下为当前配置中的每张表创建 entry。
5. 每张表默认：

```json
{
  "snapshot_done": false,
  "applied_dml": {},
  "ddl_watermark": 0
}
```

如果 state 文件存在但缺少必须字段、字段类型错误、版本不支持，直接报错，不做猜测修复。

## API Shape

`pkg/state` 隐藏 JSON 路径、锁和字段细节。调用方只表达状态变化。

```go
func (m *Manager) Load(ctx context.Context, tables []string) (*State, error)
func (m *Manager) Save(ctx context.Context) error

func (m *Manager) SnapshotDone(table string) bool
func (m *Manager) MarkSnapshotDone(ctx context.Context, table string) error

func (m *Manager) AppliedDML(table string, key DMLKey) uint64
func (m *Manager) MarkDMLApplied(ctx context.Context, table string, key DMLKey, fileIndex uint64) error

func (m *Manager) DDLWatermark(table string) uint64
func (m *Manager) MarkDDLApplied(ctx context.Context, table string, tableVersion uint64) error

func (m *Manager) Source() SourceState
func (m *Manager) SetSourceSnapshotTSO(ctx context.Context, tso string) error
func (m *Manager) SetExportID(ctx context.Context, id string) error
func (m *Manager) SetChangefeedID(ctx context.Context, id string) error
```

所有 `Mark*` 和 `Set*` 方法都必须在 manager 内部持锁、修改内存 state、写回同一个 `tidb2snowflake.state.json`。

## Removal Of Old Scheme

新方案不读取旧 marker：

- `db.table.loadinfo`
- `*.checkpoint`
- `_consumer/progress.json`
- 旧结构的 `tidb2snowflake.state.json`

切换成本：

- 已经跑过旧版本的 storage prefix 不能无缝 resume。
- 上线新版本前，要么使用新的空 storage prefix，要么手工生成新 state。
- 如果直接在旧 prefix 上跑，新代码会把旧 marker 当普通对象忽略。

这是用户明确接受“之前的方案直接删除”的前提。

## Required Fields

最终持久化字段只有这些：

```text
version
source.snapshot_tso
source.export_id
source.changefeed_id
tables
tables.<table>.snapshot_done
tables.<table>.applied_dml
tables.<table>.ddl_watermark
```

新增字段必须先证明一件事：没有它，重启恢复会错误、重复创建 source job、跳过未应用数据，或无法判断 DDL/DML 是否已经应用。不能证明就不进入 state。
