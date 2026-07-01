# Snowflake 目标对象布局

本文档记录 TiDB/MySQL 表同步到 Snowflake 时的目标对象布局。

## 目标

- 将上游两层命名 `source_database.source_table` 映射到 Snowflake 三层命名空间。
- 目标表在 Snowflake 里应该自然可查，不依赖带点的 quoted table name。
- stage 是加载基础设施，数量不能随表数量或上游 database 数量增长。
- 删除用户侧的 `--snowflake.schema`。目标 schema 从上游 database 推导。

## 表映射

上游表：

```text
source.t
```

目标表：

```text
ODS_DB.source.t
```

对应关系：

| 概念 | 来源 |
|---|---|
| Snowflake database | `--snowflake.database`，例如 `ODS_DB` 或 `RAW_DB` |
| Snowflake schema | 上游 TiDB/MySQL database 名，例如 `source` |
| Snowflake table | 上游 TiDB/MySQL table 名，例如 `t` |

生成 SQL 时，每一段 identifier 必须单独 quote：

```sql
"ODS_DB"."source"."t"
```

不要把上游的点号表名整体 quote 成一个 identifier：

```sql
-- 错误：这会在当前 schema 下创建一个名为 source.t 的表。
"source.t"
```

## Schema 创建

snapshot 数据里包含上游 database DDL 文件，例如 `test-schema-create.sql`：

```sql
CREATE DATABASE `test` /*!40100 DEFAULT CHARACTER SET utf8mb4 */;
```

在 Snowflake 里，这个上游 database 应映射为目标 database 下的 schema：

```sql
CREATE SCHEMA IF NOT EXISTS "ODS_DB"."test";
```

它不映射为 Snowflake database。Snowflake database 始终是用户通过
`--snowflake.database` 指定的目标贴源层 database。

snapshot prepare 阶段先扫描存在的 `*-schema-create.sql` 文件；如果该 database
出现在配置表里，就在目标 database 下创建对应 schema。随后再读取
`db.table-schema.sql` 创建表。
`CreateTable` 只负责创建表，不隐式创建 schema。

Dumpling 文件名会转义特殊字符，例如上游 database `d.b` 的 schema create
文件名是 `d%2Eb-schema-create.sql`，对应的 Snowflake schema 仍是 `"d.b"`。

## Stage 布局

Snowflake named stage 是 schema-scoped object。使用 named stage 时，它必须属于
某个 Snowflake schema。

采用一个固定的 external stage：

```text
ODS_DB.TIDB2SNOWFLAKE_INTERNAL.tidb2snowflake_external
```

它的三段含义如下：

| 段 | 示例 | 来源 | 含义 |
|---|---|---|---|
| Snowflake database | `ODS_DB` | 用户传入的 `--snowflake.database` | 目标 raw/ODS database |
	| Snowflake schema | `TIDB2SNOWFLAKE_INTERNAL` | tidb2snowflake 固定内部名 | 内部 schema，只放 tidb2snowflake 的基础设施对象 |
	| Snowflake stage | `tidb2snowflake_external` | tidb2snowflake 固定内部名 | 唯一的 external stage，指向 `--storage` 根路径 |

`ODS_DB` 只是示例。实际名字由用户配置决定，例如 `RAW_DB` 或 `DEV_ODS`。
`TIDB2SNOWFLAKE_INTERNAL` 和 `tidb2snowflake_external` 是程序内部对象名，不来自
上游 TiDB/MySQL，也不作为用户配置项暴露。

完整 stage identifier 必须按三段分别 quote：

```sql
@"ODS_DB"."TIDB2SNOWFLAKE_INTERNAL"."tidb2snowflake_external"
```

目标表按上游 database 分 schema，并不意味着 stage 也要按上游 database 拆分。
stage 是加载基础设施，不是业务数据对象。对一个 replication workspace 来说，
stage 数量固定为 1：

```text
tidb2snowflake_external
```

拒绝按 source database 创建 stage：

```text
ODS_DB.source1.snapshot_external
ODS_DB.source1.increment_external
ODS_DB.source2.snapshot_external
ODS_DB.source2.increment_external
```

这种布局会让 stage 数量变成：

```text
2 * distinct(source_database)
```

这个增长方式不可接受。

snapshot 和 incremental 共用同一个 stage。阶段差异通过 storage object path 表达：

```text
snapshot/...
increment/...
```

`--storage` 只用于创建 external stage 的 URL。`COPY` 和 `MERGE ... SELECT FROM`
都引用 named stage，不在加载 SQL 里直接展开 storage URI。

snapshot 加载：

```sql
COPY INTO "ODS_DB"."source"."t"
FROM @"ODS_DB"."TIDB2SNOWFLAKE_INTERNAL"."tidb2snowflake_external"
FILES = ('snapshot/source.t.000000001.csv')
FILE_FORMAT = (...);
```

incremental 加载：

```sql
MERGE INTO "ODS_DB"."source"."t" AS T
USING (
  SELECT ...
  FROM @"ODS_DB"."TIDB2SNOWFLAKE_INTERNAL"."tidb2snowflake_external"/increment/...
) AS S
...
```

程序启动时应创建内部 schema 和 stage：

```sql
CREATE SCHEMA IF NOT EXISTS "ODS_DB"."TIDB2SNOWFLAKE_INTERNAL";
CREATE OR REPLACE STAGE "ODS_DB"."TIDB2SNOWFLAKE_INTERNAL"."tidb2snowflake_external" ...;
```

`TIDB2SNOWFLAKE_INTERNAL` 只是 tidb2snowflake 自己放 stage 的 schema 名。
它不参与业务映射规则，也不是用户需要配置的目标 schema。

## 配置

保留：

```text
--snowflake.database
--snowflake.warehouse
```

删除：

```text
--snowflake.schema
```

`--snowflake.database` 选择目标 raw/ODS database。source schema 在 snapshot
prepare 阶段根据上游 database 名创建。

`--snowflake.warehouse` 选择执行 DDL、`COPY` 和 `MERGE` 的 Snowflake warehouse。

## 代码设计

Snowflake identifier 拼接应该集中在 `pkg/snowflake`。调用方传结构化的源库、
源表信息，不要提前拼好目标 SQL 字符串。

需要的基础能力：

```go
quoteQualifiedIdent(idents ...string) string
```

不要为 table/stage 再套一层只转发参数的 wrapper。生成表名和 stage 名时，
直接用 `quoteQualifiedIdent` 拼完整路径：

```sql
"<target_database>"."<source_database>"."<source_table>"
@"<target_database>"."TIDB2SNOWFLAKE_INTERNAL"."tidb2snowflake_external"
```

SQL 生成不依赖 current schema 解析 stage，也不要重新引入 `source.table`
这种点号拼接 helper 来生成 Snowflake 对象名。
