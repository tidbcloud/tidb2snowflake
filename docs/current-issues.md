# tidb2snowflake 当前问题与修复建议

本文记录一次仓库阅读后的问题清单。范围包括 CLI 编排、快照加载、增量回放、Snowflake SQL 生成、测试覆盖和运维可观测性。

## 当前意图

`tidb2snowflake` 目标是把 TiDB Cloud Serverless / Essential 的数据复制到 Snowflake。工具通过 TiDB Cloud OpenAPI 创建 snapshot export 和 cloud-storage changefeed，让 TiDB Cloud 把 snapshot CSV 与 CDC CSV 写入 S3，然后本工具从 S3 加载到 Snowflake。

核心链路是：

1. `cmd/core.go:109` 创建或恢复 export/changefeed，并生成 `snapshot/`、`increment/` 两个存储前缀。
2. `replicate/snapshot.go:96` 读取 snapshot CSV，建 Snowflake 表，执行 `COPY INTO`，成功后写 `<db>.<table>.loadinfo`。
3. `replicate/increment.go:640` 周期扫描 CDC 文件，按 schema version 和文件序号执行 DDL / DML，成功后写 `.checkpoint` 和 `_consumer/progress.json`。

## P0 正确性问题

### 1. progress 与 checkpoint 不一致时可能跳过未应用 CDC 文件

位置：

- `replicate/increment.go:259`
- `replicate/increment.go:347`
- `replicate/increment.go:446`
- `docs/correctness-test-plan.md:731`

问题：

`loadProgress` 会把 `_consumer/progress.json` 里的位置同时写入 `progressDMLIdxMap` 和 `tableDMLIdxMap`。后续 `getNewFiles` 用 `tableDMLIdxMap` 作为扫描前游标来计算新增范围。如果 progress 指到 `CDC000005.csv`，但对应 `.checkpoint` 缺失，扫描后可能认为没有新文件需要处理，导致未确认应用的 CDC 文件被跳过。

影响：

progress 是加速恢复用的游标，不能单独作为“数据已进 Snowflake”的证据。当前行为违背文档里的正确性要求：checkpoint 应该强于 progress。

建议：

- 启动时先 reconcile progress 和 checkpoint。对 progress 覆盖到的每个文件，缺 checkpoint 时必须回放或明确失败。
- 保持 marker 顺序：`MERGE` 成功后写 `.checkpoint`，再推进 `_consumer/progress.json`。
- 加 package-level 回归测试：构造 progress ahead of checkpoint，断言文件被重放或启动失败。

### 2. 没有表入场校验，无主键表可能进入复制流程

位置：

- `README.md:106`
- `pkg/snowflake/sql.go:183`
- `docs/correctness-test-plan.md:153`

问题：

README 写明只支持有主键的表，但代码没有在启动前拒绝无主键表。增量 `MERGE` 依赖 `tableDef.Columns` 中的 `IsPK == "true"` 生成 `ON` 条件和 `QUALIFY partition by`。没有主键时，生成的 SQL 要么失败，要么行为不明确。

影响：

这是数据正确性问题，不应该等到 Snowflake SQL 执行阶段才失败。更危险的是，snapshot 可能已经建表或加载，留下部分副作用。

建议：

- 在创建 export/changefeed 前增加 admission check。
- 检查每张表是否存在稳定主键，类型是否支持，标识符是否落在首版支持范围内。
- 对不支持的表，在 Snowflake 建表、snapshot COPY、loadinfo、checkpoint、progress 之前失败。

### 3. 标识符转义缺失，简单标识符以外容易失败

位置：

- `pkg/snowflake/sql.go:19`
- `pkg/snowflake/sql.go:133`
- `pkg/snowflake/sql.go:172`
- `pkg/tidb/ddl.go:115`
- `pkg/tidb/ddl.go:198`

问题：

当前 SQL 生成大量直接拼接 table、column、stage 名。`utils.EscapeString` 处理的是字符串字面量，不是 SQL 标识符。混合大小写、保留字、空格、反引号、双引号、标点符号等名字都没有一致策略。

影响：

这会导致 SQL 语法错误、对象名大小写漂移、Snowflake 目标表冲突，极端情况下还会变成 SQL 注入面。

建议：

- 先定义首版支持的标识符子集。例如只支持 `[A-Za-z_][A-Za-z0-9_]*`。
- 不在子集内的表或列，在 admission 阶段失败。
- 如果要支持更广标识符，集中实现 TiDB identifier quoting、Snowflake identifier quoting 和 S3 pattern escaping，不要在各处拼字符串。

### 4. 快照恢复是表级 all-or-nothing

位置：

- `replicate/snapshot.go:96`
- `replicate/snapshot.go:120`
- `replicate/snapshot.go:187`
- `pkg/snowflake/sql.go:165`

问题：

快照加载先 `CREATE OR REPLACE TABLE`，再执行 `COPY INTO`，全部完成后才写表级 `loadinfo`。如果 COPY 已经写入一部分数据但进程崩溃，或 Snowflake 成功但写 `loadinfo` 失败，下一次运行会重建表再重灌。

影响：

大表失败恢复成本高。`CREATE OR REPLACE` 还会造成装载窗口内目标表为空或半成品，对下游查询不友好。

建议：

- 短期：利用 Snowflake load history 或 per-file checkpoint，避免成功文件重复加载。
- 中期：改成 staging table 加原子 swap，避免下游看到空表。
- 长期：把 snapshot dump/load 做成流水线，按文件落地后逐步 COPY。

## P1 测试和验证问题

### 5. P0 测试计划完整，但执行覆盖不足

位置：

- `docs/correctness-test-plan.md:145`
- `docs/correctness-test-plan-review.md:57`
- `test/e2e/e2e_test.go:110`
- `scripts/local_tiup_s3_snowflake_e2e.sh:303`

问题：

现有单元测试主要覆盖请求构造、类型映射、DDL diff、少量 helper。真实正确性要求里提到的 marker 一致性、restart idempotency、failure injection、unsupported admission、identifier escaping、类型值 round-trip 都还没有完整自动化覆盖。

影响：

当前测试通过只能说明基础构造和 happy path 有保障，不能证明工具满足文档里的复制正确性合同。

建议：

- 先为 `replicate` 增加 fake storage + fake connector 测试，覆盖 marker 顺序和恢复。
- 增加 no-PK、unsupported type、bad identifier 的负向测试。
- 增加 matrix value round-trip e2e，验证 `supported_matrix.sql` 里的真实值进入 Snowflake 后仍正确。
- e2e 断言必须包括 `loadinfo`、`.checkpoint`、`_consumer/progress.json`。

### 6. DDL matrix smoke 脚本生成的 Go probe 有别名错误

位置：

- `scripts/local_tiup_ddl_matrix_smoke.sh:65`
- `scripts/local_tiup_ddl_matrix_smoke.sh:115`
- `scripts/local_tiup_ddl_matrix_smoke.sh:186`

问题：

脚本生成的 Go 文件导入了 `github.com/tidbcloud/tidb2snowflake/pkg/snowflake`，但后面调用的是 `snowsql.GetSnowflakeTypeString` 和 `snowsql.GenDDLViaColumnsDiff`。没有导入别名时，`go run` 会编译失败。

影响：

文档把该脚本列为 entry gate，但脚本当前不能真正跑通到断言阶段。

建议：

- 把导入改成 `snowsql "github.com/tidbcloud/tidb2snowflake/pkg/snowflake"`，或把调用点改为 `snowflake.*`。
- 修完后把脚本纳入常规验证命令，至少在本地 release 前跑一次。

### 7. Cloud e2e 和 local e2e 只覆盖 happy path

位置：

- `test/e2e/e2e_test.go:96`
- `test/e2e/e2e_test.go:114`
- `test/e2e/e2e_test.go:148`
- `scripts/local_tiup_s3_snowflake_e2e.sh:326`

问题：

Cloud e2e 覆盖 snapshot-only、full、incremental-only 的基础流程。local e2e 覆盖 snapshot + insert/update/delete。但缺少 delete/reinsert、primary-key update 决策、marker 断言、重启复跑、数据复用不带 API credentials、state-ID 复用等场景。

影响：

这些测试无法证明重复运行不会重放、不会漏数据，也无法证明对象存储 marker 和 Snowflake 数据状态一致。

建议：

- 给每个 P0 scenario 建 case-to-runner 表，避免“计划里有”被误认为“测试已覆盖”。
- 把 local e2e 扩展成可跑矩阵：`snapshot.load-mode` x `snapshot.compression`。
- 为 restart idempotency 加同 prefix 复跑脚本。

## P2 性能、运维和可观测性问题

### 8. 增量扫描仍然按整张表子树 WalkDir

位置：

- `replicate/increment.go:347`
- `optimization.md:1`

现状：

当前代码已经用本轮扫描得到的 checkpoint set 去掉了一部分 HEAD 请求，这是正确方向。但每轮仍然 `WalkDir` 整个 `<db>/<table>` 子树，文件长期堆积后 LIST 成本和扫描延迟仍会增长。

建议：

- 下一步按日期目录扫描，只保留活跃日期。
- 再往后读取 TiCDC 写出的 `meta/CDC.index`，用持久化游标直接定位待处理文件。
- 配合 S3 lifecycle 清理已消费的旧日期目录。

### 9. 可观测性还停留在库级指标，没有服务暴露面

位置：

- `pkg/metrics/metrics.go:1`
- `cmd/snowflake.go:91`

问题：

项目定义了 Prometheus 指标，但 CLI 没有 HTTP metrics endpoint，也缺少关键运行指标，例如 CDC lag、last applied file、last checkpoint path、state reuse decision、load duration、scan duration。

影响：

生产运行时很难判断是 export 卡住、changefeed 没跑、S3 没文件、Snowflake COPY/MERGE 慢，还是 marker 写入失败。

建议：

- 增加可选 `--metrics.addr`。
- 增加 per-table 状态指标和结构化日志字段。
- 在等待 export/changefeed、snapshot COPY、increment MERGE、marker write 这些边界都打明确日志。

## 建议修复顺序

1. 修 `scripts/local_tiup_ddl_matrix_smoke.sh` 的导入别名，让现有 gate 可执行。
2. 增加 admission check：主键、支持类型、标识符子集，在任何 Snowflake 或 marker 副作用前失败。
3. 修 progress/checkpoint reconcile，并补 fake storage/fake connector 回归测试。
4. 补 marker 顺序测试：snapshot loadinfo、increment checkpoint、increment progress。
5. 改快照恢复策略，至少避免 `CREATE OR REPLACE` 导致的大表全量重灌。
6. 扩展 local/cloud e2e，覆盖重启、数据复用、marker 断言和 supported matrix value round-trip。
7. 做增量扫描优化和 metrics endpoint。

## 已验证事项

本次阅读后跑过：

```bash
GOCACHE=$PWD/.cache/go-build go test -ldflags=-checklinkname=0 ./...
bash -n scripts/local_tiup_ddl_matrix_smoke.sh
bash -n scripts/local_tiup_s3_snowflake_e2e.sh
```

结果：

- Go 单元测试通过。
- 两个 shell 脚本语法检查通过。
- 未运行真实 e2e，因为需要 TiDB Cloud、S3、Snowflake 凭据和外部资源。

