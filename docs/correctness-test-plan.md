# Correctness Test Plan

This document is a tool-level correctness test plan for tidb2snowflake. The goal
is to prove that the whole tool moves TiDB data into Snowflake correctly across
snapshot, incremental CDC replay, DDL handling, restarts, and failure recovery.

This phase is correctness-only. Do not use this plan to evaluate throughput,
latency, S3 request cost, Snowflake warehouse sizing, or long-running soak
stability.

## Correctness Contract

For every supported table, after tidb2snowflake reaches a stable point:

- every committed TiDB row at or before the verified CDC checkpoint exists in
  Snowflake exactly once
- every committed TiDB update is reflected in Snowflake
- every committed TiDB delete is reflected in Snowflake
- unsupported table/type/DDL cases fail clearly or remain documented skips
- rerunning the tool with the same storage prefix does not duplicate or lose data
- object-storage markers (`tidb2snowflake.state.json`, `loadinfo`, `.checkpoint`,
  `_consumer/progress.json`) agree with data that has actually reached Snowflake

## Non-Goals

- COPY or MERGE performance benchmarking
- warehouse size, clustering, or Snowflake cost optimization
- S3 LIST/HEAD cost measurement
- high-concurrency stress testing
- implementing unsupported TiDB types or DDLs
- multi-day soak testing

## Test Environments

### Local TiUP Path

Use for repeatable correctness testing without TiDB Cloud:

- TiUP playground with TiDB, PD, TiKV, and TiCDC
- Dumpling writes snapshot CSV to S3
- TiCDC writes incremental CSV to S3
- tidb2snowflake loads S3 files into a real Snowflake schema

Runner:

```bash
./scripts/local_tiup_s3_snowflake_e2e.sh
```

### TiDB Cloud OpenAPI Path

Use for validating the production control path:

- TiDB Cloud export creation or reuse
- TiDB Cloud changefeed creation or reuse
- OpenAPI authentication and cluster/changefeed state polling
- same Snowflake load and merge logic as the local path

Use this path at least once before release because local TiUP does not validate
OpenAPI request shape, TiDB Cloud export metadata, or managed changefeed states.

## Entry Gates

Run before deeper validation:

```bash
go test -ldflags '-checklinkname=0' ./...
./scripts/local_tiup_ddl_matrix_smoke.sh
```

Pass criteria:

- all Go tests pass
- the local DDL/type matrix smoke test passes
- tests do not depend on stale generated files
- unrelated local files are not included in the result

## Validation Method

For each end-to-end case, verify both data and control markers.

### CDC Verification Cutoff

Incremental tests must define a stable verification cutoff before comparing TiDB
and Snowflake. Do not compare Snowflake against a TiDB table that is still
accepting writes.

Required procedure for incremental, DDL, restart, and failure-injection cases:

- quiesce writes before verification
- record the expected final state produced by the test workload, or record a
  TiDB snapshot point that corresponds to the CDC files being verified
- record the last applied CDC file per table from `_consumer/progress.json`
- record the matching `.checkpoint` file for each applied CDC file
- when TiCDC metadata exposes a checkpoint-ts or commit-ts for the verified
  files, record it in the test output
- compare Snowflake against the explicit expected state, or against TiDB after
  writes are quiesced and all CDC files up to the recorded cutoff are applied

Pass criteria:

- every data diff names the cutoff it verified
- no test passes only because TiDB and Snowflake happened to match at an
  unspecified time
- files beyond the recorded cutoff are not required for the assertion

Data verification:

- compare TiDB source rows and Snowflake target rows by primary key
- compare row count
- compare checksum-style aggregates for supported deterministic columns
- verify deleted rows are absent
- verify updated rows contain the latest values
- verify nulls, empty strings, binary values, decimals, and timestamps preserve
  expected semantics

Marker verification:

- `tidb2snowflake.state.json` contains expected export and changefeed IDs when
  those resources are created or reused
- snapshot `loadinfo` exists only after successful snapshot load
- each applied CDC file has a matching `.checkpoint`
- `_consumer/progress.json` points at files that are actually applied
- rerun logs show snapshot/changefeed reuse when expected

Execution mapping:

- local happy-path coverage: `./scripts/local_tiup_s3_snowflake_e2e.sh`
- type/DDL smoke coverage: `./scripts/local_tiup_ddl_matrix_smoke.sh`
- package-level regression coverage: `go test -ldflags '-checklinkname=0' ./...`
- Cloud happy-path coverage: `go test -tags e2e ./test/e2e`
- failure-injection coverage: requires the hooks defined in this plan before it
  can be a release gate

This mapping describes current entry points, not full satisfaction of the
correctness contract. A case is not considered covered until its runner asserts
the data oracle, CDC cutoff, and marker state required by this plan.

## Current Coverage and Required Additions

As a test plan, this document defines the required product behavior even when
the current implementation or existing runners do not satisfy it yet.

| Area | Current runner | Current coverage | Required addition |
|---|---|---|---|
| Unit/API request construction | `go test -ldflags '-checklinkname=0' ./...` | request fields and helper logic | keep as entry gate |
| Local happy path | `./scripts/local_tiup_s3_snowflake_e2e.sh` | snapshot plus insert/update/delete final state | add CDC cutoff capture, marker assertions, delete/reinsert, and primary-key update decision |
| Cloud happy path | `go test -tags e2e ./test/e2e` | snapshot-only, full, incremental-only happy paths | add marker assertions, delete/reinsert, and explicit cutoff reporting |
| DDL/type metadata smoke | `./scripts/local_tiup_ddl_matrix_smoke.sh` | TiDB metadata mapping and generated DDL SQL | keep as fast gate only; it does not prove Snowflake value round-trip |
| DDL/type value round-trip | not yet implemented | not covered | add a Snowflake e2e runner for `supported_matrix.sql` values and DML |
| Restart idempotency | partial/manual | not a release-quality gate | add scripted rerun with same storage prefix and marker assertions |
| Failure injection | not yet implemented | design-required only | add hooks or fake connector/storage tests before making it a release gate |
| Progress without checkpoint | not yet fixed | expected failure / known P0 risk | add regression test and fix loader reconciliation |
| Unsupported type failures | type mapper errors partially covered | documented/partially covered | add negative tests that assert error context and no Snowflake/marker side effects |
| Unsupported table admission | not yet implemented | expected failure / known P0 risk | add pre-snapshot admission checks for no-PK tables and PK changes |
| Identifier escaping | not yet implemented | not covered / likely failure outside simple identifiers | define first-release identifier subset and add escaping/collision tests |

P0 case-to-runner matrix:

| P0 scenario | Required runner | Status |
|---|---|---|
| snapshot-only baseline | Cloud e2e and local e2e mode | partially covered; marker assertions missing |
| full snapshot plus incremental DML | Cloud e2e and local e2e mode | partially covered; delete/reinsert and marker assertions missing |
| local mode/compression matrix | local e2e script | command path exists; each mode needs final-state and marker assertions |
| restart idempotency | local e2e rerun harness | not fully covered |
| existing data reuse without API credentials | local/object-storage reuse harness | not fully covered |
| stored state-ID reuse with API credentials | Cloud e2e | not fully covered |
| supported type value round-trip | new matrix Snowflake e2e | not covered |
| supported DDL final-state correctness | new matrix Snowflake e2e | not covered |
| unsupported type cases fail clearly | unit tests and negative e2e | documented only; type mapper errors partially covered |
| unsupported table-admission cases fail before side effects | local negative e2e and unit tests | not covered; no-PK admission is a known P0 risk |
| identifier escaping and supported identifier subset | unit tests and e2e | not covered |

## P0 End-to-End Cases

### Snapshot-Only Baseline

Seed a table before replication starts and run snapshot-only mode.

Data:

- 3 to 10 rows
- simple primary key
- varchar, bigint, decimal, timestamp, nullable column, empty string

Expected:

- Snowflake table is created with correct columns and primary key metadata where
  supported
- all snapshot rows are loaded exactly once
- null and empty string values remain distinguishable
- no incremental files are required

### Full Snapshot Plus Incremental DML

Seed rows, start snapshot and changefeed, then apply DML after CDC starts:

- insert a new row
- update an existing row
- delete an existing row
- delete and reinsert the same primary key
- primary-key update decision:
  - if tidb2snowflake supports primary-key updates, update a primary key and
    verify the old key disappears and the new key exists
  - if primary-key updates are unsupported, assert the tool fails clearly or the
    test marks this as an explicit unsupported case

Expected:

- final Snowflake table equals TiDB by primary key
- insert/update/delete/reinsert final state is correct
- primary-key update behavior is either correctly replicated or explicitly
  rejected; it must not silently create duplicate logical rows
- no duplicate row exists for any primary key
- all applied CDC files have checkpoints and progress

### Local Mode and Compression Matrix

Run the local e2e script for all combinations:

```bash
SNAPSHOT_LOAD_MODE=bulk SNAPSHOT_COMPRESSION=none \
  ./scripts/local_tiup_s3_snowflake_e2e.sh

SNAPSHOT_LOAD_MODE=per-file SNAPSHOT_COMPRESSION=none \
  ./scripts/local_tiup_s3_snowflake_e2e.sh

SNAPSHOT_LOAD_MODE=bulk SNAPSHOT_COMPRESSION=gzip \
  ./scripts/local_tiup_s3_snowflake_e2e.sh

SNAPSHOT_LOAD_MODE=per-file SNAPSHOT_COMPRESSION=gzip \
  ./scripts/local_tiup_s3_snowflake_e2e.sh
```

Expected:

- all four runs produce identical final Snowflake data
- gzip snapshot files are read correctly
- bulk mode uses one table-wide load pattern
- per-file mode loads every data file and no metadata files

### Restart Idempotency

Run the same storage prefix twice without deleting S3 or Snowflake state.

Expected:

- second run skips completed snapshot load through `loadinfo`
- second run resumes incremental replay through progress/checkpoints
- Snowflake data remains identical before and after rerun
- row count does not increase unless new TiDB changes were committed

### Existing Data Reuse Without API Credentials

Prepare object storage with existing snapshot and incremental data, but no
stored export or changefeed ID that must be waited on.

Expected:

- the tool can load without creating a new export/changefeed when data already
  exists
- TiDB Cloud API credentials and `--tidbcloud.cluster-id` are not required for
  this data-only reuse path
- the run does not call TiDB Cloud OpenAPI

### Stored State-ID Reuse With API Credentials

Reuse a previous `tidb2snowflake.state.json` that contains an export ID or
changefeed ID.

Expected:

- the tool waits for or reuses the stored export/changefeed instead of creating a
  duplicate
- TiDB Cloud API credentials and `--tidbcloud.cluster-id` are required because
  the stored job IDs must be checked through OpenAPI
- if the stored job is terminal-failed or missing, the command fails clearly and
  does not mark load success

## P0 Data Type Cases

Use a matrix table with a stable primary key.

Authoritative fixture source:

- supported and skipped matrix categories are maintained in
  `docs/ddl-matrix.md`
- executable supported cases live in
  `test/fixtures/supported_matrix.sql`
- intentionally skipped examples live in
  `test/fixtures/unsupported_or_skipped.sql`
- the local runner is `./scripts/local_tiup_ddl_matrix_smoke.sh`

Split this area into two gates:

- metadata/DDL smoke gate: `./scripts/local_tiup_ddl_matrix_smoke.sh`
  verifies TiDB metadata extraction, type mapping, unsigned detection, and DDL
  generation; it is a fast entry gate
- Snowflake value round-trip gate: a required e2e runner must dump/load
  `supported_type_matrix` and `supported_composite_pk_dml` into Snowflake,
  replay their DML, and execute the normalization checks in this plan; this is
  the P0 correctness gate for values

Passing the metadata/DDL smoke gate alone does not prove that supported values
round-trip correctly through Dumpling/TiCDC/S3/COPY/MERGE into Snowflake.

Supported types to verify:

- `BOOLEAN`
- signed integer family: `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `BIGINT`
- unsigned integer family, especially max values
- `DECIMAL` and `NUMERIC` up to Snowflake precision 38
- `FLOAT` and `DOUBLE`
- `DATE`, `DATETIME`, `TIMESTAMP`, `TIME`, `YEAR`
- `CHAR`, `VARCHAR`, `TINYTEXT`, `TEXT`, `MEDIUMTEXT`, `LONGTEXT`
- `ENUM` as the selected label text
- `VECTOR` as the textual vector representation
- `BINARY`, `VARBINARY`, `TINYBLOB`, `BLOB`
- nullable and non-null columns
- default values that are represented in TiDB metadata

Values to include:

- minimum, maximum, zero, negative, and null numeric values
- `BIGINT UNSIGNED` max value
- decimal scale boundary values
- empty string and non-empty string
- multi-byte UTF-8 text
- binary values containing `00`, `FF`, quote, comma, newline, and backslash bytes
- timestamp values with fractional seconds
- date/time edge values accepted by TiDB and Snowflake

Expected:

- Snowflake columns have compatible types
- values round-trip into Snowflake according to the documented mapping
- TiDB unsigned metadata is preserved before Snowflake DDL generation
- unsupported precision/type cases fail before corrupting data

Current status:

- metadata/DDL smoke is covered by the existing local runner
- Snowflake value round-trip for the full supported matrix is not yet covered
  and must be added before this P0 area is considered complete

## P0 DDL Cases

Run DDL after the initial schema is established and verify subsequent DML lands
in the correct Snowflake shape.

Use `docs/ddl-matrix.md` as the authoritative DDL/type matrix reference and keep
this section aligned with that fixture set.

### Schema Evolution DDL

- add nullable column
- add non-null column with default
- drop column
- rename column
- widen integer type
- widen varchar length
- widen decimal precision or scale within Snowflake limits
- set column not null
- drop not null
- drop default

For each schema-evolution DDL:

- apply DML before the DDL
- apply DML after the DDL
- verify old rows and new rows in Snowflake match the expected final schema
- verify DDL-generated Snowflake SQL is deterministic and safe to rerun where
  applicable

Expected:

- DDL is applied before later DML for that table version
- column ID mapping keeps data aligned across rename/modify operations
- dropped columns are not populated by later DML

### Terminal or Destructive DDL

These DDLs need separate final-state semantics because the target object or old
rows may intentionally disappear.

Cases:

- truncate table
- drop table
- drop schema

Truncate expected state:

- all pre-truncate rows are absent
- post-truncate rows are present after later DML
- `_consumer/progress.json` and `.checkpoint` identify the CDC file containing
  the truncate and all later applied DML files

Drop table expected state:

- the Snowflake table no longer exists, or the tool reaches a documented terminal
  state that prevents later loads into that table
- no later DML for the dropped table is silently merged into a stale table
- rerun preserves the same terminal state

Drop schema expected state:

- the Snowflake schema no longer exists, or the tool reaches a documented
  terminal state that prevents later loads into that schema
- unrelated schemas are not affected
- rerun preserves the same terminal state

## P0 Unsupported Case Handling

These cases must not silently produce incorrect Snowflake data.

Split unsupported behavior into type-mapping failures and table-admission
failures. They have different side-effect risks.

Unsupported types:

- `JSON`
- `SET`
- `BIT` and `BIT(n)`
- `MEDIUMBLOB`
- `LONGBLOB`
- `DECIMAL` precision greater than 38

Type-mapping expected state:

- the tool returns a clear error before writing incorrect values
- error context identifies the table, column, and TiDB type
- if Snowflake objects were created before type discovery, the test must assert
  the object is empty or cleaned up according to the documented behavior
- no `loadinfo`, `.checkpoint`, or `_consumer/progress.json` marker claims the
  unsupported data was applied

Unsupported table-admission semantics:

- table without a stable primary key
- primary-key add/drop/change after table admission

Table-admission expected state:

- the tool rejects the table before Snowflake table creation, snapshot `COPY`,
  `loadinfo`, CDC progress, or CDC checkpoints
- no partial target table is left behind unless the behavior is explicitly
  documented and the table is empty
- error context identifies the table and the admission rule that failed

Unsupported DDL/task semantics:

- `CREATE TABLE` during a running task
- `CREATE SCHEMA` during a running task
- `RENAME TABLE`
- drop and recreate the same table name with a changed shape

DDL/task expected state:

- the tool returns a clear error or documented skip
- no partial incorrect Snowflake data is committed for the unsupported table
- error messages include enough context to identify table, column, or DDL

Required negative gates:

- no-PK table admission: create a source table without primary key and assert the
  tool fails before Snowflake table creation, snapshot load, `loadinfo`,
  `_consumer/progress.json`, or `.checkpoint`
- PK change after admission: add/drop/change the primary key and assert the tool
  fails before applying later DML with ambiguous merge semantics
- unsupported type table: include one unsupported column and assert error context
  plus absence of success markers
- running-task `CREATE TABLE` and `CREATE SCHEMA`: assert explicit rejection or
  documented skip without silent data loss
- `RENAME TABLE`: assert explicit rejection and no later DML is merged into the
  old table identity
- drop/recreate changed shape: assert explicit rejection or versioned-table
  behavior before any mixed-shape data is merged

Current status:

- unsupported cases are documented in `test/fixtures/unsupported_or_skipped.sql`
- type mapper errors are partially covered by unit behavior
- executable negative e2e coverage is not yet present
- no-PK admission is an expected-failing P0 risk until pre-snapshot admission
  validation exists

## P1 Multi-Table Cases

### Independent Tables

Replicate two or more tables in one run.

Expected:

- each table has independent snapshot load state
- each table has independent incremental progress
- DML for one table does not affect another table
- rerun can skip one completed table while continuing another incomplete table

### Composite Primary Key

Use a table with a composite primary key.

Cases:

- insert rows with same first key part and different second key part
- update non-key columns
- delete one row from a shared key prefix
- reinsert the deleted key

Expected:

- Snowflake merge matches using the full composite key
- no row is collapsed using only a partial key

### Table and Column Name Escaping

Use identifiers with mixed case, underscores, digits, and characters that affect
SQL or regex generation where supported by TiDB and the tool.

First-release decision:

- define the supported identifier subset before release
- if the first release supports only simple unquoted identifiers, the tool must
  reject unsupported names before Snowflake table creation or data load
- if broader identifiers are supported, all generated Snowflake SQL and S3
  patterns must quote/escape consistently

Expected:

- Snowflake SQL escapes identifiers correctly
- S3 pattern generation matches only the intended files
- table names do not collide after normalization

Cases:

- mixed-case table and column names
- reserved words such as `order`, `group`, and `select`
- names containing underscores and digits
- names requiring Snowflake quotes, such as spaces, punctuation, or symbols
- names that differ only by case or normalize to the same target spelling

Current status:

- not covered by existing runners
- expected to fail or be unsupported outside the simple identifier subset until
  identifier policy and escaping tests are implemented

## P1 Snapshot Edge Cases

### Empty Table Snapshot

Replicate an empty table.

Expected:

- Snowflake table is created
- row count is zero
- loadinfo behavior is well-defined and does not require data files to exist

### Multiple Snapshot Files

Force Dumpling to produce multiple files.

Expected:

- bulk mode loads all files once
- per-file mode loads all files once
- final row count equals TiDB row count

### Metadata and Extra Files

Place non-data files under the snapshot prefix.

Expected:

- metadata, loadinfo, logs, and unrelated files are not loaded
- the run either ignores them or reports a clear error if they match an unsafe
  pattern

## P1 Incremental Edge Cases

### Multiple CDC Files

Force multiple CDC files for the same table/date.

Expected:

- files are applied in file-index order
- progress advances monotonically
- missing lower-index files are not silently skipped

### Multiple Dates

Generate CDC files across date directories.

Expected:

- all dates are scanned and applied
- per-date progress is independent
- later dates do not cause earlier unfinished files to be skipped

### DDL Before DML in Same Table Version

Ensure a schema file and DML files exist for the same table version.

Expected:

- schema/DDL is applied before DML for that table version
- DML is decoded with the correct schema

### Duplicate or Replayed CDC File

Run with a CDC file that has already been checkpointed.

Expected:

- file is skipped
- progress is preserved or backfilled
- Snowflake data is unchanged

## Failure Injection Cases

These tests intentionally interrupt execution at correctness-sensitive points.
They are release gates only after deterministic hooks exist.

Required mechanics:

- add env-controlled crash hooks or Go failpoints at the exact points listed
  below
- each hook should exit non-zero after the preceding side effect has completed
  and before the following marker write begins
- each hook should log the hook name, table, file path, and storage prefix
- alternatively, package-level tests may use fake Snowflake and fake object
  storage connectors to assert the same ordering without process crashes

Suggested hook names:

- `T2SF_FAIL_AFTER_SNAPSHOT_COPY_BEFORE_LOADINFO`
- `T2SF_FAIL_AFTER_N_SNAPSHOT_FILES`
- `T2SF_FAIL_AFTER_INCREMENT_MERGE_BEFORE_CHECKPOINT`
- `T2SF_FAIL_AFTER_INCREMENT_CHECKPOINT_BEFORE_PROGRESS`
- `T2SF_FAIL_ON_SNOWFLAKE_COPY`
- `T2SF_FAIL_ON_SNOWFLAKE_MERGE`
- `T2SF_FAIL_ON_MARKER_WRITE`

For every failure case, record:

- trigger or fake used
- expected process exit
- marker files present immediately after the failure
- rerun command
- final Snowflake-vs-expected-state assertion at a named CDC cutoff

### Snapshot COPY Succeeds, Loadinfo Missing

Stop after Snowflake COPY succeeds but before loadinfo is written.

Trigger:

- run with `T2SF_FAIL_AFTER_SNAPSHOT_COPY_BEFORE_LOADINFO=1`, or use a fake
  connector that makes `LoadSnapshot` succeed and fake storage that blocks
  `loadinfo` creation

Expected:

- command exits non-zero
- snapshot data may already be present in Snowflake
- `db.table.loadinfo` is absent after the failed run
- rerun reaches a correct final table state
- no duplicate snapshot rows are present
- loadinfo is eventually written

This is a high-risk case because the current marker is table-level.

### Snapshot Partial Per-File Load

Stop after only part of the snapshot files are loaded in per-file mode.

Trigger:

- run per-file mode with `T2SF_FAIL_AFTER_N_SNAPSHOT_FILES=1` for a table that
  has at least two snapshot data files

Expected:

- command exits non-zero after exactly one file is loaded
- table-level `loadinfo` is absent
- rerun reaches a correct final table state
- already-loaded files are not duplicated

If duplicate rows are possible, record a P0 correctness bug.

### Increment MERGE Succeeds, Checkpoint Missing

Stop after Snowflake `MERGE` succeeds but before checkpoint write.

Trigger:

- run with `T2SF_FAIL_AFTER_INCREMENT_MERGE_BEFORE_CHECKPOINT=1`, or use a fake
  connector that records `LoadIncrement` success and fake storage that prevents
  checkpoint creation

Expected:

- command exits non-zero
- the target CDC file has no `.checkpoint`
- `_consumer/progress.json` does not advance past that file
- rerun may merge the same file again
- final Snowflake state remains correct because merge is primary-key based
- checkpoint and progress are eventually written

### Increment Checkpoint Exists, Progress Missing

Stop after checkpoint write but before progress write.

Trigger:

- run with `T2SF_FAIL_AFTER_INCREMENT_CHECKPOINT_BEFORE_PROGRESS=1`

Expected:

- command exits non-zero
- the target CDC file has a `.checkpoint`
- `_consumer/progress.json` is absent or still points before that file
- rerun skips the checkpointed file
- progress is backfilled
- final Snowflake data remains correct

### Increment Progress Exists, Checkpoint Missing

Simulate progress for a file without a matching checkpoint.

Trigger:

- manually write `_consumer/progress.json` past a CDC file while deleting that
  file's `.checkpoint`, or use fake storage to expose that marker state

Expected:

- implementation must not skip a never-applied file based on progress alone
- checkpoint should be treated as stronger applied-state evidence than progress
- if progress says a file was applied but checkpoint is missing, the loader must
  either replay that file or fail clearly before advancing

Current status:

- this is an expected-failing P0 correctness risk until startup progress is
  reconciled against checkpoint files
- add a package-level regression test that creates `_consumer/progress.json`
  ahead of a missing checkpoint and asserts replay or clear failure
- do not waive this behavior because of current implementation details; the
  product correctness rule is that progress alone is not sufficient evidence of
  applied data

### Snowflake Error During Load

Inject a Snowflake COPY or MERGE error.

Trigger:

- run with `T2SF_FAIL_ON_SNOWFLAKE_COPY=1` for snapshot failure
- run with `T2SF_FAIL_ON_SNOWFLAKE_MERGE=1` for incremental failure
- package-level tests may use a fake connector whose `LoadSnapshot` or
  `LoadIncrement` returns an error

Expected:

- no success marker is written for the failed file or snapshot
- rerun can retry the failed work
- error message includes table and file path

### Object Storage Error During Marker Write

Make marker write fail after data load succeeds.

Trigger:

- run with `T2SF_FAIL_ON_MARKER_WRITE=loadinfo|checkpoint|progress`, or use fake
  storage whose `WriteFile` fails for the selected marker path

Expected:

- command returns an error
- rerun reaches correct final data
- duplicate risk is explicitly assessed for snapshot load

## TiDB Cloud OpenAPI Cases

### Create Export Request

Expected request properties:

- target S3 URI points to the snapshot prefix
- CSV dialect is Snowflake-compatible
- backslash escaping is disabled as expected
- snapshot TSO is included when configured
- compression matches `--snapshot.compression`

### Create Changefeed Request

Expected request properties:

- target S3 URI points to the incremental prefix
- protocol is CSV
- date separator and file sizing match config
- start position uses snapshot TSO when available
- column IDs are emitted so DDL rename/modify can be handled correctly

### API Failure Handling

Cases:

- authentication failure
- cluster not found
- export create failure
- export terminal failure state
- changefeed create failure
- changefeed terminal failure state
- polling timeout or context cancellation

Expected:

- command fails with clear error
- no local marker claims success
- rerun can continue after the external issue is fixed

## Verification Queries

Use exact expected-row assertions for small P0 cases. Use generated diff queries
only for larger matrix cases where listing every row is impractical.

TiDB row count:

```sql
SELECT COUNT(*) FROM db.table;
```

Snowflake row count:

```sql
SELECT COUNT(*) FROM schema.table;
```

Small-case exact assertion:

```sql
-- Snowflake. Replace table and columns for the case.
SELECT id, name, amount, note
FROM schema.table
ORDER BY id;
```

Expected rows must be checked in the test code, not only inspected manually.

Primary-key diff for larger cases:

```sql
-- Export TiDB expected rows at the named cutoff into a temp/staging table, then
-- compare normalized values in Snowflake.
WITH expected AS (
  SELECT id, col_a, col_b, col_c FROM expected_state
),
actual AS (
  SELECT id, col_a, col_b, col_c FROM schema.table
)
SELECT 'missing_or_different' AS diff_type, expected.*
FROM expected
LEFT JOIN actual USING (id)
WHERE actual.id IS NULL
   OR expected.col_a IS DISTINCT FROM actual.col_a
   OR expected.col_b IS DISTINCT FROM actual.col_b
   OR expected.col_c IS DISTINCT FROM actual.col_c
UNION ALL
SELECT 'extra' AS diff_type, actual.*
FROM actual
LEFT JOIN expected USING (id)
WHERE expected.id IS NULL;
```

The diff query must return zero rows.

Type normalization rules:

- NULL: use null-safe comparison, never string equality against `"NULL"`
- empty string: assert separately from NULL
- decimal/numeric: compare as fixed-scale decimal strings or exact numeric
  values within Snowflake precision 38
- float/double: compare with an explicit tolerance, such as absolute difference
  <= `1e-9` for ordinary values, and avoid using approximate columns in
  checksum-only assertions
- binary: compare canonical hex strings; TiDB `HEX(col)` should match
  Snowflake `HEX_ENCODE(col)` or an equivalent expression
- date/datetime/timestamp: normalize to UTC or document the session timezone
  used by both systems before comparison
- time: compare formatted strings with the expected fractional-second precision
- text: compare exact UTF-8 string values, including comma, quote, newline,
  backslash, and multi-byte characters

Required value checks:

- check every expected primary key in small cases
- for matrix cases, check every boundary column listed in the data-type matrix
- for delete cases, assert deleted primary keys are absent
- for truncate cases, assert pre-truncate primary keys are absent and
  post-truncate keys are present
- for unsupported cases, assert no target data was silently written for the
  unsupported table

## Exit Criteria

### Current Executable Gates

These gates can be required with the current runner shape, after the missing
assertions called out in this document are added to those runners:

- entry gates pass
- P0 end-to-end cases pass in local TiUP mode
- at least one TiDB Cloud OpenAPI run passes
- all four snapshot mode/compression combinations pass
- supported type/DDL metadata smoke passes
- supported type value round-trip e2e passes after the required matrix e2e runner
  is added
- supported DDL final-state e2e passes after the required matrix e2e runner is
  added
- unsupported cases fail clearly or remain documented skips
- restart/rerun idempotency preserves final Snowflake correctness
- markers match actual applied data
- progress-without-checkpoint is either fixed and covered by regression test, or
  remains a blocking P0 expected failure
- no case requires manual data repair in Snowflake or object storage

### Gates After Failure Hooks Exist

Until deterministic crash hooks or equivalent fake connector/storage tests exist,
failure-injection cases are design-required but not executable release gates.

After those hooks exist, correctness validation also requires:

- snapshot COPY success followed by missing loadinfo reruns to a correct final
  state
- partial per-file snapshot load reruns without duplicate rows
- increment MERGE success followed by missing checkpoint reruns correctly
- checkpoint-without-progress backfills progress and does not remerge
- Snowflake COPY/MERGE failures do not write success markers
- marker-write failures return errors and rerun to the correct final state
