# DDL and Type Matrix Smoke Test

`test/fixtures/supported_matrix.sql` is derived from
`tidb-fivetran-connector/e2e-test/test.sql`. It keeps the TiDB data types and
DML shapes currently supported by tidb2snowflake:

- integer and unsigned integer families
- decimal/numeric up to Snowflake precision 38
- float/double
- date/datetime/timestamp/time/year
- char/varchar/text/enum/vector families
- binary/varbinary/tinyblob/blob
- single-column and composite primary-key DML, including update/delete/reinsert

Run the local TiUP smoke test:

```bash
./scripts/local_tiup_ddl_matrix_smoke.sh
```

The script starts a local TiDB cluster with TiUP, loads the supported fixture,
then uses the project code to verify:

- TiDB metadata for every fixture type maps to a Snowflake type.
- Unsigned integer metadata is preserved.
- Supported DDL generation covers add column, drop column, rename column,
  widening modify, nullability changes, dropping defaults, truncate table,
  drop table, rename table, and drop schema.

`test/fixtures/unsupported_or_skipped.sql` records upstream connector cases that
are intentionally not executed yet by tidb2snowflake's local smoke test. Some
type mappings now exist but are still outside this smoke fixture, such as JSON,
SET, BIT(n), MEDIUMBLOB/LONGBLOB, and unsigned decimal values above Snowflake
precision 38. Others are replication-semantics issues that need an explicit
table-admission/versioning design before they are safe to run, such as
running-task CREATE TABLE/CREATE SCHEMA, drop/recreate with a changed shape,
primary-key changes, and tables without a stable primary key. `RENAME TABLE`
is supported as a DDL replay operation, but an exact `--table db.old` task does
not switch to consuming future DML from `db.new` after the rename.
