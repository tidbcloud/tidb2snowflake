# DDL and Type Matrix Smoke Test

`test/fixtures/supported_matrix.sql` is derived from
`tidb-fivetran-connector/e2e-test/test.sql`. It keeps the TiDB data types and
DML shapes currently supported by tidb2snowflake:

- integer and unsigned integer families
- decimal/numeric up to Snowflake precision 38
- float/double
- date/datetime/timestamp/time
- char/varchar/text families
- binary/varbinary/tinyblob/blob
- single-column and composite primary-key DML, including update/delete/reinsert

Run the local TiUP smoke test:

```bash
./scripts/local_tiup_ddl_matrix_smoke.sh
```

The script starts a local TiDB cluster with TiUP, loads the supported fixture,
then uses the project code to verify:

- TiDB metadata for every supported type maps to a Snowflake type.
- Unsigned integer metadata is preserved.
- Supported DDL generation covers add column, drop column, rename column,
  widening modify, nullability changes, dropping defaults, truncate table,
  drop table, rename table, and drop schema.

`test/fixtures/unsupported_or_skipped.sql` records upstream connector cases that
are intentionally not executed yet by tidb2snowflake's local smoke test. Some of
these are supported by the Fivetran connector but do not have a tidb2snowflake
Snowflake type policy yet, such as JSON, SET, YEAR, BIT(1), VECTOR, and larger
blob families. Others are replication-semantics issues that need an explicit
table-admission/versioning design before they are safe to run, such as
running-task CREATE TABLE/CREATE SCHEMA, drop/recreate with a changed shape,
primary-key changes, and tables without a stable primary key. `RENAME TABLE`
is supported as a DDL replay operation, but an exact `--table db.old` task does
not switch to consuming future DML from `db.new` after the rename.
