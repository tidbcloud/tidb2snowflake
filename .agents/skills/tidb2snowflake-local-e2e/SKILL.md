---
name: tidb2snowflake-local-e2e
description: "Run and debug the tidb2snowflake local end-to-end path that avoids TiDB Cloud: start a local TiDB cluster with TiUP playground, seed TiDB data, dump snapshot CSV to S3 with Dumpling, stream incremental CSV to S3 with TiCDC, then run tidb2snowflake to load and merge into Snowflake. Use when asked to test tidb2snowflake locally, reproduce the dumping + CDC to S3 flow, verify Snowflake loading, or explain/run the local_tiup_s3_snowflake_e2e.sh workflow."
---

# tidb2snowflake Local E2E

## Core Rule

Do not use TiDB Cloud for this workflow. The local path is:

```text
TiUP playground TiDB -> Dumpling snapshot CSV -> S3 snapshot/
TiUP playground TiCDC -> S3 increment/
S3 snapshot/ + increment/ -> tidb2snowflake -> Snowflake
```

`tidb2snowflake` skips TiDB Cloud OpenAPI when the S3 root already contains objects under both `snapshot/` and `increment/`.

## Quick Start

Run from the nested repo:

```bash
cd /Users/jiangjianyuan/work/git/pingcap/tidb2snowflake
./scripts/local_tiup_s3_snowflake_e2e.sh
```

The script reads `tidb2snowflake/.env` for S3 and Snowflake credentials. It creates a unique S3 prefix and Snowflake schema per run, builds `bin/tidb2snowflake`, starts `tiup playground v8.5.6 --ticdc 1`, seeds one table, dumps snapshot data, creates a TiCDC cloud-storage changefeed, applies DMLs, starts the loader, and polls Snowflake for the final state.

Useful overrides:

```bash
RUN_ID=manual1 \
TIDB_VERSION=v8.5.6 \
PORT_OFFSET=22000 \
SNOWFLAKE_DATABASE=TIDB2SNOWFLAKE_E2E \
SNOWFLAKE_SCHEMA=LOCAL_MANUAL1 \
./scripts/local_tiup_s3_snowflake_e2e.sh
```

Use `SNOWFLAKE_ACCOUNT_ID`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASS`, `SNOWFLAKE_WAREHOUSE`, `STORAGE_URI`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_SESSION_TOKEN` to override `.env`.

## Expected Evidence

A successful S3 preparation has objects like:

```text
s3://.../local-<run>/snapshot/metadata
s3://.../local-<run>/snapshot/<db>.<table>.000000000.csv
s3://.../local-<run>/increment/metadata
s3://.../local-<run>/increment/<db>/<table>/<table-version>/meta/schema_<version>_<hash>.json
s3://.../local-<run>/increment/<db>/<table>/<table-version>/<yyyy-mm-dd>/CDC00000000000000000001.csv
```

The loader log should include:

```text
snapshot data already exists in storage, skipping export creation
increment data already exists in storage, skipping changefeed creation
```

Those two lines prove the run is not using TiDB Cloud OpenAPI.

## Debugging

Logs are written to `tidb2snowflake/.local-e2e/<run-id>/`:

```text
playground.log
dumpling.log
changefeed.toml
changefeed-create.log
tidb2snowflake.log
snowflake_probe.go
```

For TiCDC internals, inspect:

```bash
~/.tiup/data/tidb2snowflake-<run-id>/cdc-0/ticdc.log
```

Common fixes:

- If `changefeed-id` is invalid, use only letters, digits, and hyphens.
- If TiCDC cannot access S3, ensure AWS env vars are exported before starting playground; the CDC server, not only `cdc cli`, needs credentials.
- If `aws s3 ls` finds no increment files, check `changefeed-create.log`, then query the changefeed with `tiup cdc cli changefeed query --server http://127.0.0.1:<cdc-port> -c <id>`.
- If Go fails with `invalid reference to runtime.buildVersion`, build/run Go helpers with `-ldflags=-checklinkname=0`.
- If Snowflake returns `390100 Incorrect username or password`, the S3 and local TiDB/TiCDC path can still be valid; replace or override Snowflake credentials before retesting the final COPY/MERGE phase.

## Manual Commands

Use the script by default. If debugging manually, mirror its important details:

```bash
tiup playground v8.5.6 --tag tidb2snowflake-<run> --db 1 --pd 1 --kv 1 --ticdc 1 --tiflash 0 --without-monitor --port-offset 21000
```

Dump snapshot:

```bash
tiup dumpling -h 127.0.0.1 -P 25000 -u root \
  --filetype csv --no-header --no-schemas \
  --csv-output-dialect snowflake --escape-backslash=false \
  --tables-list tidb2snowflake_local.t_<run> \
  --output "s3://<bucket>/<prefix>/snapshot" \
  --s3.region us-west-2 \
  --output-filename-template '{{.DB}}.{{.Table}}.{{.Index}}'
```

Create changefeed with `protocol=csv`, `date-separator=day`, `include-commit-ts=true`, and `binary-encoding-method='hex'`. Apply DML only after changefeed creation, then wait for increment files before starting `tidb2snowflake`.
