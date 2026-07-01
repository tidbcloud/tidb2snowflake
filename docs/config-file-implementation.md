# Config File Implementation Plan

## Goal

Change `tidb2snowflake snowflake` from many business flags to one TOML config file:

```bash
tidb2snowflake snowflake --config config.toml
```

This is an intentional breaking CLI change. Old business flags such as `--table`,
`--storage`, `--tidb.*`, `--snowflake.*`, and `--aws.*` must be removed and should
fail with Cobra's `unknown flag` error.

## Decisions

- Use `github.com/BurntSushi/toml v1.6.0`, matching TiCDC's direct TOML dependency.
- Decode strictly by checking `toml.MetaData.Undecoded()`, following TiCDC's
  `StrictDecodeFile` pattern in `/Users/edison/go/ticdc/cmd/util/util.go`.
- `--config` is the only accepted configuration flag for the `snowflake` command.
- TiDB Cloud API credentials are not part of the TOML schema. They are read only
  from the existing environment variables:
  - `TIDBCLOUD_CLUSTER_ID`
  - `TIDBCLOUD_PUBLIC_KEY`
  - `TIDBCLOUD_PRIVATE_KEY`
  - `TIDBCLOUD_HOST` optional
- The TiDB Cloud environment variables keep the existing lazy validation behavior:
  validate them only when the run needs to call TiDB Cloud OpenAPI.
- Config validation errors should use config paths, not removed flag names.
  Example: `snowflake.database is required`, not `--snowflake.database is required`.
- `RunE` should load the TOML file before logger initialization. If the config
  file cannot be read or decoded, print the error to Cobra stderr with
  `cmd.PrintErrf` and return it. After config is loaded, initialize the logger
  with `[log]` settings before running replication.

## TOML Schema

Minimal TiDB Cloud config:

```toml
mode = "full"        # optional: full | snapshot-only | incremental-only
source = "tidbcloud" # optional: tidbcloud | op
tables = ["db1.t1"]

[storage]
uri = "s3://bucket/path?region=us-west-2"
access-key = "..."
secret-key = "..."

[snowflake]
account-id = "org-account"
user = "..."
pass = "..."
database = "ODS_DB"
warehouse = "COMPUTE_WH" # optional
```

OP config adds TiDB and TiCDC sections:

```toml
source = "op"

[tidb]
host = "127.0.0.1"
port = 4000
user = "root"
pass = ""
tls = false
ssl-ca = ""

[ticdc]
address = "http://127.0.0.1:8300"
```

Optional tuning:

```toml
[snapshot]
tso = ""
compression = "none"
concurrency = 8

[changefeed]
flush-interval = "60s"
file-size = 64

[increment]
scan-interval = "1m"

[log]
level = "info"
file = ""
```

## Required Config

Always required:

- `tables`
- `storage.uri`
- `storage.access-key`
- `storage.secret-key`
- `snowflake.account-id`
- `snowflake.user`
- `snowflake.pass`
- `snowflake.database`

Conditionally required:

- `ticdc.address`: required when `source = "op"` and `mode != "snapshot-only"`.
- `TIDBCLOUD_CLUSTER_ID`, `TIDBCLOUD_PUBLIC_KEY`, `TIDBCLOUD_PRIVATE_KEY`:
  required only when a TiDB Cloud OpenAPI call is needed.

Defaults:

- `mode = "full"`
- `source = "tidbcloud"`
- `tidb.host = "127.0.0.1"`
- `tidb.port = 4000`
- `tidb.user = "root"`
- `snowflake.warehouse = "COMPUTE_WH"`
- `snapshot.compression = "none"`
- `snapshot.concurrency = 8`
- `changefeed.flush-interval = "60s"`
- `changefeed.file-size = 64`
- `increment.scan-interval = "1m"`
- `log.level = "info"`

## Verification

- Passed: `go test -ldflags '-checklinkname=0' ./cmd`
- Passed: `go test -ldflags '-checklinkname=0' ./...`
- Passed: `go test -tags e2e -ldflags '-checklinkname=0' -run '^$' ./test/e2e`
- Passed: `bash -n scripts/local_tiup_s3_snowflake_e2e.sh`
