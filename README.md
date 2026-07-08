# TiDB2Snowflake

Replicate snapshot and incremental data from TiDB to Snowflake through object storage.

TiDB2Snowflake supports both:

- TiDB Cloud source mode (managed export + changefeed via OpenAPI)
- OP source mode (Dumpling snapshot + TiCDC OpenAPI v2 changefeed)

It is designed for resumable replication with state persisted in object storage.

## Choose Your Journey

- **Just want to use the tool?** Jump straight to [Install](#install)
- **Want to hack on the code?** Skip to [Contributor Guide](#contributor-guide)

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Usage](#usage)
- [Configuration & Runtime Behavior](#configuration--runtime-behavior)
- [Reference](#reference)
- [Contributor Guide](#contributor-guide)
    - [Development Workflow](#development-workflow)
    - [How to Contribute](#how-to-contribute)
- [License](#license)

## Features

- Snapshot + incremental replication into Snowflake
- Source mode switch: `tidbcloud` and `op`
- Resumable execution with persisted replication state
- Configurable changefeed flush interval, file size, and RCU
- Automatic mode/source normalization (case-insensitive config values)
- DDL-aware incremental loading pipeline

## Architecture

```text
TiDB Cloud export / OP Dumpling snapshot ----+
                                             +--> S3-compatible storage --> tidb2snowflake --> Snowflake
TiDB Cloud changefeed / OP TiCDC changefeed -+
```

## Prerequisites

- Go 1.23+ (for building from source)
- S3-compatible storage
- Snowflake account, warehouse, and target database

For `source = "tidbcloud"`:

- TiDB Cloud cluster
- TiDB Cloud API credentials when create flow needs to create/wait managed jobs

For `source = "op"`:

- TiDB SQL endpoint
- TiCDC OpenAPI endpoint

## Usage

### Install

Build from source:

```bash
git clone https://github.com/tidbcloud/tidb2snowflake.git
cd tidb2snowflake
make build
```

Check version:

```bash
./bin/tidb2snowflake version
```

### Quick Start

1. Copy and edit config:

```bash
cp config.example.toml config.toml

# Modify the example configurations to your own.
```

2. Start replication:

```bash
./bin/tidb2snowflake create -c config.toml
```

3. Delete the corresponding changefeed (with confirmation prompt):

```bash
./bin/tidb2snowflake delete -c config.toml
```

### Commands

- `version`: print build/version information
- `create`: run snapshot/incremental replication into Snowflake
- `delete`: delete the changefeed ID recorded in state and clear it from state

Notes:

- `create` and `delete` both use TOML config (`-c, --config`)
- `delete` asks for `y/N` confirmation
- For `delete` with `source = "tidbcloud"`, `tidbcloud.cluster-id`, `tidbcloud.public-key`, and `tidbcloud.private-key` are required

## Configuration & Runtime Behavior

### Configuration

Use `config.example.toml` as the template.

### Run Modes

- `mode = "all"` (default): snapshot + incremental
- `mode = "snapshot-only"`: snapshot only
- `mode = "incremental-only"`: incremental only (requires snapshot finished state)

`mode` and `source` are normalized to lowercase before validation.

### Parameter Reference

`Required` means required by `create` validation.

| Key | Type | Required | Default | Applies to | Description |
|---|---|---|---|---|---|
| `mode` | string | No | `all` | all | `all`, `snapshot-only`, `incremental-only` |
| `source` | string | No | `tidbcloud` | all | `tidbcloud` or `op` |
| `tables` | array[string] | Yes | none | all | Source tables to replicate |
| `storage.uri` | string | Yes | none | all | S3 URI for snapshot, incremental, and state |
| `storage.access-key` | string | Yes | none | all | S3 access key |
| `storage.secret-access-key` | string | Yes | none | all | S3 secret access key |
| `snowflake.account-id` | string | Yes | none | all | Snowflake account ID |
| `snowflake.user` | string | Yes | none | all | Snowflake user |
| `snowflake.password` | string | Yes | none | all | Snowflake password |
| `snowflake.database` | string | Yes | none | all | Target Snowflake database |
| `snowflake.warehouse` | string | No | `COMPUTE_WH` | all | Snowflake warehouse |
| `tidbcloud.cluster-id` | string | Conditional | none | `source=tidbcloud` | Cluster ID |
| `tidbcloud.public-key` | string | Conditional | none | `source=tidbcloud` | API public key |
| `tidbcloud.private-key` | string | Conditional | none | `source=tidbcloud` | API private key |
| `tidbcloud.host` | string | No | `serverless.tidbapi.com` | `source=tidbcloud` | Optional API host override |
| `tidb.host` | string | No | `127.0.0.1` | `source=op` | TiDB SQL host |
| `tidb.port` | int | No | `4000` | `source=op` | TiDB SQL port |
| `tidb.user` | string | No | `root` | `source=op` | TiDB SQL user |
| `tidb.password` | string | No | empty | `source=op` | TiDB SQL password |
| `tidb.tls` | bool | No | `false` | `source=op` | Enable TLS to TiDB |
| `tidb.ssl-ca` | string | No | empty | `source=op` | CA file path |
| `ticdc.address` | string | Conditional | none | `source=op` | TiCDC OpenAPI address |
| `snapshot.tso` | string | No | empty | all | Optional start TSO (>0 when set) |
| `snapshot.compression` | string | No | `none` | all | `none` or `gzip` |
| `snapshot.concurrency` | int | No | `8` | `source=op` | Dumpling concurrency |
| `changefeed.flush-interval` | duration string | No | `60s` | all | Flush interval |
| `changefeed.file-size` | int (MiB) | No | `64` | all | File size in MiB |
| `changefeed.rcu` | int | No | `2` | `source=tidbcloud` | Optional changefeed RCU; `0` also uses `2`; negative is invalid |
| `increment.scan-interval` | duration string | No | `1m` | all | Increment file scan interval |
| `log.level` | string | No | `info` | all | Log level |
| `log.file` | string | No | empty | all | Log file path |

### State and Resume Behavior

Replication state is stored in object storage and used for restart/resume behavior.

- Existing export/changefeed IDs in state are reused on restart
- Existing snapshot/increment storage data may skip source creation phase
- Snapshot completion is committed atomically with checkpoint update

For delete flow, the command reads `task_info.changefeed_id` from state.

## Reference

### Type Mapping

The main type mapping rules:

- Integer types -> `NUMBER`
- `DECIMAL`/`NUMERIC` -> `NUMBER(p, s)`
- Temporal types -> Snowflake temporal types
- `JSON`/`VECTOR` -> `VARCHAR`
- Binary families -> `BINARY(n)` with Snowflake size constraints

See source tests and mapping implementation for detailed edge cases.

### Project Structure

| Path | Description |
|---|---|
| `main.go` | CLI entrypoint |
| `cmd/` | Commands, config parsing, validation |
| `source/` | Source preparation logic |
| `snapshot/` | Snapshot loading |
| `incremental/` | Incremental loading |
| `pkg/snowflake/` | Snowflake connector and SQL/DDL helpers |
| `pkg/tidbcloud/` | TiDB Cloud API client |
| `pkg/ticdc/` | TiCDC API client |
| `pkg/dumpling/` | Dumpling helpers |
| `pkg/state/` | Replication state management |

### Known Limitations

- Snowflake is the only supported target
- Source tables must have primary keys
- Not all DDL is supported across TiDB and Snowflake compatibility boundaries
- Binary values larger than Snowflake limits fail during load

## Contributor Guide

This section covers the local development loop and how to contribute changes.

### Development Workflow

Common commands:

```bash
make fmt
make test
make build
```

End-to-end tests:

```bash
make e2e
```

Local helper scripts:

- `scripts/local_tiup_s3_snowflake_e2e.sh`
- `scripts/local_tiup_ddl_matrix_smoke.sh`
- `scripts/compare_table_counts.sh`

Pull request checklist:

1. Fork the repository and create a feature branch.
2. Make your code and test updates.
3. Run the local checks listed above before opening a PR.
4. If your change affects runtime behavior, add or update docs and tests.
5. Open a PR with a clear problem statement, scope, and validation notes.

Recommended areas to start with:

- CLI/config validation in `cmd/`
- Source orchestration in `source/`
- Snowflake loading and SQL generation in `pkg/snowflake/`

### How to Contribute

Contributions are welcome.

- Open an issue for discussion before major changes
- Keep tests updated with behavior changes
- Run `make fmt test` before submitting a PR

## License

Apache 2.0. See `LICENSE`.
