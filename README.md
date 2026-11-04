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
- Sensitive-column projection for TiDB Cloud snapshots and changefeeds
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

- A supported TiDB Cloud Starter/Essential or Premium API cluster (Dedicated is rejected)
- TiDB Cloud API credentials when create flow needs to create/wait managed jobs
- Source SQL credentials (`tidbcloud.export-user` and `tidbcloud.export-password`) when creating a Premium API export

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

### TiDB Cloud Plans and API Selection

The tool identifies the actual `servicePlan` using the cluster ID and API key:

- `Starter` and `Essential` use the Serverless v1beta1 API.
- `Premium`, `Essential_V2`, `Premium_Reserved`, and `BYOC` use the Cloud v1beta2 API.
- `Dedicated` and unknown plans are rejected. Plan routing does not override the service's feature or region restrictions.

The same `changefeed.rcu` setting is used for both APIs: `4` becomes `rcu: 4`
for Serverless or `changefeedCapacity: "4ccu"` for Premium. The default remains
`2`; if it is unavailable for the selected plan, the tool reports the available
values. Existing changefeeds are reused without automatic resizing.

Only two new configuration fields are needed for a new Premium export:

```toml
[tidbcloud]
export-user = "<source-sql-user>"
export-password = "<source-sql-password>"
```

They are not required when waiting for an existing export, loading an existing
snapshot, creating only a changefeed, or deleting a changefeed. Explicit
`snapshot.tso` for a **new Premium export** is rejected until the public API
contract supports it; default snapshots use the export's actual TSO.

Managed job creation supports standard AWS S3. Custom endpoints, non-AWS
providers, temporary session tokens, and other connection settings that both
managed APIs cannot represent are rejected before creation. Premium AWS
changefeeds require the bucket and source cluster to use the same region;
ordinary non-`us-east-1` buckets are supported through bucket region discovery.
OP mode and file-only loading retain their existing storage behavior.

### Column Selectors

For `source = "tidbcloud"`, optional ordered `[[column-selectors]]` rules can
exclude sensitive columns from both the managed snapshot Export and the Cloud
Storage Changefeed:

```toml
[[column-selectors]]
matcher = ["app.orders"]
columns = ["*", "!customer_email", "!phone_number"]
```

The same ordered rules are sent to both jobs, so the snapshot and incremental
streams use the same column projection. Omit the section (or leave it empty)
to retain the legacy all-column behavior. Each rule requires at least one
`matcher` and one `columns` entry. Keep every primary-key column selected,
because Snowflake incremental `MERGE` requires the primary key.

`column-selectors` is not yet supported for `source = "op"`; the command
returns a validation error rather than silently exporting unfiltered data.

### Parameter Reference

`Required` means required by `create` validation.

| Key | Type | Required | Default | Applies to | Description |
|---|---|---|---|---|---|
| `mode` | string | No | `all` | all | `all`, `snapshot-only`, `incremental-only` |
| `source` | string | No | `tidbcloud` | all | `tidbcloud` or `op` |
| `tables` | array[string] | Yes | none | all | Source tables to replicate |
| `column-selectors` | array[table] | No | empty | `source=tidbcloud` | Ordered `matcher`/`columns` projection rules shared by Export and Cloud Storage Changefeed |
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
| `tidbcloud.export-user` | string | Conditional | empty | Premium API export creation | Source TiDB SQL username |
| `tidbcloud.export-password` | string | Conditional | empty | Premium API export creation | Source TiDB SQL password, preserved without trimming |
| `tidbcloud.host` | string | No | `serverless.tidbapi.com` | `source=tidbcloud` | Optional API host override; known prod/staging/dev product hosts select the matching environment; arbitrary hosts retain Serverless-only behavior |
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
| `changefeed.rcu` | int | No | `2` | `source=tidbcloud` | Capacity value: Serverless RCU or the numeric CCU count for Premium; `0` also uses `2`; negative is invalid |
| `increment.scan-interval` | duration string | No | `1m` | all | Increment file scan interval |
| `log.level` | string | No | `info` | all | Log level |
| `log.file` | string | No | empty | all | Log file path |

### State and Resume Behavior

Replication state is stored in object storage and used for restart/resume behavior.

- Existing export/changefeed IDs in state are reused on restart
- An export recorded in state is checked before reusing metadata that may have appeared during an unfinished export
- Changefeed creation requires a nonzero checkpoint, snapshot metadata TSO, or explicit start TSO; the old `FROM_TSO=0` path is rejected
- Reused jobs must match the source cluster, output prefix, table/column filters, and CSV format
- Existing snapshot/increment storage data may skip source creation phase
- Snapshot completion is committed atomically with checkpoint update

For delete flow, the command reads `task_info.changefeed_id` from state,
verifies cluster ownership, and clears the ID only after deletion is confirmed.
It does not delete exported files or Snowflake data.

Cloud-managed tasks use state version 2 to bind the cluster, API family, and
environment. Version 1 jobs are checked through the historical Serverless API
before upgrade; a `replication-state.v1.backup.json` copy is retained. Use the
same host and cluster ID as the original task when upgrading. OP/file-only
state does not require this upgrade. Old binaries cannot read version 2; never
restore an old backup after newer checkpoints have advanced.

A small `pending_create` marker is saved before each managed creation. A timeout,
lost response, or state-write failure can leave the result unknown. Restart then
stops instead of creating another job. Stop the tool and back up state before
checking the named task in Console/API. Verify its cluster, output prefix and
configuration before recording its ID under `task_info` and removing
`pending_create`; clear the marker without an ID only after confirming no task
was created. The tool does not automatically discover or adopt orphan jobs.
Legacy version 1 orphan jobs cannot be recovered automatically either.

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
- `column-selectors` currently requires `source=tidbcloud`
- Managed incremental replication rejects STORED generated columns because snapshot and CDC output differ; VIRTUAL generated columns are skipped. Snapshot-only mode omits generated columns.
- Not all DDL is supported across TiDB and Snowflake compatibility boundaries
- Export finishes before CDC is created. If the snapshot TSO has passed the upstream GC window, creation fails; the tool does not silently move the start point forward or adjust GC
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
