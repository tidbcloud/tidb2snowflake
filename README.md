# tidb2snowflake

Replicate snapshot and incremental data from a **TiDB** cluster into
**Snowflake**.

The default source deployment mode is **TiDB Cloud**. In that mode the tool
drives the managed cluster through **TiDB Cloud OpenAPI**:

- **Snapshot** — `ExportService.CreateExport` exports the full snapshot (CSV) to
  your object storage.
- **Incremental** — `ChangefeedService.CreateChangefeed` streams change data
  (CSV) to the same object storage via a `CLOUD_STORAGE` sink.

For OP-deployed TiDB clusters, use `--source.mode=op`. In OP mode the tool uses
the configured TiDB SQL endpoint directly and creates a TiCDC OpenAPI v2
cloud-storage changefeed against `--ticdc.address`; snapshots are dumped with
Dumpling into the same object storage layout.

The tool then loads the snapshot and applies the incremental changes into
Snowflake.

```
TiDB Cloud cluster ──(OpenAPI export)──┐
                                       ├─► object storage (S3) ──► tidb2snowflake ──► Snowflake
TiDB Cloud cluster ──(OpenAPI cdc)─────┘

OP TiDB cluster ──(Dumpling snapshot)──┐
                                       ├─► object storage (S3) ──► tidb2snowflake ──► Snowflake
OP TiCDC service ──(OpenAPI cdc)───────┘
```

## Status

🚧 Under active development. This repository currently contains the migrated,
reusable building blocks (Snowflake loader, TiDB schema utilities, incremental /
snapshot replication logic, metrics). The OpenAPI-driven orchestration CLI is
being built — see the planning doc and task board.

## Build from source

```bash
git clone https://github.com/tidbcloud/tidb2snowflake.git
cd tidb2snowflake
make build       # produces bin/tidb2snowflake
```

```bash
./bin/tidb2snowflake version
```

## Project layout

| Path | Description |
|------|-------------|
| `main.go` | CLI entrypoint (cobra) |
| `pkg/snowflake` | Snowflake connector, DDL translation, type mapping |
| `pkg/tidb` | TiDB connection and schema/DDL helpers |
| `pkg/utils` | Shared helpers (CSV escaping, incremental table columns) |
| `pkg/metrics` | Prometheus metrics |
| `pkg/tidbcloud` | TiDB Cloud OpenAPI client |
| `pkg/ticdc` | Direct TiCDC OpenAPI v2 client |
| `pkg/dumpling` | Dumpling snapshot wrapper for OP deployments |
| `replicate` | Snapshot loading and incremental apply into the warehouse |
| `version` | Build/version info |

## Requirements

- For the default `--source.mode=tidbcloud`: a TiDB Cloud Serverless / Essential
  cluster and an API key (public/private).
- For `--source.mode=op`: a TiDB SQL endpoint and a reachable TiCDC OpenAPI
  service address.
- A Snowflake account, warehouse, database and schema.
- Object storage (S3) writable by the export/changefeed and readable by this tool.

## Source deployment modes

`--source.mode=tidbcloud` is the default and preserves the existing behavior.
TiDB Cloud API parameters are only needed when the tool must create or wait on a
managed export/changefeed:

```bash
./bin/tidb2snowflake snowflake \
  --source.mode=tidbcloud \
  --tidbcloud.cluster-id "$TIDBCLOUD_CLUSTER_ID" \
  --tidbcloud.public-key "$TIDBCLOUD_PUBLIC_KEY" \
  --tidbcloud.private-key "$TIDBCLOUD_PRIVATE_KEY" \
  --tidb.host "$TIDB_HOST" \
  --storage "s3://bucket/path" \
  --table db1.t1
```

`--source.mode=op` uses the direct TiDB and TiCDC services instead of TiDB Cloud
OpenAPI. The existing `--tidb.*` flags configure the TiDB SQL endpoint, and
`--ticdc.address` points at the TiCDC OpenAPI service:

```bash
./bin/tidb2snowflake snowflake \
  --source.mode=op \
  --tidb.host "$TIDB_HOST" \
  --tidb.port 4000 \
  --ticdc.address "http://ticdc.example.com:8300" \
  --storage "s3://bucket/path" \
  --table db1.t1
```

In full OP mode, the tool records a TiDB TSO, creates a TiCDC cloud-storage
changefeed from that TSO, waits for the changefeed to become running, then dumps
the snapshot with Dumpling. `--snapshot.concurrency` controls Dumpling snapshot
dump concurrency in OP mode.

## Reusing an existing export / changefeed

Before creating an export or changefeed, the tool checks whether the storage
already contains `snapshot/` or `increment/` data. If it does — because a
previous run created it, or because you created the export/changefeed yourself
(handy for testing) — that step is skipped and the existing data is loaded as
is. If both already exist, the run loads without contacting the TiDB Cloud API
in TiDB Cloud mode or the TiCDC API in OP mode.

Snapshot loading writes a per-table `loadinfo` marker after Snowflake `COPY`
finishes. A later run skips snapshot loading for tables with that marker, then
continues incremental replay. By default, snapshot files are loaded with one
bulk `COPY` statement using a Snowflake `PATTERN`; use
`--snapshot.load-mode=per-file` to fall back to one `COPY` per exported file.
Snapshot export compression defaults to `none`; use `--snapshot.compression=gzip`
to ask TiDB Cloud export for gzip CSV files and configure Snowflake `COPY` to
read gzip input.

Incremental replay writes a per-table `_consumer/progress.json` under the
incremental storage prefix after each successfully applied CDC file. On restart,
the loader restores that applied-file cursor before scanning object storage. Use
`--increment.scan-interval` to tune how often the loader scans the incremental
storage prefix; the default is `1m`.

## Type mapping

The target Snowflake type is derived from TiDB column metadata. The table below
documents the intended mapping for the supported scalar type families.

| TiDB type family | Snowflake type | Notes |
|---|---|---|
| `BOOL`, `BOOLEAN` | `BOOLEAN` | Boolean values. |
| `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `BIGINT` | `NUMBER` | Signed and unsigned integer variants map to `NUMBER`. |
| `YEAR` | `NUMBER` | Preserves the numeric year value. |
| `FLOAT`, `DOUBLE` | `FLOAT` | Approximate numeric values. |
| `DECIMAL`, `NUMERIC` | `NUMBER(p, s)` | Must fit Snowflake precision/scale limits. |
| `DATE` | `DATE` | Date values. |
| `DATETIME` | `DATETIME(p)` | Precision follows TiDB metadata. |
| `TIMESTAMP` | `TIMESTAMP(p)` | Precision follows TiDB metadata. |
| `TIME` | `TIME(p)` | Precision follows TiDB metadata. |
| `CHAR`, `VARCHAR` | `CHAR(n)`, `VARCHAR(n)` | Length follows TiDB metadata. |
| `TINYTEXT`, `TEXT`, `MEDIUMTEXT`, `LONGTEXT` | `TEXT` | Text values. |
| `ENUM` | `VARCHAR` | Stores the selected enum label as text. |
| `VECTOR` | `VARCHAR` | Stores the textual vector representation. |
| `BINARY`, `VARBINARY`, `TINYBLOB`, `BLOB` | `BINARY(n)` | Binary values are loaded with hex decoding for incremental files. |

## Known limitations

- Only Snowflake is supported as the target.
- Only tables with a primary key are supported.
- Not all DDLs are supported (TiDB and Snowflake are not fully type-compatible).

## License

[Apache 2.0](LICENSE)
