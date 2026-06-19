# tidb2snowflake

Replicate snapshot and incremental data from a **TiDB Cloud Serverless / Essential**
cluster into **Snowflake**.

Unlike the legacy `tidb2snowflake` tool, this tool does **not** embed Dumpling and does
**not** talk to TiCDC directly. Instead it drives the managed cluster through
**TiDB Cloud OpenAPI**:

- **Snapshot** — `ExportService.CreateExport` exports the full snapshot (CSV) to
  your object storage.
- **Incremental** — `ChangefeedService.CreateChangefeed` streams change data
  (CSV) to the same object storage via a `CLOUD_STORAGE` sink.

The tool then loads the snapshot and applies the incremental changes into
Snowflake.

```
TiDB Cloud cluster ──(OpenAPI export)──┐
                                       ├─► object storage (S3) ──► tidb2snowflake ──► Snowflake
TiDB Cloud cluster ──(OpenAPI cdc)─────┘
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
| `pkg/snowsql` | Snowflake connector, DDL translation, type mapping |
| `pkg/tidbsql` | TiDB connection and schema/DDL helpers |
| `pkg/coreinterfaces` | Connector interfaces |
| `pkg/utils` | Shared helpers (CSV escaping, incremental table columns) |
| `pkg/metrics` | Prometheus metrics |
| `replicate` | Snapshot loading and incremental apply into the warehouse |
| `version` | Build/version info |

## Requirements

- A TiDB Cloud Serverless / Essential cluster and an API key (public/private).
- A Snowflake account, warehouse, database and schema.
- Object storage (S3) writable by the export/changefeed and readable by this tool.

## Reusing an existing export / changefeed

Before creating an export or changefeed, the tool checks whether the storage
already contains `snapshot/` or `increment/` data. If it does — because a
previous run created it, or because you created the export/changefeed yourself
(handy for testing) — that step is skipped and the existing data is loaded as
is. If both already exist, the run loads without contacting the TiDB Cloud API,
so the API key is not required in that case.

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
the loader restores that applied-file cursor before scanning object storage.

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
