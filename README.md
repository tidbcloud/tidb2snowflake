# tidb2snowflake

Replicate snapshot and incremental data from a **TiDB Cloud Serverless / Essential**
cluster into **Snowflake**.

Unlike the legacy `tidb2dw` tool, this tool does **not** embed Dumpling and does
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
git clone <repo-url>
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

## Known limitations

- Only Snowflake is supported as the target.
- Only tables with a primary key are supported.
- Not all DDLs are supported (TiDB and Snowflake are not fully type-compatible).

## License

[Apache 2.0](LICENSE)
