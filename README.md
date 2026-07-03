# tidb2snowflake

Replicate snapshot and incremental data from a **TiDB** cluster into
**Snowflake**.

The default source deployment mode is **TiDB Cloud**. In that mode the tool
drives the managed cluster through **TiDB Cloud OpenAPI**:

- **Snapshot** — `ExportService.CreateExport` exports the full snapshot (CSV) to
  your object storage.
- **Incremental** — `ChangefeedService.CreateChangefeed` streams change data
  (CSV) to the same object storage via a `CLOUD_STORAGE` sink.

For OP-deployed TiDB clusters, set `source = "op"` in the config file. In OP
mode the tool uses the configured TiDB SQL endpoint directly and creates a TiCDC
OpenAPI v2 cloud-storage changefeed against `ticdc.address`; snapshots are
dumped with Dumpling into the same object storage layout.

The tool then loads the snapshot and applies the incremental changes into
Snowflake.

```
TiDB Cloud cluster --(OpenAPI export)---+
                                        +-> object storage (S3) -> tidb2snowflake -> Snowflake
TiDB Cloud cluster --(OpenAPI cdc)------+

OP TiDB cluster --(Dumpling snapshot)---+
                                        +-> object storage (S3) -> tidb2snowflake -> Snowflake
OP TiCDC service --(OpenAPI cdc)--------+
```

## Status

Under active development. The CLI can orchestrate TiDB Cloud OpenAPI or OP
TiDB/TiCDC sources, persist replication state in object storage, and load
snapshot / incremental data into Snowflake.

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
| `pkg/state` | Replication state file manager |
| `snapshot` | Snapshot loading into Snowflake |
| `incremental` | Incremental CDC apply into Snowflake |
| `version` | Build/version info |

## Requirements

- For the default `source = "tidbcloud"`: a TiDB Cloud Serverless / Essential
  cluster and an API key (public/private).
- For `source = "op"`: a TiDB SQL endpoint and a reachable TiCDC OpenAPI service
  address.
- A Snowflake account, warehouse, and target database.
- Object storage (S3) writable by the export/changefeed and readable by this tool.

Snowflake target objects use this layout:

```text
<snowflake_database>.<source_database>.<source_table>
```

Regular source identifiers are created as uppercase Snowflake identifiers so
they can be queried without double quotes. For example, source table `source.t`
loads into `ODS_DB.SOURCE.T` when `snowflake.database = "ODS_DB"` is used. The
loader also creates one internal external stage at
`<snowflake_database>.TIDB2SNOWFLAKE_INTERNAL.TIDB2SNOWFLAKE_EXTERNAL`.

## Configuration

The `create` command reads runtime settings from a TOML file:

```bash
./bin/tidb2snowflake create --config config.toml
```

Minimal TiDB Cloud config:

```toml
mode = "all"
source = "tidbcloud"
tables = ["db1.t1"]

[storage]
uri = "s3://bucket/path?region=us-west-2"
access-key = "..."
secret-access-key = "..."

[snowflake]
account-id = "org-account"
user = "..."
password = "..."
database = "ODS_DB"
warehouse = "COMPUTE_WH"

[tidbcloud]
cluster-id = "..."
public-key = "..."
private-key = "..."
host = ""
```

## Source deployment modes

`source = "tidbcloud"` is the default.
TiDB Cloud API parameters are only needed when the tool must create or wait on a
managed export/changefeed. Configure TiDB Cloud OpenAPI in the config file:

```toml
[tidbcloud]
cluster-id = "..."
public-key = "..."
private-key = "..."
host = ""
```

`source = "op"` uses the direct TiDB and TiCDC services instead of TiDB Cloud
OpenAPI. Add TiDB and TiCDC sections to the config file:

```toml
mode = "all"
source = "op"
tables = ["db1.t1"]

[storage]
uri = "s3://bucket/path?region=us-west-2"
access-key = "..."
secret-access-key = "..."

[tidb]
host = "tidb.example.com"
port = 4000
user = "root"
password = ""
tls = false
ssl-ca = ""

[ticdc]
address = "http://ticdc.example.com:8300"

[snowflake]
account-id = "org-account"
user = "..."
password = "..."
database = "ODS_DB"
warehouse = "COMPUTE_WH"
```

In OP mode with `mode = "all"`, the tool records a TiDB TSO, creates a TiCDC cloud-storage
changefeed from that TSO, waits for the changefeed to become running, then dumps
the snapshot with Dumpling. After Dumpling finishes, `snapshot/metadata` `Pos`
is read back as the final snapshot TSO. `snapshot.concurrency` controls Dumpling
snapshot dump concurrency in OP mode.

Set `changefeed.rcu` to request a TiDB Cloud changefeed RCU tier. When it is
non-zero, the tool fetches the cluster's available changefeed specifications
before creating the changefeed and fails fast if the requested value is not
available.

To delete the changefeed recorded in `replication-state.json`, use the same
config file:

```bash
./bin/tidb2snowflake delete --config config.toml
```

## Reusing an existing export / changefeed

If state contains an existing export or changefeed id, the tool waits for that
same source job on restart. If no source job id exists, the tool checks whether
the storage already contains `snapshot/` or `increment/` data. If it does, that
source creation step is skipped and the existing data is used.

For snapshot data, `snapshot/metadata` `Pos` is the final source of truth for
the initial `checkpoint_ts`. Snapshot load is all-or-nothing at the phase level:
after all configured snapshot files have been loaded into Snowflake,
`checkpoint_ts` and `snapshot_finished=true` are written together. On the next
run, `snapshot_finished=true` skips snapshot loading.

Snapshot export compression defaults to `none`; set
`snapshot.compression = "gzip"` to ask TiDB Cloud export for gzip CSV files and
configure Snowflake `COPY` to read gzip input. Use `increment.scan-interval` to
tune how often the loader scans incremental storage; the default is `1m`.

## Type mapping

The target Snowflake type is derived from TiDB column metadata. The table below
documents the intended mapping for the supported scalar type families.

| TiDB type family | Snowflake type | Notes |
|---|---|---|
| `BOOL`, `BOOLEAN` | `BOOLEAN` | Boolean values. |
| `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, `BIGINT` | `NUMBER` | Signed and unsigned integer variants map to `NUMBER`. |
| `YEAR` | `NUMBER` | Preserves the numeric year value. |
| `FLOAT`, `DOUBLE` | `FLOAT` | Approximate numeric values. |
| `DECIMAL`, `NUMERIC` | `NUMBER(p, s)` | Must fit Snowflake precision/scale limits. Unsigned decimals with precision greater than 38 are stored as `VARCHAR`. |
| `DATE` | `DATE` | Date values. |
| `DATETIME` | `DATETIME(p)` | Precision follows TiDB metadata. |
| `TIMESTAMP` | `TIMESTAMP(p)` | Precision follows TiDB metadata. |
| `TIME` | `TIME(p)` | Precision follows TiDB metadata. |
| `CHAR`, `VARCHAR` | `CHAR(n)`, `VARCHAR(n)` | Length follows TiDB metadata. |
| `TINYTEXT`, `TEXT`, `MEDIUMTEXT`, `LONGTEXT` | `TEXT` | Text values. |
| `ENUM` | `VARCHAR` | Stores the selected enum label as text. |
| `SET` | `VARCHAR` | Stores the selected set labels as text. |
| `JSON` | `VARCHAR` | Stores the JSON text. It is not loaded as Snowflake `VARIANT`. |
| `VECTOR` | `VARCHAR` | Stores the textual vector representation. |
| `BIT` | `NUMBER` | TiCDC CSV encodes bit values as integers. |
| `BINARY`, `VARBINARY`, `TINYBLOB`, `BLOB`, `MEDIUMBLOB`, `LONGBLOB` | `BINARY(n)` | Binary values are loaded with hex decoding for incremental files. `LONGBLOB` is capped at Snowflake's 64 MB `BINARY` limit. |

## Known limitations

- Only Snowflake is supported as the target.
- Only tables with a primary key are supported.
- Not all DDLs are supported (TiDB and Snowflake are not fully type-compatible).
- Binary values larger than Snowflake's `BINARY` limit fail during load. The tool does not truncate, skip, or ignore those columns.

## License

[Apache 2.0](LICENSE)
