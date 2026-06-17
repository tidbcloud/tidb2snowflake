# End-to-end tests

The E2E tests in [`test/e2e`](../test/e2e) drive the real `tidb2snowflake` binary
against a **real TiDB Cloud Serverless cluster**, **real object storage (S3)**,
and a **real Snowflake account**. They are gated behind the `e2e` build tag and
skip automatically unless the required environment variables are set.

```bash
make e2e
```

## What they cover

Each test seeds a uniquely named source table (`tidb2snowflake_e2e.t_*`) with
baseline rows, runs the tool, and verifies the data in a uniquely named
Snowflake schema (`E2E_*`). Resources are cleaned up afterwards (Snowflake
schema, TiDB table, and the export/changefeed recorded in the run's
`tidb2snowflake.state.json`).

| Test | Mode | Flow |
|------|------|------|
| `TestSnapshotOnly` | `snapshot-only` | seed 3 rows → export + load → assert 3 rows in Snowflake |
| `TestFullReplication` | `full` | snapshot loads, then INSERT/UPDATE/DELETE replicate via the changefeed → assert final state |
| `TestIncrementalOnly` | `snapshot-only` then `incremental-only` | establish snapshot, then stream increments → assert |

Baseline rows have ids `1,2,3`. The incremental step inserts `4,5`, updates
`id=2`'s amount to `222`, and deletes `id=3`; the expected final state is ids
`{1,2,4,5}` with `id=2` amount `222`.

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `E2E_TIDB_HOST` | yes | Source cluster SQL endpoint host |
| `E2E_TIDB_PORT` | no (4000) | SQL endpoint port |
| `E2E_TIDB_USER` | yes | SQL user |
| `E2E_TIDB_PASS` | no | SQL password |
| `E2E_TIDBCLOUD_CLUSTER_ID` | yes | Cluster ID for the OpenAPI |
| `E2E_TIDBCLOUD_PUBLIC_KEY` | yes | API key public part |
| `E2E_TIDBCLOUD_PRIVATE_KEY` | yes | API key private part |
| `E2E_TIDBCLOUD_HOST` | no | OpenAPI host override (default `serverless.tidbapi.com`) |
| `E2E_STORAGE` | yes | `s3://bucket/prefix` working path |
| `E2E_AWS_ACCESS_KEY` | yes | AWS access key for the bucket |
| `E2E_AWS_SECRET_KEY` | yes | AWS secret key for the bucket |
| `E2E_SNOWFLAKE_ACCOUNT_ID` | yes | `<organization>-<account>` |
| `E2E_SNOWFLAKE_USER` | yes | Snowflake user |
| `E2E_SNOWFLAKE_PASS` | yes | Snowflake password |
| `E2E_SNOWFLAKE_WAREHOUSE` | no (`COMPUTE_WH`) | Warehouse |
| `E2E_SNOWFLAKE_DATABASE` | yes | Database (schemas are created per run) |

> The TiDB SQL endpoint (`E2E_TIDB_*`) and the OpenAPI cluster
> (`E2E_TIDBCLOUD_CLUSTER_ID`) must be the **same** cluster: the tests seed data
> over SQL and the export reads it via the API.

## Notes

- The tests are timing-sensitive (real export + changefeed + Snowflake COPY).
  Assertions poll with generous deadlines; tune the timeouts in
  `e2e_test.go` for slow environments.
- `make e2e` builds the binary itself (in `TestMain`), so no prior `make build`
  is needed.
