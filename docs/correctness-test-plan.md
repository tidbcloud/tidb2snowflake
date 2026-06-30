# Correctness Test Plan

This plan covers the current state-manager based replication path. It is a
correctness plan, not a throughput, warehouse sizing, or long-running soak plan.

## Contract

For every configured table, after the tool reaches a stable checkpoint:

- Snowflake contains every committed TiDB row at or before the verified
  checkpoint exactly once.
- TiDB updates and deletes are reflected in Snowflake.
- Re-running with the same storage prefix does not duplicate or lose rows.
- Recovery decisions come from `replication-state.json`, `snapshot/metadata`,
  TiCDC `increment/metadata`, and TiCDC `.index` files.

Legacy `loadinfo`, per-file `.checkpoint`, per-table `_consumer/progress.json`,
and old `tidb2snowflake.state.json` structures are not recovery sources.

## State Sources

- `replication-state.json`: tidb2snowflake durable state.
- `snapshot/metadata` `Pos`: final source of truth for `snapshot.tso`.
- `snapshot.finished`: phase gate. `true` means all configured snapshot files
  have reached Snowflake, so the next run skips snapshot loading.
- `increment/metadata` `checkpoint-ts`: source for a new incremental scan high
  watermark.
- `.index`: strict visibility marker for incremental DML files.

## Entry Gates

```bash
go test -ldflags '-checklinkname=0' ./...
./scripts/local_tiup_ddl_matrix_smoke.sh
```

The DDL matrix script is a fast metadata and generated-SQL smoke test. It does
not prove Snowflake value round-trip correctness.

## Required Cases

### Snapshot TSO

- Existing `snapshot/` data must contain `snapshot/metadata`.
- The `Pos` line in `snapshot/metadata` must be written to `snapshot.tso`.
- If `snapshot.tso` was already recorded and differs from `Pos`, the run fails
  before creating or consuming incremental data.
- TiDB Cloud export TSO, OP pre-dump TSO, and `--snapshot.tso` are allowed to
  pin source job creation, but metadata `Pos` is the final value.

### Snapshot Completion

- `snapshot.finished=false`: load snapshot files and only then write
  `snapshot.finished=true`.
- If snapshot loading fails, `snapshot.finished` stays `false`.
- `snapshot.finished=true`: skip snapshot loading on restart.
- If state write fails after Snowflake COPY succeeds, the next run repeats the
  snapshot phase. `CREATE OR REPLACE TABLE` keeps this safe at phase level.

### Full Replication

- Create or reuse the export/changefeed source job.
- Persist `task_info.export_id` and `task_info.changefeed_id` when jobs are
  created or resumed from state.
- Start incremental consumption from the snapshot TSO.
- Verify insert, update, delete, and delete-then-reinsert final state in
  Snowflake.

### Incremental Scan

- With no active scan, read `increment/metadata` and write
  `incremental.scan.high_watermark` when it is greater than
  `incremental.checkpoint_ts`.
- While a scan is active, keep using the stored high watermark even if TiCDC
  metadata advances.
- Consume only files visible through `.index`.
- Process each table in order by table version, partition, date, and file index.
- Different tables may run concurrently; the same table must remain ordered.
- After all table work in the scan succeeds, set
  `incremental.checkpoint_ts = scan.high_watermark` and clear `scan`.

### DDL

- Build table metadata from snapshot schema files and incremental schema files.
- Apply DDL before DML for the same table version.
- Advance `ddl_table_version_watermark` only after Snowflake DDL succeeds.
- Unsupported DDL must fail clearly without advancing the watermark.

### Restart

- Restart with `snapshot.finished=true` skips snapshot.
- Restart with an active incremental scan resumes the same
  `scan.high_watermark`.
- Restart after a DML file reaches Snowflake but before state advances may replay
  that file. `MERGE` must keep the result idempotent.
- Restart after DDL reaches Snowflake but before state advances needs explicit
  coverage; non-idempotent DDL replay is still a correctness risk.

## Known Test Debt

- Add fake storage / fake connector tests for incremental scan freezing,
  active-scan restart, DDL-before-DML ordering, same-table ordering with
  cross-table concurrency, and state-write failure replay.
- Add Snowflake value round-trip coverage for supported type fixtures.
- Add negative admission tests for no-primary-key tables and unsupported table
  shapes before source jobs or Snowflake side effects.
