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

## State Sources

- `replication-state.json`: tidb2snowflake durable state.
- `snapshot/metadata` `Pos`: final source of truth for the initial
  `checkpoint_ts`, written to state after snapshot load succeeds.
- `snapshot_finished`: phase gate. `true` means all configured snapshot files
  have reached Snowflake, so the next run skips snapshot loading.
- `increment/metadata` `checkpoint-ts`: TiCDC checkpoint confirmed flushed to
  storage; it is a progress lower bound, not a row-level consume upper bound.
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
- The `Pos` line in `snapshot/metadata` must initialize `checkpoint_ts` after
  snapshot load succeeds.
- TiDB Cloud export TSO, OP pre-dump TSO, and `--snapshot.tso` are allowed to
  pin source job creation, but metadata `Pos` is the final value.

### Snapshot Completion

- `snapshot_finished=false`: load snapshot files and only then write
  `checkpoint_ts` and `snapshot_finished=true` together.
- If snapshot loading fails, `snapshot_finished` stays `false`.
- `snapshot_finished=true`: skip snapshot loading on restart.
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

- Read `increment/metadata` as the checkpoint to persist only after this round
  succeeds.
- Consume complete DML files visible through actual `.index` files.
- Do not synthesize `meta/CDC.index` from a date directory and treat missing
  index as an error.
- Use each table's per-DML-stream cursor to avoid replaying already consumed
  files after restart.
- Process each table in order by table version, partition, date, and file index.
- Different tables may run concurrently; the same table must remain ordered.
- After all table work in the round succeeds, advance `checkpoint_ts` to the
  metadata checkpoint read at the start of the round.

### DDL

- Build table metadata from snapshot schema files and incremental schema files.
- Apply DDL before DML for the same table version.
- Advance `ddl_table_version_watermark` only after Snowflake DDL succeeds.
- Unsupported DDL must fail clearly without advancing the watermark.

### Restart

- Restart with `snapshot_finished=true` skips snapshot.
- Restart after a DML file reaches Snowflake but before state advances may replay
  that file. `MERGE` must keep the result idempotent.
- Restart after DDL reaches Snowflake but before state advances needs explicit
  coverage; non-idempotent DDL replay is still a correctness risk.

## Known Test Debt

- Add fake storage / fake connector tests for metadata checkpoint lower-bound
  semantics, actual-index discovery, DML cursor resume, DDL-before-DML ordering,
  same-table ordering with cross-table concurrency, and state-write failure replay.
- Add Snowflake value round-trip coverage for supported type fixtures.
- Add negative admission tests for no-primary-key tables and unsupported table
  shapes before source jobs or Snowflake side effects.
