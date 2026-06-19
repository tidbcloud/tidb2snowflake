# Correctness Test Plan Review

Reviewed document: [correctness-test-plan.md](./correctness-test-plan.md)

## Summary

The updated plan is much stronger than the previous version. It now names the
real state file, defines a CDC verification cutoff, separates terminal DDL
semantics, gives concrete failure-injection hook ideas, and adds value
normalization rules for comparisons.

The remaining issues are mostly about execution fidelity: several P0 claims are
not actually covered by the mapped runners yet, and one expected marker
precedence rule conflicts with the current increment loader behavior.

## Resolved Since Prior Review

- `tidb2snowflake.state.json` is now used instead of the old `state.json`
  shorthand.
- Incremental verification now requires a named CDC cutoff.
- Failure-injection cases now define hook names and marker-state expectations.
- Terminal DDL cases are separated from ordinary schema evolution.
- Verification now includes exact-row checks and type-specific normalization
  rules.

## Findings

### P0: Progress-without-checkpoint expectation conflicts with current loader behavior

The plan says that if `_consumer/progress.json` points past a CDC file but that
file's `.checkpoint` is missing, the implementation must not skip the file and
checkpoint must be treated as stronger applied-state evidence than progress.

That is the right correctness rule, but it does not match the current loader. On
startup, `loadProgress` copies each progress entry into both `progressDMLIdxMap`
and `tableDMLIdxMap`. Later, `getNewFiles` computes new work by diffing the
newly scanned max file index against that original map. If progress says file 5
was applied, and storage contains files 1 through 5 without checkpoint files, the
scan can leave the max at 5 and produce no new range to process. In that state,
the missing-checkpoint file can be skipped based on progress alone.

Recommended fix:

- Mark this case as an expected failure or known P0 correctness risk until the
  loader is fixed.
- Update the implementation so startup progress is reconciled against
  checkpoints before it suppresses replay.
- Add a package-level test that creates `progress.json` ahead of a missing
  checkpoint and asserts the file is replayed or the run fails clearly.

References:

- [correctness-test-plan.md (L586-L599)](./correctness-test-plan.md#L586-L599)
- [increment.go (L245-L272)](../replicate/increment.go#L245-L272)
- [increment.go (L320-L367)](../replicate/increment.go#L320-L367)

### P0: P0 data-type value round-trip is not covered by the mapped matrix runner

The plan requires supported data types to round-trip into Snowflake with correct
value semantics, including unsigned max values, decimals, binary bytes,
timestamps, NULLs, and empty strings. It maps the type/DDL matrix to
`./scripts/local_tiup_ddl_matrix_smoke.sh`.

That script currently loads the SQL fixture into local TiDB and verifies metadata
mapping plus generated DDL SQL. It does not dump the matrix table to object
storage, load it into Snowflake, replay DML, or run the value comparison rules
defined later in the plan. Passing this runner therefore does not prove the P0
data-type correctness contract.

Recommended fix:

- Split the matrix into two gates: metadata/DDL smoke and Snowflake value
  round-trip.
- Add a matrix e2e path that loads `supported_type_matrix` and
  `supported_composite_pk_dml` into Snowflake and runs the normalization checks.
- Keep the current smoke test as a fast entry gate, but do not count it as the
  P0 value round-trip gate.

References:

- [correctness-test-plan.md (L222-L265)](./correctness-test-plan.md#L222-L265)
- [correctness-test-plan.md (L728-L753)](./correctness-test-plan.md#L728-L753)
- [ddl-matrix.md (L21-L28)](./ddl-matrix.md#L21-L28)
- [local_tiup_ddl_matrix_smoke.sh (L103-L168)](../scripts/local_tiup_ddl_matrix_smoke.sh#L103-L168)
- [supported_matrix.sql (L10-L225)](../test/fixtures/supported_matrix.sql#L10-L225)

### P1: Existing state reuse still implies API credentials, despite the plan wording

The existing export/changefeed reuse case says TiDB Cloud API credentials are
not required when creation is skipped by existing data or state. Existing data
and existing state are different paths in the current implementation.

If snapshot or increment objects already exist and there is no stored job ID, the
tool can skip creation without calling TiDB Cloud. But when
`tidb2snowflake.state.json` contains an export or changefeed ID, the code calls
the OpenAPI client to resume/wait for that job. In that state-based reuse path,
credentials and `--tidbcloud.cluster-id` are still required.

Recommended fix:

- Change the plan to say credentials are not required only when object-storage
  data exists and no stored export/changefeed ID needs to be waited on.
- Add separate test cases for data-only reuse and state-ID reuse.

References:

- [correctness-test-plan.md (L208-L220)](./correctness-test-plan.md#L208-L220)
- [core.go (L144-L165)](../cmd/core.go#L144-L165)
- [core.go (L172-L187)](../cmd/core.go#L172-L187)
- [core.go (L223-L275)](../cmd/core.go#L223-L275)

### P1: Execution mapping overstates P0 end-to-end coverage

The plan's P0 full snapshot plus incremental DML case includes delete/reinsert
of the same primary key and primary-key update when supported. The local and
Cloud happy-path runners currently exercise insert, non-key update, and delete,
but not delete/reinsert or primary-key update. They also do not assert the
object-storage markers required by the plan.

This leaves a gap between "P0 end-to-end cases pass" and what the named commands
actually prove.

Recommended fix:

- Add a case-to-runner table that lists every P0 scenario and the exact script or
  Go test that executes it.
- Extend local and Cloud e2e coverage to include delete/reinsert, primary-key
  update if supported, and marker assertions.
- Until then, mark those bullets as uncovered rather than implied by the current
  happy-path runners.

References:

- [correctness-test-plan.md (L155-L170)](./correctness-test-plan.md#L155-L170)
- [correctness-test-plan.md (L117-L124)](./correctness-test-plan.md#L117-L124)
- [local_tiup_s3_snowflake_e2e.sh (L303-L339)](../scripts/local_tiup_s3_snowflake_e2e.sh#L303-L339)
- [e2e_test.go (L110-L139)](../test/e2e/e2e_test.go#L110-L139)
- [e2e_test.go (L141-L176)](../test/e2e/e2e_test.go#L141-L176)

### P2: Failure-injection gate is still conditional, but exit criteria are unconditional

The plan correctly says failure-injection cases become release gates only after
deterministic hooks exist. The exit criteria, however, still require
failure-injection cases to preserve final correctness without restating that
precondition. That can confuse release execution today, because the hook names in
the plan are proposed but not implemented in the codebase.

Recommended fix:

- In the exit criteria, split "current gates" from "gates after hook
  implementation".
- Or add an explicit line: "Until the hooks exist, failure-injection cases are
  design-required but not executable release gates."

References:

- [correctness-test-plan.md (L478-L501)](./correctness-test-plan.md#L478-L501)
- [correctness-test-plan.md (L755-L767)](./correctness-test-plan.md#L755-L767)

## Suggested Next Steps

1. Treat the progress-without-checkpoint case as the next implementation bug or
   expected-failing test.
2. Add a real Snowflake value round-trip runner for the supported type matrix.
3. Split existing-data reuse from stored-state reuse in the plan and tests.
4. Add a case-to-runner coverage table for every P0 scenario.
5. Make the failure-injection exit criteria conditional until hooks are actually
   implemented.
