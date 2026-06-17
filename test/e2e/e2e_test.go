//go:build e2e

package e2e

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

// toolBinary is the path to the tidb2snowflake binary built in TestMain.
var toolBinary string

const (
	sourceDB = "tidb2snowflake_e2e"

	// generous bounds: real export + changefeed + Snowflake COPY take a while.
	snapshotTimeout  = 10 * time.Minute
	incrementTimeout = 10 * time.Minute
)

func TestMain(m *testing.M) {
	bin, err := buildToolBinary()
	if err != nil {
		fmt.Fprintf(os.Stderr, "build tidb2snowflake binary: %v\n", err)
		os.Exit(1)
	}
	toolBinary = bin
	os.Exit(m.Run())
}

func buildToolBinary() (string, error) {
	out, err := os.MkdirTemp("", "tidb2snowflake-e2e")
	if err != nil {
		return "", err
	}
	bin := out + "/tidb2snowflake"
	cmd := exec.Command("go", "build", "-ldflags=-checklinkname=0", "-o", bin, "github.com/tidbcloud/tidb2snowflake")
	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	if b, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("%v: %s", err, b)
	}
	return bin, nil
}

// runID returns a unique suffix for a test run's table/schema/storage names.
func runID() string {
	return time.Now().UTC().Format("20060102t150405")
}

// seedSource creates the source table and the baseline rows (ids 1,2,3).
func seedSource(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	mustExec := func(q string, args ...any) {
		if _, err := db.Exec(q, args...); err != nil {
			t.Fatalf("seed %q: %v", q, err)
		}
	}
	mustExec("CREATE DATABASE IF NOT EXISTS " + sourceDB)
	mustExec("DROP TABLE IF EXISTS " + table)
	mustExec(fmt.Sprintf("CREATE TABLE %s (id BIGINT PRIMARY KEY, name VARCHAR(64), amount BIGINT)", table))
	mustExec(fmt.Sprintf("INSERT INTO %s (id, name, amount) VALUES (1,'a',100),(2,'b',200),(3,'c',300)", table))
}

// applyIncrementDMLs inserts ids 4,5, updates id 2's amount, deletes id 3.
// Final expected state: ids {1,2,4,5}, id 2 amount = 222.
func applyIncrementDMLs(t *testing.T, db *sql.DB, table string) {
	t.Helper()
	mustExec := func(q string) {
		if _, err := db.Exec(q); err != nil {
			t.Fatalf("dml %q: %v", q, err)
		}
	}
	mustExec(fmt.Sprintf("INSERT INTO %s (id, name, amount) VALUES (4,'d',400),(5,'e',500)", table))
	mustExec(fmt.Sprintf("UPDATE %s SET amount = 222 WHERE id = 2", table))
	mustExec(fmt.Sprintf("DELETE FROM %s WHERE id = 3", table))
}

// TestSnapshotOnly exports the baseline snapshot and verifies it loads into
// Snowflake. The snapshot-only run terminates on its own.
func TestSnapshotOnly(t *testing.T) {
	cfg := loadE2EConfig(t)
	id := runID()
	table := "t_snap_" + id
	dbTable := sourceDB + "." + table
	schema := "E2E_SNAP_" + id
	storagePath := cfg.StoragePath + "/snap_" + id

	tidb := cfg.tidbDB(t)
	defer tidb.Close()
	seedSource(t, tidb, dbTable)
	defer cleanup(t, cfg, storagePath, schema, dbTable)

	ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
	defer cancel()
	if err := runTool(ctx, t, cfg, "snapshot-only", storagePath, schema, dbTable); err != nil {
		t.Fatalf("snapshot-only run failed: %v", err)
	}

	sf := cfg.snowflakeDB(t, schema)
	defer sf.Close()
	waitForRowCount(t, sf, table, 3, 2*time.Minute)
	waitForColValue(t, sf, table, "amount", 2, 200, time.Minute)
}

// TestFullReplication runs the full pipeline (snapshot + streaming increment),
// applies DMLs while it streams, and verifies the final Snowflake state.
func TestFullReplication(t *testing.T) {
	cfg := loadE2EConfig(t)
	id := runID()
	table := "t_full_" + id
	dbTable := sourceDB + "." + table
	schema := "E2E_FULL_" + id
	storagePath := cfg.StoragePath + "/full_" + id

	tidb := cfg.tidbDB(t)
	defer tidb.Close()
	seedSource(t, tidb, dbTable)
	defer cleanup(t, cfg, storagePath, schema, dbTable)

	stop := startTool(t, cfg, "full", storagePath, schema, dbTable)
	defer stop()

	sf := cfg.snowflakeDB(t, schema)
	defer sf.Close()

	// snapshot should land first
	waitForRowCount(t, sf, table, 3, snapshotTimeout)

	// then apply incremental changes and verify they replicate
	applyIncrementDMLs(t, tidb, dbTable)
	waitForRowCount(t, sf, table, 4, incrementTimeout) // {1,2,4,5}
	waitForColValue(t, sf, table, "amount", 2, 222, incrementTimeout)
	waitForColValue(t, sf, table, "amount", 3, -1, incrementTimeout) // id 3 deleted
}

// TestIncrementalOnly first establishes a snapshot (snapshot-only), then runs
// the streaming incremental mode and verifies later DMLs replicate.
func TestIncrementalOnly(t *testing.T) {
	cfg := loadE2EConfig(t)
	id := runID()
	table := "t_incr_" + id
	dbTable := sourceDB + "." + table
	schema := "E2E_INCR_" + id
	storagePath := cfg.StoragePath + "/incr_" + id

	tidb := cfg.tidbDB(t)
	defer tidb.Close()
	seedSource(t, tidb, dbTable)
	defer cleanup(t, cfg, storagePath, schema, dbTable)

	// establish the snapshot first
	ctx, cancel := context.WithTimeout(context.Background(), snapshotTimeout)
	if err := runTool(ctx, t, cfg, "snapshot-only", storagePath, schema, dbTable); err != nil {
		cancel()
		t.Fatalf("snapshot-only (setup) failed: %v", err)
	}
	cancel()

	sf := cfg.snowflakeDB(t, schema)
	defer sf.Close()
	waitForRowCount(t, sf, table, 3, 2*time.Minute)

	// now stream increments
	stop := startTool(t, cfg, "incremental-only", storagePath, schema, dbTable)
	defer stop()

	applyIncrementDMLs(t, tidb, dbTable)
	waitForRowCount(t, sf, table, 4, incrementTimeout)
	waitForColValue(t, sf, table, "amount", 2, 222, incrementTimeout)
}
