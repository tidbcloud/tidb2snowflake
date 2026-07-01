//go:build e2e

// Package e2e contains end-to-end tests that drive the tidb2snowflake binary
// against a real TiDB Cloud Serverless cluster, real object storage, and a real
// Snowflake account. They are gated behind the `e2e` build tag and are skipped
// unless the required environment is configured (see e2eConfig).
//
// Run with: make e2e   (see docs/e2e.md for the env vars)
package e2e

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	putil "github.com/pingcap/ticdc/pkg/util"
	storage "github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidb"
)

// e2eConfig is the full set of credentials/endpoints an e2e run needs, read
// from environment variables.
type e2eConfig struct {
	// Source TiDB (the SQL endpoint of the same cluster the API points at).
	TiDBHost string
	TiDBPort int
	TiDBUser string
	TiDBPass string

	// TiDB Cloud OpenAPI.
	ClusterID  string
	PublicKey  string
	PrivateKey string
	APIHost    string

	// Object storage.
	StoragePath  string // s3://bucket/prefix
	AWSAccessKey string
	AWSSecretKey string

	// Snowflake.
	SFAccountID string
	SFUser      string
	SFPass      string
	SFWarehouse string
	SFDatabase  string
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// loadE2EConfig reads the e2e environment, skipping the test when the mandatory
// variables are absent.
func loadE2EConfig(t *testing.T) *e2eConfig {
	t.Helper()
	required := []string{
		"E2E_TIDB_HOST", "E2E_TIDB_USER",
		"E2E_TIDBCLOUD_CLUSTER_ID", "E2E_TIDBCLOUD_PUBLIC_KEY", "E2E_TIDBCLOUD_PRIVATE_KEY",
		"E2E_STORAGE", "E2E_AWS_ACCESS_KEY", "E2E_AWS_SECRET_KEY",
		"E2E_SNOWFLAKE_ACCOUNT_ID", "E2E_SNOWFLAKE_USER", "E2E_SNOWFLAKE_PASS", "E2E_SNOWFLAKE_DATABASE",
	}
	var missing []string
	for _, k := range required {
		if os.Getenv(k) == "" {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		t.Skipf("skipping e2e: missing env %s", strings.Join(missing, ", "))
	}
	port := 4000
	fmt.Sscanf(env("E2E_TIDB_PORT", "4000"), "%d", &port)
	return &e2eConfig{
		TiDBHost:     os.Getenv("E2E_TIDB_HOST"),
		TiDBPort:     port,
		TiDBUser:     os.Getenv("E2E_TIDB_USER"),
		TiDBPass:     os.Getenv("E2E_TIDB_PASS"),
		ClusterID:    os.Getenv("E2E_TIDBCLOUD_CLUSTER_ID"),
		PublicKey:    os.Getenv("E2E_TIDBCLOUD_PUBLIC_KEY"),
		PrivateKey:   os.Getenv("E2E_TIDBCLOUD_PRIVATE_KEY"),
		APIHost:      os.Getenv("E2E_TIDBCLOUD_HOST"),
		StoragePath:  strings.TrimRight(os.Getenv("E2E_STORAGE"), "/"),
		AWSAccessKey: os.Getenv("E2E_AWS_ACCESS_KEY"),
		AWSSecretKey: os.Getenv("E2E_AWS_SECRET_KEY"),
		SFAccountID:  os.Getenv("E2E_SNOWFLAKE_ACCOUNT_ID"),
		SFUser:       os.Getenv("E2E_SNOWFLAKE_USER"),
		SFPass:       os.Getenv("E2E_SNOWFLAKE_PASS"),
		SFWarehouse:  env("E2E_SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
		SFDatabase:   os.Getenv("E2E_SNOWFLAKE_DATABASE"),
	}
}

func (c *e2eConfig) tidbConfig() *tidb.Config {
	return &tidb.Config{Host: c.TiDBHost, Port: c.TiDBPort, User: c.TiDBUser, Pass: c.TiDBPass, TLS: true}
}

func (c *e2eConfig) Config() *snowflake.Config {
	return &snowflake.Config{
		AccountId: c.SFAccountID,
		Warehouse: c.SFWarehouse,
		User:      c.SFUser,
		Pass:      c.SFPass,
		Database:  c.SFDatabase,
	}
}

func (c *e2eConfig) tidbDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := tidb.OpenDB(c.tidbConfig())
	if err != nil {
		t.Fatalf("open TiDB: %v", err)
	}
	return db
}

func (c *e2eConfig) snowflakeDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := snowflake.OpenDB(c.Config())
	if err != nil {
		t.Fatalf("open Snowflake: %v", err)
	}
	return db
}

// runTool runs the built binary to completion (used for snapshot-only) and
// returns its combined output.
func runTool(ctx context.Context, t *testing.T, cfg *e2eConfig, mode, storagePath, table string) error {
	t.Helper()
	cmd := exec.CommandContext(ctx, toolBinary, toolArgs(cfg, mode, storagePath, table)...)
	cmd.Env = toolEnv(cfg)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err := cmd.Run()
	t.Logf("tool (mode=%s) output:\n%s", mode, buf.String())
	return err
}

// startTool runs the binary in the background (used for full/incremental-only,
// which stream). The returned stop function terminates the process.
func startTool(t *testing.T, cfg *e2eConfig, mode, storagePath, table string) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, toolBinary, toolArgs(cfg, mode, storagePath, table)...)
	cmd.Env = toolEnv(cfg)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start tool: %v", err)
	}
	return func() {
		cancel()
		_ = cmd.Wait()
		t.Logf("tool (mode=%s) output:\n%s", mode, buf.String())
	}
}

func toolArgs(cfg *e2eConfig, mode, storagePath, table string) []string {
	args := []string{
		"snowflake",
		"--mode", mode,
		"--tidb.host", cfg.TiDBHost,
		"--tidb.port", fmt.Sprintf("%d", cfg.TiDBPort),
		"--tidb.user", cfg.TiDBUser,
		"--tidb.pass", cfg.TiDBPass,
		"--tidb.tls",
		"--snowflake.account-id", cfg.SFAccountID,
		"--snowflake.user", cfg.SFUser,
		"--snowflake.pass", cfg.SFPass,
		"--snowflake.warehouse", cfg.SFWarehouse,
		"--snowflake.database", cfg.SFDatabase,
		"--storage", storagePath,
		"--aws.access-key", cfg.AWSAccessKey,
		"--aws.secret-key", cfg.AWSSecretKey,
		"--table", table,
		"--log.level", "info",
	}
	return args
}

func toolEnv(cfg *e2eConfig) []string {
	env := append([]string{}, os.Environ()...)
	env = append(env,
		"TIDBCLOUD_CLUSTER_ID="+cfg.ClusterID,
		"TIDBCLOUD_PUBLIC_KEY="+cfg.PublicKey,
		"TIDBCLOUD_PRIVATE_KEY="+cfg.PrivateKey,
	)
	if cfg.APIHost != "" {
		env = append(env, "TIDBCLOUD_HOST="+cfg.APIHost)
	}
	return env
}

// ---- assertions ----

// waitForRowCount polls Snowflake until the table has the expected row count or
// the deadline passes.
func waitForRowCount(t *testing.T, db *sql.DB, table string, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var got int
	for time.Now().Before(deadline) {
		got = queryCount(t, db, table)
		if got == want {
			return
		}
		time.Sleep(5 * time.Second)
	}
	t.Fatalf("table %s: row count = %d, want %d after %s", table, got, want, timeout)
}

func queryCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		return -1
	}
	return n
}

// waitForRow polls until a single-column int value for the given id matches want
// (or -1 to assert the row is absent).
func waitForColValue(t *testing.T, db *sql.DB, table, col string, id, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var v sql.NullInt64
		err := db.QueryRow(fmt.Sprintf("SELECT %s FROM %s WHERE id = %d", col, table, id)).Scan(&v)
		switch {
		case want < 0 && err == sql.ErrNoRows:
			return
		case err == nil && want >= 0 && v.Valid && int(v.Int64) == want:
			return
		}
		time.Sleep(5 * time.Second)
	}
	t.Fatalf("table %s id=%d %s did not reach %d within %s", table, id, col, want, timeout)
}

// ---- cleanup ----

// cleanup best-effort removes the Snowflake target table and the TiDB table.
func cleanup(t *testing.T, cfg *e2eConfig, targetTable, dbTable string) {
	t.Helper()
	// Snowflake target table.
	if db, err := snowflake.OpenDB(cfg.Config()); err == nil {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + targetTable); err != nil {
			t.Logf("cleanup: drop snowflake table: %v", err)
		}
		_ = db.Close()
	}
	// TiDB table.
	if db := mustTiDB(cfg); db != nil {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + dbTable); err != nil {
			t.Logf("cleanup: drop tidb table: %v", err)
		}
		_ = db.Close()
	}
}

func targetTable(cfg *e2eConfig, sourceDB, sourceTable string) string {
	return quoteSnowflakeIdent(cfg.SFDatabase) + "." + quoteSnowflakeIdent(sourceDB) + "." + quoteSnowflakeIdent(sourceTable)
}

func quoteSnowflakeIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func mustTiDB(cfg *e2eConfig) *sql.DB {
	db, err := tidb.OpenDB(cfg.tidbConfig())
	if err != nil {
		return nil
	}
	return db
}

func waitForStorageFile(t *testing.T, cfg *e2eConfig, storagePath, file string, timeout time.Duration) {
	t.Helper()
	store := openRunStorage(t, cfg, storagePath)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		exists, err := store.FileExists(context.Background(), file)
		if err == nil && exists {
			return
		}
		time.Sleep(5 * time.Second)
	}
	t.Fatalf("storage file %s did not appear under %s within %s", file, storagePath, timeout)
}

func openRunStorage(t *testing.T, cfg *e2eConfig, storagePath string) storage.Storage {
	t.Helper()
	uri, err := url.Parse(storagePath)
	if err != nil {
		t.Fatalf("parse storage path: %v", err)
	}
	q := url.Values{}
	q.Set("access-key", cfg.AWSAccessKey)
	q.Set("secret-access-key", cfg.AWSSecretKey)
	uri.RawQuery = q.Encode()
	ctx := context.Background()
	store, err := putil.GetExternalStorageWithDefaultTimeout(ctx, uri.String())
	if err != nil {
		t.Fatalf("open storage: %v", err)
	}
	return store
}
