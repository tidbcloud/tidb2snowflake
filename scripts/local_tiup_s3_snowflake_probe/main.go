package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/pingcap/log"
	"github.com/tidbcloud/tidb2snowflake/pkg/snowflake"
	"go.uber.org/zap"
)

func main() {
	var schema string
	var table string
	var timeout time.Duration
	flag.StringVar(&schema, "schema", "", "source database / Snowflake target schema")
	flag.StringVar(&table, "table", "", "table name")
	flag.DurationVar(&timeout, "timeout", 5*time.Minute, "poll timeout")
	flag.Parse()
	if schema == "" {
		fmt.Fprintln(os.Stderr, "--schema is required")
		os.Exit(2)
	}
	if table == "" {
		fmt.Fprintln(os.Stderr, "--table is required")
		os.Exit(2)
	}

	cfg := &snowflake.Config{
		AccountId: os.Getenv("SNOWFLAKE_ACCOUNT_ID"),
		Warehouse: os.Getenv("SNOWFLAKE_WAREHOUSE"),
		User:      os.Getenv("SNOWFLAKE_USER"),
		Pass:      os.Getenv("SNOWFLAKE_PASS"),
		Database:  os.Getenv("SNOWFLAKE_DATABASE"),
	}
	db, err := snowflake.OpenDB(cfg)
	if err != nil {
		log.Panic("open Snowflake", zap.Error(err))
	}
	defer db.Close()

	targetTable := quoteSnowflakeIdent(cfg.Database) + "." + quoteSnowflakeIdent(schema) + "." + quoteSnowflakeIdent(table)
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		var count int
		var amount sql.NullInt64
		var year sql.NullInt64
		var enumVal sql.NullString
		var vectorVal sql.NullString
		var deleted int
		err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*), COALESCE(MAX(IFF(id=2, amount, NULL)), -1), COALESCE(MAX(IFF(id=2, c_year, NULL)), -1), COALESCE(MAX(IFF(id=2, c_enum, NULL)), ''), COALESCE(MAX(IFF(id=2, c_vector, NULL)), ''), SUM(IFF(id=3, 1, 0)) FROM %s", targetTable)).
			Scan(&count, &amount, &year, &enumVal, &vectorVal, &deleted)
		if err == nil {
			last = fmt.Sprintf("count=%d id2_amount=%d id2_year=%d id2_enum=%s id2_vector=%s id3_rows=%d", count, amount.Int64, year.Int64, enumVal.String, vectorVal.String, deleted)
			if count == 4 &&
				amount.Valid && amount.Int64 == 222 &&
				year.Valid && year.Int64 == 2030 &&
				enumVal.Valid && enumVal.String == "large" &&
				vectorVal.Valid && vectorVal.String == "[9,8,7]" &&
				deleted == 0 {
				fmt.Println(last)
				return
			}
		} else {
			last = err.Error()
		}
		time.Sleep(5 * time.Second)
	}
	log.Panic("Snowflake assertion did not pass before timeout", zap.String("last", last))
}

func quoteSnowflakeIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}
