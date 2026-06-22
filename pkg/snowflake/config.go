package snowflake

import (
	"database/sql"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/snowflakedb/gosnowflake"
	"go.uber.org/zap"
)

type Config struct {
	AccountId string
	Warehouse string
	User      string
	Pass      string
	Database  string
	Schema    string
}

// Open a connection to Snowflake.
func OpenDB(config *Config) (*sql.DB, error) {
	sfConfig := gosnowflake.Config{
		Account:  config.AccountId,
		User:     config.User,
		Password: config.Pass,
	}
	db, err := establishSnowflakeConnection(&sfConfig)
	if err != nil {
		return nil, err
	}
	// make sure database exists, if not then create
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS IDENTIFIER(?)", config.Database)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to create database")
	}
	// make sure schema exists, if not then create
	_, err = db.Exec("CREATE SCHEMA IF NOT EXISTS IDENTIFIER(?)", fmt.Sprintf("%s.%s", config.Database, config.Schema))
	if err != nil {
		return nil, errors.Annotate(err, "Failed to create schema")
	}
	sfConfig.Database = config.Database
	sfConfig.Schema = config.Schema
	sfConfig.Warehouse = config.Warehouse
	db, err = establishSnowflakeConnection(&sfConfig)
	if err != nil {
		return nil, err
	}

	log.Info("Snowflake connection established",
		zap.String("account", config.AccountId),
		zap.String("warehouse", config.Warehouse),
		zap.String("database", config.Database),
		zap.String("schema", config.Schema))
	return db, nil
}

func establishSnowflakeConnection(sfConfig *gosnowflake.Config) (*sql.DB, error) {
	dsn, err := gosnowflake.DSN(sfConfig)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to generate Snowflake DSN")
	}
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to open Snowflake connection")
	}
	// make sure the connection is available
	if err = db.Ping(); err != nil {
		return nil, errors.Annotate(err, "Failed to open Snowflake connection")
	}
	return db, nil
}
