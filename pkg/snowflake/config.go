package snowflake

import (
	"database/sql"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/snowflakedb/gosnowflake/v2"
	"go.uber.org/zap"
)

type Config struct {
	AccountId string
	User      string
	Pass      string
	Warehouse string
	Database  string
}

// Open a connection to Snowflake.
func OpenDB(config *Config) (*sql.DB, error) {
	sfConfig := gosnowflake.Config{
		Account:  config.AccountId,
		User:     config.User,
		Password: config.Pass,
	}
	testDB, err := establishSnowflakeConnection(&sfConfig)
	if err != nil {
		return nil, err
	}
	defer testDB.Close()
	// make sure database exists, if not then create
	_, err = testDB.Exec("CREATE DATABASE IF NOT EXISTS IDENTIFIER(?)", config.Database)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to create database")
	}

	sfConfig.Warehouse = config.Warehouse
	sfConfig.Database = config.Database
	result, err := establishSnowflakeConnection(&sfConfig)
	if err != nil {
		return nil, err
	}

	log.Info("Snowflake connection established",
		zap.String("account", config.AccountId),
		zap.String("warehouse", config.Warehouse),
		zap.String("database", config.Database))
	return result, nil
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
		db.Close()
		return nil, errors.Annotate(err, "Failed to open Snowflake connection")
	}
	return db, nil
}
