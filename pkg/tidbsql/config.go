package tidbsql

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type TiDBConfig struct {
	Host  string
	Port  int
	User  string
	Pass  string
	TLS   bool
	SSLCA string
}

/// implement the Config interface

// func Open opens a connection to TiDB
func (config *TiDBConfig) OpenDB() (*sql.DB, error) {
	tidbConfig := mysql.NewConfig()
	tidbConfig.User = config.User
	tidbConfig.Passwd = config.Pass
	tidbConfig.Net = "tcp"
	tidbConfig.Addr = fmt.Sprintf("%s:%d", config.Host, config.Port)
	if config.SSLCA != "" {
		rootCertPool := x509.NewCertPool()
		pem, err := os.ReadFile(config.SSLCA)
		if err != nil {
			return nil, err
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			return nil, errors.Errorf("Failed to append PEM.")
		}
		mysql.RegisterTLSConfig("tidb", &tls.Config{
			RootCAs:    rootCertPool,
			MinVersion: tls.VersionTLS12,
			ServerName: config.Host,
		})
		tidbConfig.TLSConfig = "tidb"
	} else if config.TLS {
		tidbConfig.TLSConfig = "true"
	}
	db, err := sql.Open("mysql", tidbConfig.FormatDSN())
	if err != nil {
		return nil, errors.Annotate(err, "Failed to open TiDB connection")
	}
	// make sure the connection is available
	if err = db.Ping(); err != nil {
		return nil, errors.Annotate(err, "Failed to open TiDB connection")
	}
	log.Info("TiDB connection established",
		zap.String("host", config.Host),
		zap.Int("port", config.Port),
		zap.String("user", config.User),
		zap.Bool("tls", config.TLS || config.SSLCA != ""),
		zap.Bool("sslCAConfigured", config.SSLCA != ""))
	return db, nil
}
