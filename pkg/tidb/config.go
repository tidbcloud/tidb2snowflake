package tidb

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

type Config struct {
	Host  string
	Port  int
	User  string
	Pass  string
	TLS   bool
	SSLCA string
}

func newMySQLConfig(cfg *Config) (*mysql.Config, error) {
	config := &mysql.Config{
		User:   cfg.User,
		Passwd: cfg.Pass,
		Net:    "tcp",
		Addr:   fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
	}
	if cfg.SSLCA != "" {
		rootCertPool := x509.NewCertPool()
		pem, err := os.ReadFile(cfg.SSLCA)
		if err != nil {
			return nil, err
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			return nil, errors.Errorf("Failed to append PEM from SSL CA file: %s", cfg.SSLCA)
		}
		mysql.RegisterTLSConfig("tidb", &tls.Config{
			RootCAs:    rootCertPool,
			MinVersion: tls.VersionTLS12,
			ServerName: cfg.Host,
		})
		config.TLSConfig = "tidb"
	} else if cfg.TLS {
		config.TLSConfig = "true"
	}
	return config, nil
}

// OpenDB opens a connection to TiDB
func OpenDB(config *Config) (*sql.DB, error) {
	cfg, err := newMySQLConfig(config)
	if err != nil {
		return nil, errors.Annotate(err, "Failed to create MySQL config")
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
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
