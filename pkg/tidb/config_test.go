package tidb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewMySQLConfigAllowsNativePasswordForPasswordlessTiDB(t *testing.T) {
	cfg, err := newMySQLConfig(&Config{
		Host: "127.0.0.1",
		Port: 4000,
		User: "root",
	})
	require.NoError(t, err)

	require.True(t, cfg.AllowNativePasswords)
	require.NotContains(t, strings.ToLower(cfg.FormatDSN()), "allownativepasswords=false")
}
