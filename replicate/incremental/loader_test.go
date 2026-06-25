package incremental

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadStartsConfiguredTables(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tables := make([]string, 20)
	for i := range tables {
		tables[i] = fmt.Sprintf("db.t%d", i+1)
	}
	cfg := Config{
		Tables:       tables,
		ScanInterval: time.Millisecond,
	}
	started := make(map[string]struct{})
	var mu sync.Mutex
	sessions := make([]tableSession, 0, len(cfg.Tables))
	for _, tableFQN := range cfg.Tables {
		tableFQN := tableFQN
		sessions = append(sessions, tableSession{
			tableFQN: tableFQN,
			sync: func() error {
				mu.Lock()
				started[tableFQN] = struct{}{}
				if len(started) == len(cfg.Tables) {
					cancel()
				}
				mu.Unlock()
				return nil
			},
		})
	}
	err := runSessions(ctx, cfg.ScanInterval, sessions)

	require.Error(t, err)
	require.Contains(t, err.Error(), context.Canceled.Error())
	require.Len(t, started, len(cfg.Tables))
	for _, tableFQN := range cfg.Tables {
		require.Contains(t, started, tableFQN)
	}
}

func TestLoadReturnsTableError(t *testing.T) {
	tableErr := errors.New("merge failed")
	cfg := Config{
		Tables:       []string{"db.t1"},
		ScanInterval: time.Millisecond,
	}

	err := runSessions(context.Background(), cfg.ScanInterval, []tableSession{{
		tableFQN: "db.t1",
		sync: func() error {
			return tableErr
		},
	}})

	require.Error(t, err)
	require.Contains(t, err.Error(), tableErr.Error())
}
