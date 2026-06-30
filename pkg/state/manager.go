package state

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

// Manager owns durable replication state and its consistency checks.
type Manager interface {
	Snapshot() State
	SetSnapshotTSO(context.Context, uint64) error
	SetExportID(context.Context, string) error
	SetChangefeedID(context.Context, string) error
	StartIncrementalScan(context.Context, uint64) error
	FinishIncrementalScan(context.Context, uint64) error
	SetDMLFileWatermark(context.Context, string, string, uint64) error
	SetDDLTableVersionWatermark(context.Context, string, uint64) error
	MarkSnapshotFinished(context.Context) error
}

type manager struct {
	mu     sync.Mutex
	store  storeapi.Storage
	tables []string
	state  State
}

func Open(ctx context.Context, store storeapi.Storage, tables []string) (Manager, error) {
	m := &manager{
		store:  store,
		tables: append([]string(nil), tables...),
	}

	exists, err := store.FileExists(ctx, stateFileName)
	if err != nil {
		return nil, errors.Annotatef(err, "check state file %s", stateFileName)
	}
	if !exists {
		m.state = newState(tables)
		if err := m.upload(ctx, m.state); err != nil {
			return nil, err
		}
		return m, nil
	}

	data, err := store.ReadFile(ctx, stateFileName)
	if err != nil {
		return nil, errors.Annotatef(err, "read state file %s", stateFileName)
	}
	st, err := decodeState(data)
	if err != nil {
		return nil, errors.Annotatef(err, "decode state file %s", stateFileName)
	}
	changed := ensureConfiguredTables(&st, tables)
	if err := validateState(st, tables); err != nil {
		return nil, err
	}
	m.state = st
	if changed {
		if err := m.upload(ctx, m.state); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func (m *manager) Snapshot() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneState(m.state)
}

func (m *manager) update(ctx context.Context, fn func(*State) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	next := cloneState(m.state)
	if err := fn(&next); err != nil {
		return err
	}
	if err := validateState(next, m.tables); err != nil {
		return err
	}
	if err := m.upload(ctx, next); err != nil {
		return err
	}
	m.state = next
	return nil
}

func (m *manager) SetSnapshotTSO(ctx context.Context, tso uint64) error {
	if tso == 0 {
		return errors.New("snapshot.tso is empty")
	}
	return m.update(ctx, func(st *State) error {
		if st.Snapshot.TSO != 0 && st.Snapshot.TSO != tso {
			return errors.Errorf("snapshot.tso mismatch: state %d, new %d", st.Snapshot.TSO, tso)
		}
		st.Snapshot.TSO = tso
		return nil
	})
}

func (m *manager) SetExportID(ctx context.Context, exportID string) error {
	if exportID == "" {
		return nil
	}
	return m.update(ctx, func(st *State) error {
		st.TaskInfo.ExportID = exportID
		return nil
	})
}

func (m *manager) SetChangefeedID(ctx context.Context, changefeedID string) error {
	if changefeedID == "" {
		return nil
	}
	return m.update(ctx, func(st *State) error {
		st.TaskInfo.ChangefeedID = changefeedID
		return nil
	})
}

func (m *manager) StartIncrementalScan(ctx context.Context, highWatermark uint64) error {
	if highWatermark == 0 {
		return errors.New("incremental scan high watermark is empty")
	}
	return m.update(ctx, func(st *State) error {
		st.Incremental.Scan = &ScanState{HighWatermark: highWatermark}
		return nil
	})
}

func (m *manager) FinishIncrementalScan(ctx context.Context, checkpointTS uint64) error {
	if checkpointTS == 0 {
		return errors.New("incremental checkpoint is empty")
	}
	return m.update(ctx, func(st *State) error {
		st.Incremental.CheckpointTS = checkpointTS
		st.Incremental.Scan = nil
		return nil
	})
}

func (m *manager) SetDMLFileWatermark(ctx context.Context, table, scope string, fileIdx uint64) error {
	return m.update(ctx, func(st *State) error {
		tableState, ok := st.Incremental.Tables[table]
		if !ok {
			return errors.Errorf("state missing incremental table entry %q", table)
		}
		if fileIdx > tableState.DMLFileWatermarks[scope] {
			tableState.DMLFileWatermarks[scope] = fileIdx
		}
		st.Incremental.Tables[table] = tableState
		return nil
	})
}

func (m *manager) SetDDLTableVersionWatermark(ctx context.Context, table string, tableVersion uint64) error {
	return m.update(ctx, func(st *State) error {
		tableState, ok := st.Incremental.Tables[table]
		if !ok {
			return errors.Errorf("state missing incremental table entry %q", table)
		}
		if tableVersion > tableState.DDLTableVersionWatermark {
			tableState.DDLTableVersionWatermark = tableVersion
		}
		st.Incremental.Tables[table] = tableState
		return nil
	})
}

func (m *manager) MarkSnapshotFinished(ctx context.Context) error {
	return m.update(ctx, func(st *State) error {
		if st.Snapshot.TSO == 0 {
			return errors.New("snapshot.tso is required before marking snapshot finished")
		}
		st.Snapshot.Finished = true
		st.Incremental.CheckpointTS = st.Snapshot.TSO
		return nil
	})
}

func (m *manager) upload(ctx context.Context, st State) error {
	data, err := json.Marshal(st)
	if err != nil {
		return errors.Trace(err)
	}
	if err := m.store.WriteFile(ctx, stateFileName, data); err != nil {
		return errors.Annotatef(err, "write state file %s", stateFileName)
	}
	return nil
}
