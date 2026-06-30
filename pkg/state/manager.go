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
	// Snapshot returns a deep copy of the current durable state for recovery decisions.
	Snapshot() State

	// SetCheckpointTS records the latest upstream checkpoint confirmed by downstream progress.
	SetCheckpointTS(context.Context, uint64) error

	// SetExportID records the TiDB Cloud export job so a restarted process can resume waiting for it.
	SetExportID(context.Context, string) error

	// SetChangefeedID records the source changefeed job so a restarted process can resume waiting for it.
	SetChangefeedID(context.Context, string) error

	// SetDDLTableVersionWatermark records the latest schema table version applied to Snowflake for a table.
	SetDDLTableVersionWatermark(context.Context, string, uint64) error

	// SetDMLCursors records the latest consumed DML cursor positions for a table.
	SetDMLCursors(context.Context, string, map[string]DMLCursor) error

	// MarkSnapshotFinished records snapshot data and its checkpoint as loaded to Snowflake.
	MarkSnapshotFinished(context.Context, uint64) error
}

type manager struct {
	mu     sync.Mutex
	store  storeapi.Storage
	tables []string
	state  State
}

func Open(ctx context.Context, store storeapi.Storage, tables []string, snapshotFinished bool) (Manager, error) {
	m := &manager{
		store:  store,
		tables: append([]string(nil), tables...),
	}

	exists, err := store.FileExists(ctx, stateFileName)
	if err != nil {
		return nil, errors.Annotatef(err, "check state file %s", stateFileName)
	}
	if !exists {
		m.state = newState(tables, snapshotFinished)
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
	if err := validateState(st, tables); err != nil {
		return nil, err
	}
	m.state = st
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

func (m *manager) SetCheckpointTS(ctx context.Context, checkpointTS uint64) error {
	if checkpointTS == 0 {
		return errors.New("checkpoint_ts is empty")
	}
	return m.update(ctx, func(st *State) error {
		if checkpointTS < st.CheckpointTS {
			return errors.Errorf("checkpoint_ts cannot move backward: state %d, new %d", st.CheckpointTS, checkpointTS)
		}
		st.CheckpointTS = checkpointTS
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

func (m *manager) SetDDLTableVersionWatermark(ctx context.Context, table string, tableVersion uint64) error {
	return m.update(ctx, func(st *State) error {
		tableState, ok := st.Tables[table]
		if !ok {
			return errors.Errorf("state missing table entry %q", table)
		}
		if tableVersion > tableState.DDLTableVersionWatermark {
			tableState.DDLTableVersionWatermark = tableVersion
		}
		st.Tables[table] = tableState
		return nil
	})
}

func (m *manager) SetDMLCursors(ctx context.Context, table string, cursors map[string]DMLCursor) error {
	return m.update(ctx, func(st *State) error {
		tableState, ok := st.Tables[table]
		if !ok {
			return errors.Errorf("state missing table entry %q", table)
		}
		if tableState.DMLCursors == nil {
			tableState.DMLCursors = make(map[string]DMLCursor, len(cursors))
		}
		for key, cursor := range cursors {
			current, ok := tableState.DMLCursors[key]
			if ok && cursorBefore(cursor, current) {
				return errors.Errorf("dml cursor cannot move backward: table %s cursor %s", table, key)
			}
			tableState.DMLCursors[key] = cursor
		}
		st.Tables[table] = tableState
		return nil
	})
}

func cursorBefore(next, current DMLCursor) bool {
	if next.Date != current.Date {
		return next.Date < current.Date
	}
	return next.FileIndex < current.FileIndex
}

func (m *manager) MarkSnapshotFinished(ctx context.Context, checkpointTS uint64) error {
	if checkpointTS == 0 {
		return errors.New("checkpoint_ts is empty")
	}
	return m.update(ctx, func(st *State) error {
		if checkpointTS < st.CheckpointTS {
			return errors.Errorf("checkpoint_ts cannot move backward: state %d, new %d", st.CheckpointTS, checkpointTS)
		}
		st.CheckpointTS = checkpointTS
		st.SnapshotFinished = true
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
