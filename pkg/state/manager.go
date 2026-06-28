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

	MarkSnapshotFinished(context.Context) error
}
type manager struct {
	mu     sync.Mutex
	store  storeapi.Storage
	tables []string
	state  State
}

func Open(ctx context.Context, store storeapi.Storage, tables []string) (Manager, error) {
	if store == nil {
		return nil, errors.New("state storage is nil")
	}
	m := &manager{
		store:  store,
		tables: append([]string(nil), tables...),
	}

	exists, err := store.FileExists(ctx, FileName)
	if err != nil {
		return nil, errors.Annotatef(err, "check state file %s", FileName)
	}
	if !exists {
		m.state = newState(tables)
		if err := m.upload(ctx, m.state); err != nil {
			return nil, err
		}
		return m, nil
	}

	data, err := store.ReadFile(ctx, FileName)
	if err != nil {
		return nil, errors.Annotatef(err, "read state file %s", FileName)
	}
	st, err := decodeState(data)
	if err != nil {
		return nil, errors.Annotatef(err, "decode state file %s", FileName)
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
	return m.update(ctx, func(st *State) error {
		st.Snapshot.TSO = tso
		st.Incremental.CheckpointTS = st.Snapshot.TSO
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

func (m *manager) MarkSnapshotFinished(ctx context.Context) error {
	return m.update(ctx, func(st *State) error {
		st.Snapshot.Finished = true
		return nil
	})
}

func (m *manager) upload(ctx context.Context, st State) error {
	data, err := json.Marshal(st)
	if err != nil {
		return errors.Trace(err)
	}
	if err := m.store.WriteFile(ctx, FileName, data); err != nil {
		return errors.Annotatef(err, "write state file %s", FileName)
	}
	return nil
}
