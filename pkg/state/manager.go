package state

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

// Manager owns durable replication state and its consistency checks.
type Manager interface {
	Snapshot() State
	Update(context.Context, func(*State) error) error
	SetSnapshotTSO(context.Context, string) error
	UpdateExportState(context.Context, string, string) error
	UpdateChangefeedID(context.Context, string) error
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
		if err := m.save(ctx, m.state); err != nil {
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
		if err := m.save(ctx, m.state); err != nil {
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

func (m *manager) Update(ctx context.Context, fn func(*State) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	next := cloneState(m.state)
	if err := fn(&next); err != nil {
		return err
	}
	if err := validateState(next, m.tables); err != nil {
		return err
	}
	if err := m.save(ctx, next); err != nil {
		return err
	}
	m.state = next
	return nil
}

func (m *manager) SetSnapshotTSO(ctx context.Context, tso string) error {
	if tso == "" {
		return nil
	}
	if _, err := strconv.ParseUint(tso, 10, 64); err != nil {
		return errors.Annotatef(err, "parse snapshot.tso %q", tso)
	}
	return m.Update(ctx, func(st *State) error {
		if st.Snapshot.TSO != "" && st.Snapshot.TSO != tso {
			return errors.Errorf("snapshot.tso mismatch: state %s, new %s", st.Snapshot.TSO, tso)
		}
		st.Snapshot.TSO = tso
		return nil
	})
}

func (m *manager) UpdateExportState(ctx context.Context, exportID, snapshotTSO string) error {
	return m.Update(ctx, func(st *State) error {
		if exportID != "" {
			st.TaskInfo.ExportID = exportID
		}
		if snapshotTSO != "" {
			if st.Snapshot.TSO != "" && st.Snapshot.TSO != snapshotTSO {
				return errors.Errorf("snapshot.tso mismatch: state %s, export %s", st.Snapshot.TSO, snapshotTSO)
			}
			st.Snapshot.TSO = snapshotTSO
		}
		return nil
	})
}

func (m *manager) UpdateChangefeedID(ctx context.Context, changefeedID string) error {
	if changefeedID == "" {
		return nil
	}
	return m.Update(ctx, func(st *State) error {
		st.TaskInfo.ChangefeedID = changefeedID
		return nil
	})
}

func (m *manager) save(ctx context.Context, st State) error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return errors.Trace(err)
	}
	data = append(data, '\n')
	if err := m.store.WriteFile(ctx, FileName, data); err != nil {
		return errors.Annotatef(err, "write state file %s", FileName)
	}
	return nil
}
