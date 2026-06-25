package state

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

const (
	Version  = 1
	FileName = "replication-state.json"
)

type State struct {
	Version     int              `json:"version"`
	TaskInfo    TaskInfo         `json:"task_info"`
	Snapshot    SnapshotState    `json:"snapshot"`
	Incremental IncrementalState `json:"incremental"`
}

type TaskInfo struct {
	ExportID     string `json:"export_id"`
	ChangefeedID string `json:"changefeed_id"`
}

type SnapshotState struct {
	TSO      string `json:"tso"`
	Finished bool   `json:"finished"`
}

type IncrementalState struct {
	Tables map[string]TableState `json:"tables"`
}

type TableState struct {
	DMLFileWatermarks        map[string]uint64 `json:"dml_file_watermarks"`
	DDLTableVersionWatermark uint64            `json:"ddl_table_version_watermark"`
}

type Manager struct {
	mu     sync.Mutex
	store  storeapi.Storage
	tables []string
	state  State
}

func Open(ctx context.Context, store storeapi.Storage, tables []string) (*Manager, error) {
	if store == nil {
		return nil, errors.New("state storage is nil")
	}
	m := &Manager{
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

func (m *Manager) Snapshot() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneState(m.state)
}

func (m *Manager) Update(ctx context.Context, fn func(*State) error) error {
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

func (m *Manager) save(ctx context.Context, st State) error {
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

func newState(tables []string) State {
	st := State{
		Version: Version,
		Incremental: IncrementalState{
			Tables: make(map[string]TableState, len(tables)),
		},
	}
	ensureConfiguredTables(&st, tables)
	return st
}

func ensureConfiguredTables(st *State, tables []string) bool {
	changed := false
	if st.Incremental.Tables == nil {
		st.Incremental.Tables = make(map[string]TableState, len(tables))
		changed = true
	}
	for _, table := range tables {
		if _, ok := st.Incremental.Tables[table]; !ok {
			st.Incremental.Tables[table] = TableState{
				DMLFileWatermarks: make(map[string]uint64),
			}
			changed = true
		}
	}
	return changed
}

func validateState(st State, tables []string) error {
	if st.Version != Version {
		return errors.Errorf("unsupported state version %d", st.Version)
	}
	if st.Incremental.Tables == nil {
		return errors.New("state missing required field incremental.tables")
	}
	for _, table := range tables {
		if _, ok := st.Incremental.Tables[table]; !ok {
			return errors.Errorf("state missing incremental table entry %q", table)
		}
	}
	for table, tableState := range st.Incremental.Tables {
		if tableState.DMLFileWatermarks == nil {
			return errors.Errorf("state missing required field incremental.tables.%s.dml_file_watermarks", table)
		}
	}
	return nil
}

func cloneState(st State) State {
	out := st
	out.Incremental.Tables = make(map[string]TableState, len(st.Incremental.Tables))
	for table, tableState := range st.Incremental.Tables {
		copiedTable := tableState
		copiedTable.DMLFileWatermarks = make(map[string]uint64, len(tableState.DMLFileWatermarks))
		for scope, idx := range tableState.DMLFileWatermarks {
			copiedTable.DMLFileWatermarks[scope] = idx
		}
		out.Incremental.Tables[table] = copiedTable
	}
	return out
}

type rawState struct {
	Version     *int            `json:"version"`
	TaskInfo    *rawTaskInfo    `json:"task_info"`
	Snapshot    *rawSnapshot    `json:"snapshot"`
	Incremental *rawIncremental `json:"incremental"`
}

type rawTaskInfo struct {
	ExportID     *string `json:"export_id"`
	ChangefeedID *string `json:"changefeed_id"`
}

type rawSnapshot struct {
	TSO      *string `json:"tso"`
	Finished *bool   `json:"finished"`
}

type rawIncremental struct {
	Tables *map[string]rawTableState `json:"tables"`
}

type rawTableState struct {
	DMLFileWatermarks        *map[string]uint64 `json:"dml_file_watermarks"`
	DDLTableVersionWatermark *uint64            `json:"ddl_table_version_watermark"`
}

func decodeState(data []byte) (State, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()

	var raw rawState
	if err := dec.Decode(&raw); err != nil {
		return State{}, errors.Trace(err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return State{}, errors.New("state file contains trailing JSON data")
	}

	if raw.Version == nil {
		return State{}, errors.New("state missing required field version")
	}
	if raw.TaskInfo == nil {
		return State{}, errors.New("state missing required field task_info")
	}
	if raw.TaskInfo.ExportID == nil {
		return State{}, errors.New("state missing required field task_info.export_id")
	}
	if raw.TaskInfo.ChangefeedID == nil {
		return State{}, errors.New("state missing required field task_info.changefeed_id")
	}
	if raw.Snapshot == nil {
		return State{}, errors.New("state missing required field snapshot")
	}
	if raw.Snapshot.TSO == nil {
		return State{}, errors.New("state missing required field snapshot.tso")
	}
	if raw.Snapshot.Finished == nil {
		return State{}, errors.New("state missing required field snapshot.finished")
	}
	if raw.Incremental == nil {
		return State{}, errors.New("state missing required field incremental")
	}
	if raw.Incremental.Tables == nil {
		return State{}, errors.New("state missing required field incremental.tables")
	}

	st := State{
		Version: *raw.Version,
		TaskInfo: TaskInfo{
			ExportID:     *raw.TaskInfo.ExportID,
			ChangefeedID: *raw.TaskInfo.ChangefeedID,
		},
		Snapshot: SnapshotState{
			TSO:      *raw.Snapshot.TSO,
			Finished: *raw.Snapshot.Finished,
		},
		Incremental: IncrementalState{
			Tables: make(map[string]TableState, len(*raw.Incremental.Tables)),
		},
	}
	for table, tableState := range *raw.Incremental.Tables {
		if tableState.DMLFileWatermarks == nil {
			return State{}, errors.Errorf("state missing required field incremental.tables.%s.dml_file_watermarks", table)
		}
		if tableState.DDLTableVersionWatermark == nil {
			return State{}, errors.Errorf("state missing required field incremental.tables.%s.ddl_table_version_watermark", table)
		}
		watermarks := make(map[string]uint64, len(*tableState.DMLFileWatermarks))
		for scope, idx := range *tableState.DMLFileWatermarks {
			watermarks[scope] = idx
		}
		st.Incremental.Tables[table] = TableState{
			DMLFileWatermarks:        watermarks,
			DDLTableVersionWatermark: *tableState.DDLTableVersionWatermark,
		}
	}
	if err := validateState(st, nil); err != nil {
		return State{}, err
	}
	return st, nil
}
