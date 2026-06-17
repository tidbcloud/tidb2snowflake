package tidbcloud

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCreateExport_RequestAndResponse(t *testing.T) {
	var gotMethod, gotPath, gotCT string
	var gotBody CreateExportRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath, gotCT = r.Method, r.URL.Path, r.Header.Get("Content-Type")
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotBody))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"exportId":"exp-9","state":"RUNNING","snapshotTso":"449023000000000000"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	req := &CreateExportRequest{
		DisplayName: "snap",
		ExportOptions: &ExportOptions{
			FileType:    ExportFileTypeCSV,
			Compression: ExportCompressionNone,
			SnapshotTSO: "449023000000000000",
			Filter:      &ExportFilter{Table: &ExportFilterTable{Patterns: []string{"db1.t1"}}},
		},
		Target: &ExportTarget{
			Type: ExportTargetTypeS3,
			S3: &S3Target{
				URI:       "s3://bucket/snapshot",
				AuthType:  S3AuthTypeAccessKey,
				AccessKey: &S3AccessKey{ID: "AKIA", Secret: "secret"},
			},
		},
	}
	exp, err := c.CreateExport(context.Background(), "10", req)
	require.NoError(t, err)

	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "/v1beta1/clusters/10/exports", gotPath)
	require.Equal(t, "application/json", gotCT)
	require.Equal(t, "449023000000000000", gotBody.ExportOptions.SnapshotTSO)
	require.Equal(t, ExportFileTypeCSV, gotBody.ExportOptions.FileType)
	require.Equal(t, []string{"db1.t1"}, gotBody.ExportOptions.Filter.Table.Patterns)
	require.Equal(t, ExportTargetTypeS3, gotBody.Target.Type)
	require.Equal(t, "s3://bucket/snapshot", gotBody.Target.S3.URI)
	require.Equal(t, "AKIA", gotBody.Target.S3.AccessKey.ID)

	require.Equal(t, "exp-9", exp.ExportID)
	require.Equal(t, "449023000000000000", exp.SnapshotTSO)
}

func TestWaitExport_SucceedsAfterRunning(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if atomic.AddInt32(&calls, 1) < 2 {
			_, _ = w.Write([]byte(`{"exportId":"exp-9","state":"RUNNING"}`))
			return
		}
		_, _ = w.Write([]byte(`{"exportId":"exp-9","state":"SUCCEEDED","snapshotTso":"449"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	exp, err := c.WaitExport(context.Background(), "10", "exp-9", 10*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, ExportStateSucceeded, exp.State)
	require.Equal(t, "449", exp.SnapshotTSO)
}

func TestWaitExport_FailsOnFailedState(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"exportId":"exp-9","state":"FAILED","reason":"gc safepoint exceeded"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	exp, err := c.WaitExport(context.Background(), "10", "exp-9", 10*time.Millisecond)
	require.Error(t, err)
	require.Equal(t, ExportStateFailed, exp.State)
	require.Contains(t, err.Error(), "gc safepoint exceeded")
}

func TestDeleteAndCancelExport_Paths(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"exportId":"exp-9","state":"CANCELED"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL)

	_, err := c.CancelExport(context.Background(), "10", "exp-9")
	require.NoError(t, err)
	require.Equal(t, http.MethodPost, gotMethod)
	require.Equal(t, "/v1beta1/clusters/10/exports/exp-9:cancel", gotPath)

	_, err = c.DeleteExport(context.Background(), "10", "exp-9")
	require.NoError(t, err)
	require.Equal(t, http.MethodDelete, gotMethod)
	require.Equal(t, "/v1beta1/clusters/10/exports/exp-9", gotPath)
}
