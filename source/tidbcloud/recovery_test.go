package tidbcloud

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	cloudapi "github.com/tidbcloud/tidb2snowflake/pkg/tidbcloud"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
)

type handlerTransport func(*http.Request) (*http.Response, error)

func (f handlerTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func recoveryRunner(t *testing.T, handler func(*http.Request) (int, string)) *Runner {
	t.Helper()
	ctx := context.Background()
	store, err := storage.New(ctx, &url.URL{Scheme: "file", Path: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(store.Close)
	cfg := baseConfig()
	cfg.ClusterID = "10"
	cfg.Credential = testCred()
	cfg.SnapshotTSO = "466924115091783691"
	cfg.ChangefeedRCU = 4
	runner := NewRunner(cfg, store, newTestStateManager(t, ctx, store))
	runner.client, err = cloudapi.NewClient("public", "private", cloudapi.WithMaxRetries(0), cloudapi.WithBaseTransport(handlerTransport(func(r *http.Request) (*http.Response, error) {
		code, body := 200, `{"clusterId":"10","servicePlan":"Essential","state":"ACTIVE","region":{"name":"regions/aws-us-west-2"}}`
		if r.URL.Path != "/v1beta1/clusters/10" {
			code, body = handler(r)
		}
		return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
	})))
	require.NoError(t, err)
	return runner
}

func TestServerlessExportResponseOmitsDialectAndEscaping(t *testing.T) {
	runner := recoveryRunner(t, nil)
	require.NoError(t, runner.ensureBackend(context.Background()))
	request := runner.exportSpec().Request
	raw, err := json.Marshal(cloudapi.Export{ExportOptions: request.ExportOptions, Target: request.Target})
	require.NoError(t, err)
	var job cloudapi.Export
	require.NoError(t, json.Unmarshal(raw, &job))
	job.ExportOptions.CSVFormat.Dialect = "UNSPECIFIED"
	job.ExportOptions.EscapeBackslash = nil
	require.NoError(t, runner.validateExport(&job))
	// Validation must not rewrite the API response.
	require.Equal(t, cloudapi.ExportCSVDialect("UNSPECIFIED"), job.ExportOptions.CSVFormat.Dialect)
	job.ExportOptions.CSVFormat.Separator = "|"
	require.Error(t, runner.validateExport(&job))
	job.ExportOptions.CSVFormat.Separator = ","
	job.ExportOptions.CSVFormat.Dialect = cloudapi.ExportCSVDialectBigQuery
	require.Error(t, runner.validateExport(&job))
	job.ExportOptions.CSVFormat.Dialect = "UNSPECIFIED"
	escape := true
	job.ExportOptions.EscapeBackslash = &escape
	require.Error(t, runner.validateExport(&job))
	job.ExportOptions.EscapeBackslash = nil
	runner.backend.Cluster.Family = cloudapi.Premium
	require.Error(t, runner.validateExport(&job))
}

func TestUnknownCreateIsPersistedAndNeverReposted(t *testing.T) {
	posts := 0
	runner := recoveryRunner(t, func(r *http.Request) (int, string) {
		switch r.URL.Path {
		case "/v1beta1/clusters/10/changefeeds:listSpecifications":
			return 200, `{"items":[{"rcu":4}]}`
		default:
			posts++
			return 503, `{"code":13,"message":"backend unavailable"}`
		}
	})
	_, _, err := runner.createChangefeed(context.Background())
	require.Error(t, err)
	require.Equal(t, 1, posts)
	require.Equal(t, "changefeed", runner.state.Snapshot().PendingCreate.Kind)
	reopened, err := state.OpenExisting(context.Background(), runner.store)
	require.NoError(t, err)
	restarted := NewRunner(runner.cfg, runner.store, reopened)
	err = restarted.Preflight(context.Background(), true, true)
	require.ErrorContains(t, err, "outcome unknown")
	require.Equal(t, 1, posts)
}
func TestSuccessfulCreateCommitsIdentityAndClearsPending(t *testing.T) {
	runner := recoveryRunner(t, func(r *http.Request) (int, string) {
		switch r.URL.Path {
		case "/v1beta1/clusters/10/changefeeds:listSpecifications":
			return 200, `{"items":[{"rcu":4}]}`
		default:
			return 200, `{"changefeedId":"cf-1","clusterId":"10","state":"CREATING"}`
		}
	})
	id, tso, err := runner.createChangefeed(context.Background())
	require.NoError(t, err)
	require.Equal(t, "cf-1", id)
	require.Equal(t, uint64(466924115091783691), tso)
	st := runner.state.Snapshot()
	require.NotNil(t, st.SourceIdentity)
	require.Equal(t, cloudapi.Serverless, st.SourceIdentity.Family)
	require.Nil(t, st.PendingCreate)
	require.Equal(t, "cf-1", st.TaskInfo.ChangefeedID)
}
func TestMetadataCannotHideRunningOrFailedExport(t *testing.T) {
	gets := 0
	runner := recoveryRunner(t, func(r *http.Request) (int, string) {
		gets++
		return 200, `{}`
	})
	require.NoError(t, runner.store.WriteFile(context.Background(), "snapshot/metadata", []byte("Pos: 466924115091783691\n")))
	require.NoError(t, runner.ensureBackend(context.Background()))
	require.NoError(t, runner.state.SetExportID(context.Background(), "export-1"))
	client, err := cloudapi.NewClient("public", "private", cloudapi.WithBaseTransport(handlerTransport(func(r *http.Request) (*http.Response, error) {
		gets++
		want := runner.exportSpec().Request
		exp := cloudapi.Export{ExportID: "export-1", ClusterID: "10", State: cloudapi.ExportStateFailed, ExportOptions: want.ExportOptions, Target: want.Target}
		raw, e := json.Marshal(exp)
		require.NoError(t, e)
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(string(raw))), Request: r}, nil
	})))
	require.NoError(t, err)
	runner.backend, err = client.BoundBackend("10", *runner.state.Snapshot().SourceIdentity)
	require.NoError(t, err)
	runner.preflightDone = true
	require.ErrorContains(t, runner.EnsureSnapshot(context.Background()), "terminal state FAILED")
	require.Equal(t, 1, gets)
}
func TestMissingStartPointDoesNotCreate(t *testing.T) {
	posts := 0
	runner := recoveryRunner(t, func(r *http.Request) (int, string) {
		if r.Method == "POST" {
			posts++
		}
		return 200, `{"items":[{"rcu":4}]}`
	})
	runner.cfg.SnapshotTSO = ""
	_, _, err := runner.createChangefeed(context.Background())
	require.ErrorContains(t, err, "nonzero")
	require.Equal(t, 0, posts)
	require.Nil(t, runner.state.Snapshot().PendingCreate)
}
func TestMetadataTSOConflictAndMissingPosReturnErrors(t *testing.T) {
	runner := recoveryRunner(t, func(r *http.Request) (int, string) { t.Fatal("unexpected API call"); return 500, "" })
	require.NoError(t, runner.store.WriteFile(context.Background(), "snapshot/metadata", []byte("Pos: 449\n")))
	_, err := runner.snapshotTSO(context.Background(), "")
	require.ErrorContains(t, err, "does not match")
	require.NoError(t, runner.store.WriteFile(context.Background(), "snapshot/metadata", []byte("missing position\n")))
	require.NotPanics(t, func() { _, err = runner.snapshotTSO(context.Background(), "") })
	require.Error(t, err)
}
func TestPendingCreationSurvivesStateWriteFailure(t *testing.T) {
	runner := recoveryRunner(t, func(r *http.Request) (int, string) { return 200, `{"items":[{"rcu":4}]}` })
	require.NoError(t, runner.ensureBackend(context.Background()))
	require.NoError(t, runner.state.BeginCreate(context.Background(), "export", "test-export"))
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	// Some local storage backends ignore cancellation. Use a deterministic failing
	// WriteFile wrapper to exercise the state transaction's failure boundary.
	failing := &failingStore{Storage: runner.store, fail: true}
	manager, err := state.OpenExisting(context.Background(), failing)
	require.NoError(t, err)
	require.Error(t, manager.SetExportID(canceled, "committed-on-server"))
	require.NotNil(t, manager.Snapshot().PendingCreate)
	require.Empty(t, manager.Snapshot().TaskInfo.ExportID)
}

type failingStore struct {
	*storage.Storage
	fail bool
}

func (s *failingStore) WriteFile(ctx context.Context, key string, data []byte) error {
	if s.fail {
		return errors.New("injected state write failure")
	}
	return s.Storage.WriteFile(ctx, key, data)
}

func TestSavedChangefeedCannotStartAfterCheckpoint(t *testing.T) {
	runner := recoveryRunner(t, func(r *http.Request) (int, string) { return 500, "" })
	require.NoError(t, runner.state.MarkSnapshotFinished(context.Background(), 100))
	cf := &cloudapi.Changefeed{Filter: &cloudapi.ChangefeedFilter{FilterRule: runner.cfg.Tables, Mode: cloudapi.TableModeForceSync}, Sink: buildChangefeedRequest(runner.cfg, runner.store.CleanSubURI("increment"), testCred(), "101").Sink, StartPosition: &cloudapi.StartPosition{Mode: cloudapi.StartModeFromTSO, TSO: "101"}}
	require.ErrorContains(t, runner.validateChangefeed(context.Background(), cf), "after the downstream checkpoint")
	// Waiting helpers honor caller cancellation without sleeping for a full poll.
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	require.Error(t, cloudapi.WaitPoll(ctx))
}
