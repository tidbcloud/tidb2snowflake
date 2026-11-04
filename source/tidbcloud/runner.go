package tidbcloud

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"path"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/tidbcloud/tidb2snowflake/pkg/dumpling"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	"github.com/tidbcloud/tidb2snowflake/pkg/table"
	"github.com/tidbcloud/tidb2snowflake/pkg/tidbcloud"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
	"go.uber.org/zap"
)

type Config struct {
	ClusterID, PublicKey, PrivateKey, Host string
	ExportUser, ExportPassword             string
	Tables                                 []string
	ChangefeedFlushInterval                time.Duration
	ChangefeedFileSizeMiB, ChangefeedRCU   int
	SnapshotCompression                    tidbcloud.ExportCompression
	SnapshotTSO                            string
	ColumnSelectors                        []tidbcloud.ColumnSelector
	Credential                             *aws.Credentials
}
type Runner struct {
	cfg               Config
	client            *tidbcloud.Client
	backend           *tidbcloud.Backend
	store             *storage.Storage
	state             state.Manager
	preflightDone     bool
	capacitiesChecked bool
}

func NewRunner(cfg Config, store *storage.Storage, manager state.Manager) *Runner {
	if cfg.ChangefeedRCU == 0 {
		cfg.ChangefeedRCU = 2
	}
	return &Runner{cfg: cfg, store: store, state: manager}
}

// Preflight checks both requested phases before the first cloud mutation. API
// credentials remain lazy for callers that only load externally prepared files.
func (r *Runner) Preflight(ctx context.Context, snapshot, incremental bool) error {
	if pending := r.state.Snapshot().PendingCreate; pending != nil {
		return errors.Errorf("creation outcome unknown for %s %q; verify the task in Console/API before repairing pending_create", pending.Kind, pending.Name)
	}
	st := r.state.Snapshot()
	metadata, err := r.store.FileExists(ctx, path.Join(storage.SnapshotDirName, "metadata"))
	if err != nil {
		return err
	}
	needsExport := snapshot && !st.SnapshotFinished && (st.TaskInfo.ExportID != "" || !metadata)
	if !needsExport && !incremental {
		r.preflightDone = true
		return nil
	}
	if err = r.ensureBackend(ctx); err != nil {
		return err
	}
	createExport := needsExport && st.TaskInfo.ExportID == ""
	createCF := incremental && st.TaskInfo.ChangefeedID == ""
	if createExport || createCF {
		region := ""
		if createCF && r.backend.Cluster.Family == tidbcloud.Premium {
			region = r.backend.Cluster.Region
		}
		if err = r.store.ValidateManagedTarget(region); err != nil {
			return err
		}
	}
	if createExport {
		if err = r.backend.ValidateExport(r.exportSpec()); err != nil {
			return err
		}
	}
	if createCF {
		if err = r.backend.ValidateChangefeed(r.cfg.ChangefeedFlushInterval, r.cfg.ChangefeedFileSizeMiB); err != nil {
			return err
		}
		if err = r.validateChangefeedRCU(ctx); err != nil {
			return err
		}
		// A new or resumed export will produce the missing metadata before CDC
		// creation. Otherwise require a start point before any source mutation.
		if !needsExport {
			if _, err = r.changefeedStartTSO(ctx); err != nil {
				return err
			}
		}
	}
	r.preflightDone = true
	return nil
}

func (r *Runner) ensureBackend(ctx context.Context) error {
	if r.backend != nil {
		return nil
	}
	client, err := r.tidbCloudClient()
	if err != nil {
		return err
	}
	st := r.state.Snapshot()
	legacy := st.SourceIdentity == nil && (st.TaskInfo.ExportID != "" || st.TaskInfo.ChangefeedID != "")
	b, err := client.OpenBackend(ctx, r.cfg.ClusterID, st.SourceIdentity, legacy)
	if err != nil {
		return err
	}
	r.backend = b
	ready := false
	defer func() {
		if !ready {
			r.backend = nil
		}
	}()
	r.cfg.ClusterID = b.Cluster.ClusterID
	// Before upgrading v1, establish resource ownership and the actual output
	// contract, rather than interpreting legacy IDs through a different API.
	if legacy {
		if st.TaskInfo.ExportID != "" && !st.SnapshotFinished {
			job, e := b.GetExport(ctx, st.TaskInfo.ExportID)
			if e != nil {
				return e
			}
			if e = r.validateExport(job); e != nil {
				return e
			}
		}
		if st.TaskInfo.ChangefeedID != "" {
			job, e := b.GetChangefeed(ctx, st.TaskInfo.ChangefeedID)
			if e != nil {
				return e
			}
			if e = r.validateChangefeed(ctx, job); e != nil {
				return e
			}
		}
		if st.SnapshotFinished && st.TaskInfo.ChangefeedID == "" && st.TaskInfo.ExportID != "" {
			job, e := b.GetExport(ctx, st.TaskInfo.ExportID)
			if e != nil {
				return e
			}
			if e = r.validateExport(job); e != nil {
				return e
			}
		}
	}
	if err = r.state.BindCloudSource(ctx, b.Cluster); err != nil {
		r.backend = nil
		return err
	}
	ready = true
	log.Info("TiDB Cloud source resolved", zap.String("servicePlan", b.Cluster.ServicePlan), zap.String("apiFamily", string(b.Cluster.Family)))
	return nil
}
func (r *Runner) exportSpec() tidbcloud.ExportSpec {
	return tidbcloud.ExportSpec{Request: buildExportRequest(r.cfg, r.store.CleanSubURI(storage.SnapshotDirName), r.cfg.Credential, r.cfg.SnapshotTSO), User: r.cfg.ExportUser, Password: r.cfg.ExportPassword}
}

func (r *Runner) EnsureSnapshot(ctx context.Context) error {
	if !r.preflightDone {
		if err := r.Preflight(ctx, true, false); err != nil {
			return err
		}
	}
	st := r.state.Snapshot()
	if st.SnapshotFinished && st.CheckpointTS != 0 {
		return nil
	}
	// A saved export takes precedence over metadata that may appear before the
	// exporter has finished uploading all data files.
	if id := st.TaskInfo.ExportID; id != "" {
		_, _, err := r.waitExport(ctx, id)
		return err
	}
	exists, err := r.store.FileExists(ctx, path.Join(storage.SnapshotDirName, "metadata"))
	if err != nil {
		return err
	}
	if exists {
		_, err = r.snapshotTSO(ctx, "")
		return err
	}
	partial, err := r.store.DirHasObjects(ctx, storage.SnapshotDirName)
	if err != nil {
		return err
	}
	if partial {
		return errors.New("snapshot directory is incomplete: metadata missing")
	}
	id, _, err := r.createExport(ctx)
	if err != nil {
		return err
	}
	_, _, err = r.waitExport(ctx, id)
	return err
}
func (r *Runner) EnsureChangefeed(ctx context.Context) error {
	if !r.preflightDone {
		if err := r.Preflight(ctx, false, true); err != nil {
			return err
		}
	}
	if err := r.ensureBackend(ctx); err != nil {
		return err
	}
	if err := r.checkStoredGeneratedColumns(ctx); err != nil {
		return err
	}
	id := r.state.Snapshot().TaskInfo.ChangefeedID
	if id == "" {
		var err error
		id, _, err = r.createChangefeed(ctx)
		if err != nil {
			return err
		}
	}
	return r.getChangefeed(ctx, id)
}
func (r *Runner) beginCreate(ctx context.Context, kind string) (string, error) {
	var token [12]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	suffix := "increment"
	if kind == "export" {
		suffix = "snapshot"
	}
	name := "tidb2snowflake-" + hex.EncodeToString(token[:]) + "-" + suffix
	return name, r.state.BeginCreate(ctx, kind, name)
}
func (r *Runner) createExport(ctx context.Context) (string, string, error) {
	if err := r.ensureBackend(ctx); err != nil {
		return "", "", err
	}
	spec := r.exportSpec()
	if err := r.backend.ValidateExport(spec); err != nil {
		return "", "", err
	}
	name, err := r.beginCreate(ctx, "export")
	if err != nil {
		return "", "", err
	}
	spec.Request.DisplayName = name
	job, err := r.backend.CreateExport(ctx, spec)
	if err != nil {
		r.rejectedCreate(ctx, err)
		return "", "", err
	}
	if err = r.state.SetExportID(ctx, job.ExportID); err != nil {
		return "", "", err
	}
	return job.ExportID, job.SnapshotTSO, nil
}
func (r *Runner) rejectedCreate(ctx context.Context, err error) {
	if tidbcloud.DefinitelyRejected(err) {
		if e := r.state.ClearPendingCreate(ctx); e != nil {
			log.Error("failed to clear rejected creation marker", zap.Error(e))
		}
	}
}
func (r *Runner) waitExport(ctx context.Context, id string) (string, string, error) {
	if err := r.ensureBackend(ctx); err != nil {
		return "", "", err
	}
	for {
		job, err := r.backend.GetExport(ctx, id)
		if err != nil {
			return "", "", err
		}
		if err = r.validateExport(job); err != nil {
			return "", "", err
		}
		switch job.State {
		case tidbcloud.ExportStateSucceeded:
			tso, err := r.snapshotTSO(ctx, job.SnapshotTSO)
			return id, strconv.FormatUint(tso, 10), err
		case tidbcloud.ExportStateRunning:
		default:
			return "", "", errors.Errorf("export ended in unexpected/terminal state %s: %s", job.State, job.Reason)
		}
		if err = tidbcloud.WaitPoll(ctx); err != nil {
			return "", "", err
		}
	}
}
func (r *Runner) snapshotTSO(ctx context.Context, apiTSO string) (uint64, error) {
	tso, err := dumpling.LoadTSOFromMetadata(ctx, r.store)
	if err != nil {
		return 0, err
	}
	for _, expected := range []string{apiTSO, r.cfg.SnapshotTSO} {
		if expected != "" && expected != "0" {
			n, e := strconv.ParseUint(expected, 10, 64)
			if e != nil || n != tso {
				return 0, errors.New("snapshot metadata TSO does not match export/configured TSO")
			}
		}
	}
	return tso, nil
}
func (r *Runner) createChangefeed(ctx context.Context) (string, uint64, error) {
	if err := r.ensureBackend(ctx); err != nil {
		return "", 0, err
	}
	if err := r.validateChangefeedRCU(ctx); err != nil {
		return "", 0, err
	}
	tso, err := r.changefeedStartTSO(ctx)
	if err != nil {
		return "", 0, err
	}
	req := buildChangefeedRequest(r.cfg, r.store.CleanSubURI(storage.IncrementDirName), r.cfg.Credential, strconv.FormatUint(tso, 10))
	name, err := r.beginCreate(ctx, "changefeed")
	if err != nil {
		return "", 0, err
	}
	req.DisplayName = name
	job, err := r.backend.CreateChangefeed(ctx, req)
	if err != nil {
		r.rejectedCreate(ctx, err)
		return "", 0, err
	}
	if err = r.state.SetChangefeedID(ctx, job.ChangefeedID); err != nil {
		return "", 0, err
	}
	return job.ChangefeedID, tso, nil
}
func (r *Runner) validateChangefeedRCU(ctx context.Context) error {
	if r.capacitiesChecked {
		return nil
	}
	specs, err := r.backend.Capacities(ctx)
	if err != nil {
		return err
	}
	for _, n := range specs {
		if n == r.cfg.ChangefeedRCU {
			r.capacitiesChecked = true
			return nil
		}
	}
	sort.Ints(specs)
	return errors.Errorf("changefeed.rcu %d is not available; available values: %s", r.cfg.ChangefeedRCU, joinInts(specs))
}
func joinInts(values []int) string {
	out := make([]string, len(values))
	for i, n := range values {
		out[i] = strconv.Itoa(n)
	}
	return strings.Join(out, ", ")
}
func (r *Runner) changefeedStartTSO(ctx context.Context) (uint64, error) {
	if n := r.state.Snapshot().CheckpointTS; n != 0 {
		return n, nil
	}
	exists, err := r.store.FileExists(ctx, path.Join(storage.SnapshotDirName, "metadata"))
	if err != nil {
		return 0, err
	}
	if exists {
		return r.snapshotTSO(ctx, "")
	}
	if r.cfg.SnapshotTSO != "" {
		n, err := strconv.ParseUint(r.cfg.SnapshotTSO, 10, 64)
		if err == nil && n > 0 {
			return n, nil
		}
	}
	return 0, errors.New("changefeed requires a nonzero checkpoint, snapshot metadata TSO, or explicit snapshot.tso; FROM_TSO=0 is not allowed")
}
func (r *Runner) getChangefeed(ctx context.Context, id string) error {
	for {
		cf, err := r.backend.GetChangefeed(ctx, id)
		if err != nil {
			return err
		}
		if err = r.validateChangefeed(ctx, cf); err != nil {
			return err
		}
		switch string(cf.State) {
		case "RUNNING", "WARNING":
			return nil
		case "CREATING", "SCALING":
		case "EDITING", "RESUMING", "PAUSING":
			if r.backend.Cluster.Family != tidbcloud.Premium {
				return errors.Errorf("unknown Serverless changefeed state %s", cf.State)
			}
		default:
			return errors.Errorf("changefeed is not runnable: %s", cf.State)
		}
		if err = tidbcloud.WaitPoll(ctx); err != nil {
			return err
		}
	}
}
func (r *Runner) validateExport(job *tidbcloud.Export) error {
	want := r.exportSpec().Request
	if job.Target == nil || job.Target.Type != tidbcloud.ExportTargetTypeS3 || job.Target.S3 == nil || job.Target.S3.URI != want.Target.S3.URI {
		return errors.New("saved export storage does not match configured snapshot prefix")
	}
	got := job.ExportOptions
	if got == nil || got.FileType != want.ExportOptions.FileType || got.Compression != want.ExportOptions.Compression || !reflect.DeepEqual(got.Filter, want.ExportOptions.Filter) || !sameSelectors(got.ColumnSelectors, want.ExportOptions.ColumnSelectors) || !r.sameExportFormat(got, want.ExportOptions) {
		return errors.New("saved export format/filter does not match configuration")
	}
	return nil
}

func (r *Runner) sameExportFormat(got, want *tidbcloud.ExportOptions) bool {
	// Serverless accepts these options but its response converter does not echo
	// dialect or escapeBackslash. Compare every field it does return, and still
	// reject explicit conflicting values. Premium returns both fields.
	csv := got.CSVFormat
	escape := got.EscapeBackslash
	if r.backend.Cluster.Family == tidbcloud.Serverless {
		if csv != nil && (csv.Dialect == "" || csv.Dialect == "UNSPECIFIED") {
			copy := *csv
			copy.Dialect = want.CSVFormat.Dialect
			csv = &copy
		}
		if escape == nil {
			escape = want.EscapeBackslash
		}
	}
	return reflect.DeepEqual(csv, want.CSVFormat) && reflect.DeepEqual(escape, want.EscapeBackslash)
}
func (r *Runner) validateChangefeed(ctx context.Context, cf *tidbcloud.Changefeed) error {
	if cf.Sink == nil || cf.Sink.Type != tidbcloud.ChangefeedTypeCloudStorage || cf.Sink.CloudStorage == nil {
		return errors.New("saved changefeed is not a cloud storage sink")
	}
	cs := cf.Sink.CloudStorage
	if cs.Storage == nil || cs.Storage.Type != tidbcloud.CloudStorageTypeS3 || cs.Storage.S3 == nil || cs.Storage.S3.URI != r.store.CleanSubURI(storage.IncrementDirName) {
		return errors.New("saved changefeed storage does not match configured increment prefix")
	}
	if cf.Filter == nil || !slices.Equal(cf.Filter.FilterRule, r.cfg.Tables) || cf.Filter.Mode != tidbcloud.TableModeForceSync || !sameSelectors(cs.ColumnSelectors, r.cfg.ColumnSelectors) {
		return errors.New("saved changefeed filters do not match configuration")
	}
	if cs.DateSeparator != tidbcloud.DateSeparatorDay || cs.DataFormat == nil || cs.DataFormat.Protocol != tidbcloud.CloudStorageProtocolCSV || cs.DataFormat.CSVConfig == nil || !cs.DataFormat.CSVConfig.IncludeCommitTs || cs.DataFormat.CSVConfig.Encoding != tidbcloud.BinaryEncodingHex {
		return errors.New("saved changefeed CSV contract does not match loader")
	}
	cfg := cs.DataFormat.CSVConfig
	if (cfg.Delimiter != "" && cfg.Delimiter != ",") || (cfg.Quote != "" && cfg.Quote != "\"") || (cfg.NullValue != "" && cfg.NullValue != "\\N") {
		return errors.New("saved changefeed CSV delimiters do not match loader")
	}
	if cf.StartPosition == nil || cf.StartPosition.Mode != tidbcloud.StartModeFromTSO {
		return errors.New("saved changefeed is not pinned to a snapshot TSO")
	}
	start, err := strconv.ParseUint(cf.StartPosition.TSO, 10, 64)
	if err != nil || start == 0 {
		return errors.New("saved changefeed has no valid start TSO")
	}
	checkpoint := r.state.Snapshot().CheckpointTS
	if checkpoint == 0 {
		exists, e := r.store.FileExists(ctx, path.Join(storage.SnapshotDirName, "metadata"))
		if e != nil {
			return e
		}
		if exists {
			checkpoint, e = r.snapshotTSO(ctx, "")
			if e != nil {
				return e
			}
		} else if r.cfg.SnapshotTSO != "" {
			checkpoint, e = strconv.ParseUint(r.cfg.SnapshotTSO, 10, 64)
			if e != nil {
				return e
			}
		}
	}
	if checkpoint != 0 && start > checkpoint {
		return errors.New("saved changefeed starts after the downstream checkpoint/snapshot TSO")
	}
	return nil
}
func (r *Runner) checkStoredGeneratedColumns(ctx context.Context) error {
	for _, name := range r.cfg.Tables {
		db, tbl, ok := strings.Cut(name, ".")
		if !ok {
			return errors.Errorf("invalid table %q", name)
		}
		key := path.Join(storage.SnapshotDirName, table.SchemaFilePath(db, tbl))
		exists, err := r.store.FileExists(ctx, key)
		if err != nil {
			return err
		}
		if !exists {
			continue // incremental-only may obtain its first schema from TiCDC files
		}
		ddl, err := r.store.ReadFile(ctx, key)
		if err != nil {
			return err
		}
		if err = table.ValidateIncrementalSchema(string(ddl)); err != nil {
			return errors.Annotatef(err, "table %s", name)
		}
	}
	return nil
}
func (r *Runner) tidbCloudClient() (*tidbcloud.Client, error) {
	if r.client != nil {
		return r.client, nil
	}
	if err := validateTiDBCloudConfig(r.cfg); err != nil {
		return nil, err
	}
	c, err := tidbcloud.NewClient(r.cfg.PublicKey, r.cfg.PrivateKey, tidbcloud.WithHost(r.cfg.Host))
	if err != nil {
		return nil, err
	}
	r.client = c
	return c, nil
}
func validateTiDBCloudConfig(cfg Config) error {
	var missing []string
	if cfg.ClusterID == "" {
		missing = append(missing, "tidbcloud.cluster-id")
	}
	if cfg.PublicKey == "" {
		missing = append(missing, "tidbcloud.public-key")
	}
	if cfg.PrivateKey == "" {
		missing = append(missing, "tidbcloud.private-key")
	}
	if len(missing) > 0 {
		return errors.Errorf("missing TiDB Cloud API config option(s): %s", strings.Join(missing, ", "))
	}
	return nil
}

func buildChangefeedRequest(cfg Config, cleanIncrementURI string, cred *aws.Credentials, snapshotTSO string) *tidbcloud.CreateChangefeedRequest {
	return &tidbcloud.CreateChangefeedRequest{
		FlushInterval: cfg.ChangefeedFlushInterval,
		DisplayName:   "tidb2snowflake-" + strconv.FormatInt(time.Now().UnixMilli(), 10),
		Sink: &tidbcloud.Sink{
			Type: tidbcloud.ChangefeedTypeCloudStorage,
			CloudStorage: &tidbcloud.CloudStorageSink{
				Storage: &tidbcloud.CloudStorage{
					Type: tidbcloud.CloudStorageTypeS3,
					S3: &tidbcloud.S3CloudStorage{
						URI:       cleanIncrementURI,
						AuthType:  tidbcloud.S3AuthTypeAccessKey,
						AccessKey: &tidbcloud.S3AccessKey{ID: cred.AccessKeyID, Secret: cred.SecretAccessKey},
					},
				},
				DataFormat: &tidbcloud.CloudStorageDataFormat{
					Protocol: tidbcloud.CloudStorageProtocolCSV,
					CSVConfig: &tidbcloud.CSVConfig{
						IncludeCommitTs: true,
						Encoding:        tidbcloud.BinaryEncodingHex,
					},
				},
				DateSeparator:     tidbcloud.DateSeparatorDay,
				IntervalInSeconds: int(cfg.ChangefeedFlushInterval.Seconds()),
				SizeInMiB:         cfg.ChangefeedFileSizeMiB,
				ColumnSelectors:   cfg.ColumnSelectors,
			},
		},
		Filter:        &tidbcloud.ChangefeedFilter{FilterRule: cfg.Tables, Mode: tidbcloud.TableModeForceSync},
		StartPosition: &tidbcloud.StartPosition{Mode: tidbcloud.StartModeFromTSO, TSO: snapshotTSO},
		RCU:           cfg.ChangefeedRCU,
	}
}

func buildExportRequest(cfg Config, cleanSnapshotURI string, cred *aws.Credentials, snapshotTSO string) *tidbcloud.CreateExportRequest {
	req := &tidbcloud.CreateExportRequest{
		DisplayName: "tidb2snowflake-snapshot",
		ExportOptions: &tidbcloud.ExportOptions{
			FileType:        tidbcloud.ExportFileTypeCSV,
			Compression:     cfg.SnapshotCompression,
			EscapeBackslash: util.AddressOf(false),
			Filter:          &tidbcloud.ExportFilter{Table: &tidbcloud.ExportFilterTable{Patterns: cfg.Tables}},
			CSVFormat: &tidbcloud.ExportCSVFormat{
				Separator:  ",",
				Delimiter:  util.AddressOf("\""),
				NullValue:  util.AddressOf("\\N"),
				SkipHeader: true,
				Dialect:    tidbcloud.ExportCSVDialectSnowflake,
			},
			ColumnSelectors: cfg.ColumnSelectors,
		},
		Target: &tidbcloud.ExportTarget{
			Type: tidbcloud.ExportTargetTypeS3,
			S3: &tidbcloud.S3Target{
				URI:       cleanSnapshotURI,
				AuthType:  tidbcloud.S3AuthTypeAccessKey,
				AccessKey: &tidbcloud.S3AccessKey{ID: cred.AccessKeyID, Secret: cred.SecretAccessKey},
			},
		},
	}
	if snapshotTSO != "" {
		req.ExportOptions.SnapshotTSO = snapshotTSO
	}
	return req
}

func sameSelectors(a, b []tidbcloud.ColumnSelector) bool {
	return slices.EqualFunc(a, b, func(a, b tidbcloud.ColumnSelector) bool {
		return slices.Equal(a.Matcher, b.Matcher) && slices.Equal(a.Columns, b.Columns)
	})
}
