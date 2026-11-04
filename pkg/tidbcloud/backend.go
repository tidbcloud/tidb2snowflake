package tidbcloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// Backend presents the same job model for both public API families. Premium's
// wire DTOs are private and never used to marshal Serverless requests.
type Backend struct {
	client  *Client
	Cluster ClusterInfo
}
type ExportSpec struct {
	Request        *CreateExportRequest
	User, Password string
}

func (b *Backend) ValidateExport(spec ExportSpec) error {
	if b.Cluster.State != "ACTIVE" {
		return fmt.Errorf("cluster is not active: %s", b.Cluster.State)
	}
	if b.Cluster.Family == Premium {
		if spec.User == "" || spec.Password == "" {
			return fmt.Errorf("tidbcloud.export-user and tidbcloud.export-password are required to create a Premium export")
		}
		if spec.Request.ExportOptions.SnapshotTSO != "" {
			return fmt.Errorf("explicit snapshot.tso is not supported by the Premium public export contract")
		}
	}
	return nil
}
func (b *Backend) ValidateChangefeed(interval time.Duration, size int) error {
	if b.Cluster.State != "ACTIVE" {
		return fmt.Errorf("cluster is not active: %s", b.Cluster.State)
	}
	if b.Cluster.Family == Premium && (interval < 2*time.Second || interval > 10*time.Minute || size < 1 || size > 512) {
		return fmt.Errorf("Premium changefeed requires flush-interval in [2s,10m] and file-size in [1,512] MiB")
	}
	return nil
}
func (b *Backend) Capacities(ctx context.Context) ([]int, error) {
	if b.Cluster.Family == Serverless {
		r, err := b.client.ListChangefeedSpecifications(ctx, b.Cluster.ClusterID)
		if err != nil {
			return nil, err
		}
		out := make([]int, 0, len(r.Items))
		for _, s := range r.Items {
			out = append(out, s.RCU)
		}
		return out, nil
	}
	var out struct {
		CCUs []struct {
			Num  int    `json:"ccuNum"`
			Name string `json:"name"`
		} `json:"ccus"`
	}
	if err := b.client.do(ctx, http.MethodGet, "/v1beta2/changefeeds:listCCUs", nil, &out); err != nil {
		return nil, err
	}
	values := make([]int, 0, len(out.CCUs))
	for _, v := range out.CCUs {
		if v.Num <= 0 || v.Name != strconv.Itoa(v.Num)+"ccu" {
			return nil, fmt.Errorf("invalid changefeed capacity response")
		}
		values = append(values, v.Num)
	}
	return values, nil
}
func (b *Backend) CreateExport(ctx context.Context, spec ExportSpec) (*Export, error) {
	if err := b.ValidateExport(spec); err != nil {
		return nil, err
	}
	var out *Export
	var err error
	if b.Cluster.Family == Serverless {
		out, err = b.client.CreateExport(ctx, b.Cluster.ClusterID, spec.Request)
	} else {
		req := premiumExport{DisplayName: spec.Request.DisplayName, Options: premiumExportOptions{ExportOptions: *spec.Request.ExportOptions, IncludeGeneratedColumns: "NONE"}, Storage: &premiumStorage{Type: "S3", S3: spec.Request.Target.S3}, Username: spec.User, Password: spec.Password}
		var response premiumExport
		err = b.client.do(ctx, http.MethodPost, b.exportPath(""), &req, &response)
		out = response.normalized()
	}
	if err != nil {
		return nil, err
	}
	if out == nil || out.ExportID == "" {
		return nil, fmt.Errorf("create export returned no ID; creation outcome unknown")
	}
	if out.ClusterID == "" {
		verified, err := b.GetExport(ctx, out.ExportID)
		if err != nil {
			return nil, &UncertainCreateError{Err: err}
		}
		return verified, nil
	}
	if out.ClusterID != b.Cluster.ClusterID {
		return nil, fmt.Errorf("export belongs to another cluster")
	}
	out.Reason = sanitizeLogText(out.Reason)
	return out, nil
}
func (b *Backend) GetExport(ctx context.Context, id string) (*Export, error) {
	var out *Export
	var err error
	if b.Cluster.Family == Serverless {
		out, err = b.client.GetExport(ctx, b.Cluster.ClusterID, id)
	} else {
		var r premiumExport
		err = b.client.do(ctx, http.MethodGet, b.exportPath(id), nil, &r)
		if err == nil && r.Options.IncludeGeneratedColumns != "" && r.Options.IncludeGeneratedColumns != "NONE" && r.Options.IncludeGeneratedColumns != "GENERATED_COLUMNS_TYPE_UNSPECIFIED" {
			return nil, fmt.Errorf("saved export includes generated columns")
		}
		out = r.normalized()
	}
	if err != nil {
		return nil, err
	}
	if out.ExportID != id || out.ClusterID != b.Cluster.ClusterID {
		return nil, fmt.Errorf("export identity does not match saved cluster/job")
	}
	out.Reason = sanitizeLogText(out.Reason)
	return out, nil
}
func (b *Backend) exportPath(id string) string {
	p := "/v1beta2/tidbs/" + url.PathEscape(b.Cluster.ClusterID) + "/exports"
	if id != "" {
		p += "/" + url.PathEscape(id)
	}
	return p
}
func (b *Backend) CreateChangefeed(ctx context.Context, req *CreateChangefeedRequest) (*Changefeed, error) {
	var out *Changefeed
	var err error
	if b.Cluster.Family == Serverless {
		out, err = b.client.CreateChangefeed(ctx, b.Cluster.ClusterID, req)
	} else {
		cf := premiumChangefeedFrom(req, b.Cluster.ClusterID)
		var r premiumChangefeed
		err = b.client.do(ctx, http.MethodPost, "/v1beta2/changefeeds", struct {
			Changefeed premiumChangefeed `json:"changefeed"`
		}{cf}, &r)
		out = r.normalized()
	}
	if err != nil {
		return nil, err
	}
	if out == nil || out.ChangefeedID == "" {
		return nil, fmt.Errorf("create changefeed returned no ID; creation outcome unknown")
	}
	if out.ClusterID == "" {
		verified, err := b.GetChangefeed(ctx, out.ChangefeedID)
		if err != nil {
			return nil, &UncertainCreateError{Err: err}
		}
		return verified, nil
	}
	if out.ClusterID != b.Cluster.ClusterID {
		return nil, fmt.Errorf("changefeed belongs to another cluster")
	}
	return out, nil
}
func (b *Backend) GetChangefeed(ctx context.Context, id string) (*Changefeed, error) {
	var out *Changefeed
	var err error
	if b.Cluster.Family == Serverless {
		out, err = b.client.GetChangefeed(ctx, b.Cluster.ClusterID, id)
	} else {
		var r premiumChangefeed
		err = b.client.do(ctx, http.MethodGet, "/v1beta2/changefeeds/"+url.PathEscape(id), nil, &r)
		if err == nil && r.S3 != nil && (r.S3.Format.Raw || r.S3.Format.Header) {
			return nil, fmt.Errorf("saved changefeed enables unsupported raw/header CSV output")
		}
		out = r.normalized()
	}
	if err != nil {
		return nil, err
	}
	if out.ChangefeedID != id || out.ClusterID != b.Cluster.ClusterID {
		return nil, fmt.Errorf("changefeed identity does not match saved cluster/job")
	}
	return out, nil
}

// DeleteChangefeed verifies ownership before invoking the globally scoped API.
// trusted means the persisted ID was previously verified, so an initial genuine
// NotFound can safely be treated as already deleted.
func (b *Backend) DeleteChangefeed(ctx context.Context, id string, trusted bool) error {
	cf, err := b.GetChangefeed(ctx, id)
	if IsNotFound(err) && trusted {
		return nil
	}
	if err != nil {
		return err
	}
	if cf.State == ChangefeedStateDeleted {
		return nil
	}
	if b.Cluster.Family == Serverless {
		err = b.client.DeleteChangefeed(ctx, b.Cluster.ClusterID, id)
	} else {
		err = b.client.do(ctx, http.MethodDelete, "/v1beta2/changefeeds/"+url.PathEscape(id), nil, nil)
	}
	if err != nil && !IsNotFound(err) {
		return err
	}
	for {
		cf, err = b.GetChangefeed(ctx, id)
		if IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if cf.State == ChangefeedStateDeleted {
			return nil
		}
		if err = WaitPoll(ctx); err != nil {
			return err
		}
	}
}
func WaitPoll(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return nil
	}
}

// UncertainCreateError prevents a post-create verification failure from being
// mistaken for rejection of the original POST.
type UncertainCreateError struct{ Err error }

func (e *UncertainCreateError) Error() string {
	return "creation outcome unknown while verifying returned resource: " + e.Err.Error()
}
func (e *UncertainCreateError) Unwrap() error { return e.Err }

// DefinitelyRejected only clears a creation marker for a documented rejection.
func DefinitelyRejected(err error) bool {
	var unknown *UncertainCreateError
	if errors.As(err, &unknown) {
		return false
	}
	var e *APIError
	return errors.As(err, &e) && (e.StatusCode == 400 || e.StatusCode == 401 || e.StatusCode == 403)
}

// decimal preserves protobuf uint64 JSON without any floating point conversion.
type decimal uint64

func (d *decimal) UnmarshalJSON(raw []byte) error {
	s := string(raw)
	if len(raw) > 0 && raw[0] == '"' {
		if err := json.Unmarshal(raw, &s); err != nil {
			return err
		}
	}
	n, e := strconv.ParseUint(s, 10, 64)
	*d = decimal(n)
	return e
}
func (d decimal) MarshalJSON() ([]byte, error) {
	return json.Marshal(strconv.FormatUint(uint64(d), 10))
}

type premiumStorage struct {
	Type string    `json:"type"`
	S3   *S3Target `json:"s3"`
}
type premiumExportOptions struct {
	ExportOptions
	IncludeGeneratedColumns string `json:"includeGeneratedColumns"`
}
type premiumExport struct {
	ID          string               `json:"exportId,omitempty"`
	TiDBID      string               `json:"tidbId,omitempty"`
	DisplayName string               `json:"displayName,omitempty"`
	State       ExportState          `json:"state,omitempty"`
	Reason      string               `json:"reason,omitempty"`
	TSO         decimal              `json:"snapshotTso,omitempty"`
	Options     premiumExportOptions `json:"exportOptions"`
	Storage     *premiumStorage      `json:"cloudStorage,omitempty"`
	Username    string               `json:"username,omitempty"`
	Password    string               `json:"password,omitempty"`
}

func (r premiumExport) normalized() *Export {
	e := &Export{ExportID: r.ID, ClusterID: r.TiDBID, State: r.State, Reason: r.Reason, DisplayName: r.DisplayName, SnapshotTSO: strconv.FormatUint(uint64(r.TSO), 10), ExportOptions: &r.Options.ExportOptions}
	if r.Storage != nil {
		e.Target = &ExportTarget{Type: ExportTargetType(r.Storage.Type), S3: r.Storage.S3}
	}
	return e
}

type premiumFormat struct {
	Protocol      string         `json:"protocol"`
	Delimiter     string         `json:"delimiter"`
	Quote         string         `json:"quote"`
	Null          string         `json:"null"`
	DateSeparator DateSeparator  `json:"dateSeparator"`
	CommitTS      bool           `json:"includeCommitTs"`
	Encoding      BinaryEncoding `json:"binaryEncodingMethod"`
	Raw           bool           `json:"outputRawChangeEvent"`
	Header        bool           `json:"outputFieldHeader"`
}
type premiumEndpoint struct {
	URI       string `json:"s3Uri"`
	AuthType  string `json:"authType"`
	AccessKey string `json:"accessKey,omitempty"`
	SecretKey string `json:"secretKey,omitempty"`
	Provider  string `json:"provider"`
}
type premiumFlush struct {
	Interval string  `json:"interval"`
	FileSize decimal `json:"fileSize"`
}
type premiumS3 struct {
	Endpoint  premiumEndpoint  `json:"endpoint"`
	Format    premiumFormat    `json:"dataFormat"`
	Flush     premiumFlush     `json:"storageFlushConfig"`
	Selectors []ColumnSelector `json:"columnSelectors,omitempty"`
}
type premiumTables struct {
	Rules []string  `json:"filterRules"`
	Mode  TableMode `json:"mode"`
}
type premiumStart struct {
	Mode StartMode `json:"mode"`
	TSO  decimal   `json:"startTso"`
}
type premiumChangefeed struct {
	ID         string          `json:"id,omitempty"`
	TiDBID     string          `json:"tidbId"`
	Name       string          `json:"name"`
	State      ChangefeedState `json:"state,omitempty"`
	Capacity   string          `json:"changefeedCapacity"`
	Downstream string          `json:"downstreamType"`
	Tables     premiumTables   `json:"tableConfig"`
	Start      premiumStart    `json:"startPosition"`
	S3         *premiumS3      `json:"s3,omitempty"`
}

func premiumChangefeedFrom(req *CreateChangefeedRequest, cluster string) premiumChangefeed {
	cs := req.Sink.CloudStorage
	tso, _ := strconv.ParseUint(req.StartPosition.TSO, 10, 64)
	cfg := cs.DataFormat.CSVConfig
	interval := req.FlushInterval
	if interval == 0 {
		interval = time.Duration(cs.IntervalInSeconds) * time.Second
	}
	return premiumChangefeed{TiDBID: cluster, Name: req.DisplayName, Capacity: strconv.Itoa(req.RCU) + "ccu", Downstream: "S3", Tables: premiumTables{req.Filter.FilterRule, req.Filter.Mode}, Start: premiumStart{req.StartPosition.Mode, decimal(tso)}, S3: &premiumS3{
		Endpoint: premiumEndpoint{URI: cs.Storage.S3.URI, AuthType: "S3_AUTH_TYPE_AK_SK", AccessKey: cs.Storage.S3.AccessKey.ID, SecretKey: cs.Storage.S3.AccessKey.Secret, Provider: "AWS"},
		Format:   premiumFormat{Protocol: "PROTOCOL_CSV", Delimiter: ",", Quote: "\"", Null: "\\N", DateSeparator: cs.DateSeparator, CommitTS: cfg.IncludeCommitTs, Encoding: cfg.Encoding},
		Flush:    premiumFlush{Interval: interval.String(), FileSize: decimal(uint64(cs.SizeInMiB) * 1024 * 1024)}, Selectors: cs.ColumnSelectors,
	}}
}
func (r premiumChangefeed) normalized() *Changefeed {
	out := &Changefeed{ID: r.ID, ChangefeedID: r.ID, ClusterID: r.TiDBID, DisplayName: r.Name, State: r.State, Filter: &ChangefeedFilter{FilterRule: r.Tables.Rules, Mode: r.Tables.Mode}, StartPosition: &StartPosition{Mode: r.Start.Mode, TSO: strconv.FormatUint(uint64(r.Start.TSO), 10)}}
	if r.S3 != nil && r.Downstream == "S3" {
		s := r.S3
		protocol := CloudStorageProtocol(s.Format.Protocol)
		if protocol == "PROTOCOL_CSV" {
			protocol = CloudStorageProtocolCSV
		}
		out.Sink = &Sink{Type: ChangefeedTypeCloudStorage, CloudStorage: &CloudStorageSink{Storage: &CloudStorage{Type: CloudStorageTypeS3, S3: &S3CloudStorage{URI: s.Endpoint.URI}}, DataFormat: &CloudStorageDataFormat{Protocol: protocol, CSVConfig: &CSVConfig{Delimiter: s.Format.Delimiter, Quote: s.Format.Quote, NullValue: s.Format.Null, IncludeCommitTs: s.Format.CommitTS, Encoding: s.Format.Encoding}}, DateSeparator: s.Format.DateSeparator, ColumnSelectors: s.Selectors}}
	}
	return out
}
