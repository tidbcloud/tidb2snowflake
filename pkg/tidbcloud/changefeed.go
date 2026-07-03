package tidbcloud

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// ChangefeedState is the lifecycle state of a changefeed.
type ChangefeedState string

const (
	ChangefeedStateRunning       ChangefeedState = "RUNNING"
	ChangefeedStateCreating      ChangefeedState = "CREATING"
	ChangefeedStateScaling       ChangefeedState = "SCALING"
	ChangefeedStatePaused        ChangefeedState = "PAUSED"
	ChangefeedStateWarning       ChangefeedState = "WARNING"
	ChangefeedStateCreateFailed  ChangefeedState = "CREATE_FAILED"
	ChangefeedStateRunningFailed ChangefeedState = "RUNNING_FAILED"
	ChangefeedStateDeleting      ChangefeedState = "DELETING"
	ChangefeedStateDeleted       ChangefeedState = "DELETED"
)

// ChangefeedType is the sink type of a changefeed.
type ChangefeedType string

const (
	ChangefeedTypeKafka        ChangefeedType = "KAFKA"
	ChangefeedTypeMySQL        ChangefeedType = "MYSQL"
	ChangefeedTypeCloudStorage ChangefeedType = "CLOUD_STORAGE"
)

// CloudStorageType is the cloud storage backend of a cloud-storage sink.
type CloudStorageType string

const (
	CloudStorageTypeTiDBCloud CloudStorageType = "TIDB_CLOUD"
	CloudStorageTypeS3        CloudStorageType = "S3"
	CloudStorageTypeGCS       CloudStorageType = "GCS"
	CloudStorageTypeOSS       CloudStorageType = "OSS"
)

// CloudStorageProtocol is the encoding protocol of a cloud-storage sink.
type CloudStorageProtocol string

const (
	CloudStorageProtocolCanalJSON CloudStorageProtocol = "CANAL_JSON"
	CloudStorageProtocolCSV       CloudStorageProtocol = "CSV"
)

// BinaryEncoding is the encoding of binary columns in CSV.
type BinaryEncoding string

const (
	BinaryEncodingBase64 BinaryEncoding = "BASE64"
	BinaryEncodingHex    BinaryEncoding = "HEX"
)

// DateSeparator is the date-based directory partitioning of a cloud-storage sink.
type DateSeparator string

const (
	DateSeparatorNone  DateSeparator = "NONE"
	DateSeparatorYear  DateSeparator = "YEAR"
	DateSeparatorMonth DateSeparator = "MONTH"
	DateSeparatorDay   DateSeparator = "DAY"
)

// StartMode is the start position mode of a changefeed.
type StartMode string

const (
	StartModeFromNow  StartMode = "FROM_NOW"
	StartModeFromTSO  StartMode = "FROM_TSO"
	StartModeFromTime StartMode = "FROM_TIME"
)

// TableMode controls how the changefeed handles ineligible tables.
type TableMode string

const (
	TableModeIgnoreNotSupportTable TableMode = "IGNORE_NOT_SUPPORT_TABLE"
	TableModeForceSync             TableMode = "FORCE_SYNC"
)

// ChangefeedArch is the implementation architecture of a changefeed.
type ChangefeedArch string

const (
	ChangefeedArchReplicationWorker ChangefeedArch = "REPLICATION_WORKER"
	ChangefeedArchTiCDCNewArch      ChangefeedArch = "TICDC_NEWARCH"
)

// Changefeed is a changefeed resource.
type Changefeed struct {
	ID            string            `json:"id,omitempty"`
	ChangefeedID  string            `json:"changefeedId,omitempty"`
	ClusterID     string            `json:"clusterId,omitempty"`
	State         ChangefeedState   `json:"state,omitempty"`
	Name          string            `json:"name,omitempty"`
	DisplayName   string            `json:"displayName,omitempty"`
	Sink          *Sink             `json:"sink,omitempty"`
	Filter        *ChangefeedFilter `json:"filter,omitempty"`
	StartPosition *StartPosition    `json:"startPosition,omitempty"`
	RCU           int               `json:"rcu,omitempty"`
}

// ChangefeedSpecification is one RCU tier available to a cluster.
type ChangefeedSpecification struct {
	Name     string `json:"name,omitempty"`
	RCU      int    `json:"rcu,omitempty"`
	RPSLimit int    `json:"rpsLimit,omitempty"`
}

// ListChangefeedSpecificationsResponse contains the available RCU tiers.
type ListChangefeedSpecificationsResponse struct {
	Items []ChangefeedSpecification `json:"items,omitempty"`
	Total int                       `json:"total,omitempty"`
}

// Sink is the changefeed sink configuration.
type Sink struct {
	Type         ChangefeedType    `json:"type,omitempty"`
	CloudStorage *CloudStorageSink `json:"cloudStorage,omitempty"`
}

// CloudStorageSink is a cloud-storage changefeed sink.
type CloudStorageSink struct {
	Storage           *CloudStorage           `json:"storage,omitempty"`
	DataFormat        *CloudStorageDataFormat `json:"dataFormat,omitempty"`
	DateSeparator     DateSeparator           `json:"dateSeparator,omitempty"`
	IntervalInSeconds int                     `json:"intervalInSeconds,omitempty"`
	SizeInMiB         int                     `json:"sizeInMib,omitempty"`
	// OutputColumnID adds source column IDs to the CSV schema files so the
	// consumer can map columns by ID across DDL changes.
	OutputColumnID bool `json:"outputColumnId,omitempty"`
}

// CloudStorage selects the storage backend and its credentials.
type CloudStorage struct {
	Type CloudStorageType `json:"type,omitempty"`
	S3   *S3CloudStorage  `json:"s3,omitempty"`
}

// S3CloudStorage is an S3 cloud-storage backend.
type S3CloudStorage struct {
	URI       string       `json:"uri,omitempty"`
	AuthType  S3AuthType   `json:"authType,omitempty"`
	AccessKey *S3AccessKey `json:"accessKey,omitempty"`
	RoleArn   string       `json:"roleArn,omitempty"`
}

// CloudStorageDataFormat is the data format of a cloud-storage sink.
type CloudStorageDataFormat struct {
	Protocol  CloudStorageProtocol `json:"protocol,omitempty"`
	CSVConfig *CSVConfig           `json:"csvConfig,omitempty"`
}

// CSVConfig is the CSV configuration of a cloud-storage sink.
type CSVConfig struct {
	Delimiter       string         `json:"delimiter,omitempty"`
	Quote           string         `json:"quote,omitempty"`
	NullValue       string         `json:"nullValue,omitempty"`
	IncludeCommitTs bool           `json:"includeCommitTs,omitempty"`
	Encoding        BinaryEncoding `json:"encoding,omitempty"`
}

// ChangefeedFilter selects which tables to replicate.
type ChangefeedFilter struct {
	FilterRule      []string  `json:"filterRule,omitempty"`
	Mode            TableMode `json:"mode,omitempty"`
	EventFilterRule []any     `json:"eventFilterRule,omitempty"`
	CaseSensitive   bool      `json:"caseSensitive,omitempty"`
}

// StartPosition is where the changefeed begins replicating.
type StartPosition struct {
	Mode StartMode `json:"mode,omitempty"`
	TSO  string    `json:"tso,omitempty"`
	Time string    `json:"time,omitempty"`
}

// CreateChangefeedRequest is the body of CreateChangefeed.
type CreateChangefeedRequest struct {
	DisplayName   string            `json:"displayName,omitempty"`
	Sink          *Sink             `json:"sink,omitempty"`
	Filter        *ChangefeedFilter `json:"filter,omitempty"`
	StartPosition *StartPosition    `json:"startPosition,omitempty"`
	Arch          ChangefeedArch    `json:"arch,omitempty"`
	RCU           int               `json:"rcu,omitempty"`
}

func changefeedsPath(clusterID string) string {
	return fmt.Sprintf("/v1beta1/clusters/%s/changefeeds", url.PathEscape(clusterID))
}

func changefeedPath(clusterID, changefeedID string) string {
	return changefeedsPath(clusterID) + "/" + url.PathEscape(changefeedID)
}

func changefeedSpecificationsPath(clusterID string) string {
	return changefeedsPath(clusterID) + ":listSpecifications"
}

// CreateChangefeed creates a new changefeed.
func (c *Client) CreateChangefeed(ctx context.Context, clusterID string, req *CreateChangefeedRequest) (*Changefeed, error) {
	var out Changefeed
	if err := c.do(ctx, http.MethodPost, changefeedsPath(clusterID), req, &out); err != nil {
		return nil, err
	}
	normalizeChangefeedID(&out)
	return &out, nil
}

// ListChangefeedSpecifications lists the RCU tiers available for a cluster.
func (c *Client) ListChangefeedSpecifications(ctx context.Context, clusterID string) (*ListChangefeedSpecificationsResponse, error) {
	var out ListChangefeedSpecificationsResponse
	if err := c.do(ctx, http.MethodGet, changefeedSpecificationsPath(clusterID), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetChangefeed fetches a single changefeed.
func (c *Client) GetChangefeed(ctx context.Context, clusterID, changefeedID string) (*Changefeed, error) {
	var out Changefeed
	if err := c.do(ctx, http.MethodGet, changefeedPath(clusterID, changefeedID), nil, &out); err != nil {
		return nil, err
	}
	normalizeChangefeedID(&out)
	return &out, nil
}

func normalizeChangefeedID(cf *Changefeed) {
	if cf.ChangefeedID == "" {
		cf.ChangefeedID = cf.ID
	}
	if cf.ID == "" {
		cf.ID = cf.ChangefeedID
	}
}

// DeleteChangefeed deletes a changefeed.
func (c *Client) DeleteChangefeed(ctx context.Context, clusterID, changefeedID string) error {
	return c.do(ctx, http.MethodDelete, changefeedPath(clusterID, changefeedID), nil, nil)
}

// StartChangefeed resumes a stopped changefeed.
func (c *Client) StartChangefeed(ctx context.Context, clusterID, changefeedID string) (*Changefeed, error) {
	var out Changefeed
	if err := c.do(ctx, http.MethodPost, changefeedPath(clusterID, changefeedID)+":start", struct{}{}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// StopChangefeed pauses a running changefeed.
func (c *Client) StopChangefeed(ctx context.Context, clusterID, changefeedID string) (*Changefeed, error) {
	var out Changefeed
	if err := c.do(ctx, http.MethodPost, changefeedPath(clusterID, changefeedID)+":stop", struct{}{}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// WaitChangefeed polls GetChangefeed until the changefeed is operational
// (RUNNING or WARNING) or reaches a failure/terminal state.
func (c *Client) WaitChangefeed(ctx context.Context, clusterID, changefeedID string) error {
	for {
		cf, err := c.GetChangefeed(ctx, clusterID, changefeedID)
		if err != nil {
			return err
		}
		switch cf.State {
		case ChangefeedStateRunning, ChangefeedStateWarning:
			return nil
		case ChangefeedStateCreateFailed, ChangefeedStateRunningFailed,
			ChangefeedStateDeleting, ChangefeedStateDeleted:
			return fmt.Errorf("tidbcloud: changefeed %s ended in state %s", changefeedID, cf.State)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}
