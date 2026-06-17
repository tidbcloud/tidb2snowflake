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

// ChangefeedArch is the implementation architecture of a changefeed.
type ChangefeedArch string

const (
	ChangefeedArchReplicationWorker ChangefeedArch = "REPLICATION_WORKER"
	ChangefeedArchTiCDCNewArch      ChangefeedArch = "TICDC_NEWARCH"
)

// Changefeed is a changefeed resource.
type Changefeed struct {
	ChangefeedID  string            `json:"changefeedId,omitempty"`
	ClusterID     string            `json:"clusterId,omitempty"`
	State         ChangefeedState   `json:"state,omitempty"`
	DisplayName   string            `json:"displayName,omitempty"`
	Sink          *Sink             `json:"sink,omitempty"`
	Filter        *ChangefeedFilter `json:"filter,omitempty"`
	StartPosition *StartPosition    `json:"startPosition,omitempty"`
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
	FilterRule []string `json:"filterRule,omitempty"`
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

// CreateChangefeed creates a new changefeed.
func (c *Client) CreateChangefeed(ctx context.Context, clusterID string, req *CreateChangefeedRequest) (*Changefeed, error) {
	var out Changefeed
	if err := c.do(ctx, http.MethodPost, changefeedsPath(clusterID), req, &out); err != nil {
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
	return &out, nil
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
// (RUNNING or WARNING) or reaches a failure/terminal state. A non-positive
// interval defaults to 5s.
func (c *Client) WaitChangefeed(ctx context.Context, clusterID, changefeedID string, interval time.Duration) (*Changefeed, error) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	for {
		cf, err := c.GetChangefeed(ctx, clusterID, changefeedID)
		if err != nil {
			return nil, err
		}
		switch cf.State {
		case ChangefeedStateRunning, ChangefeedStateWarning:
			return cf, nil
		case ChangefeedStateCreateFailed, ChangefeedStateRunningFailed,
			ChangefeedStateDeleting, ChangefeedStateDeleted:
			return cf, fmt.Errorf("tidbcloud: changefeed %s ended in state %s", changefeedID, cf.State)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
}
