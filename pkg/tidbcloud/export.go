package tidbcloud

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// ExportState is the lifecycle state of an export task.
type ExportState string

const (
	ExportStateRunning   ExportState = "RUNNING"
	ExportStateSucceeded ExportState = "SUCCEEDED"
	ExportStateFailed    ExportState = "FAILED"
	ExportStateCanceled  ExportState = "CANCELED"
	ExportStateDeleted   ExportState = "DELETED"
	ExportStateExpired   ExportState = "EXPIRED"
)

// ExportFileType is the export file format.
type ExportFileType string

const (
	ExportFileTypeSQL     ExportFileType = "SQL"
	ExportFileTypeCSV     ExportFileType = "CSV"
	ExportFileTypeParquet ExportFileType = "PARQUET"
)

// ExportCompression is the compression algorithm of the exported files.
type ExportCompression string

const (
	ExportCompressionNone   ExportCompression = "NONE"
	ExportCompressionGzip   ExportCompression = "GZIP"
	ExportCompressionSnappy ExportCompression = "SNAPPY"
	ExportCompressionZstd   ExportCompression = "ZSTD"
)

// ExportTargetType is the destination type of an export.
type ExportTargetType string

const (
	ExportTargetTypeLocal ExportTargetType = "LOCAL"
	ExportTargetTypeS3    ExportTargetType = "S3"
	ExportTargetTypeGCS   ExportTargetType = "GCS"
	ExportTargetTypeOSS   ExportTargetType = "OSS"
)

// S3AuthType is the authentication method for an S3 target.
type S3AuthType string

const (
	S3AuthTypeAccessKey S3AuthType = "ACCESS_KEY"
	S3AuthTypeRoleArn   S3AuthType = "ROLE_ARN"
)

// ExportCSVDialect is the dumpling CSV output dialect of an export.
type ExportCSVDialect string

const (
	ExportCSVDialectDefault   ExportCSVDialect = "DEFAULT"
	ExportCSVDialectSnowflake ExportCSVDialect = "SNOWFLAKE"
	ExportCSVDialectBigQuery  ExportCSVDialect = "BIGQUERY"
	ExportCSVDialectRedshift  ExportCSVDialect = "REDSHIFT"
)

// Export is an export task resource.
type Export struct {
	ExportID      string         `json:"exportId,omitempty"`
	Name          string         `json:"name,omitempty"`
	ClusterID     string         `json:"clusterId,omitempty"`
	State         ExportState    `json:"state,omitempty"`
	ExportOptions *ExportOptions `json:"exportOptions,omitempty"`
	Target        *ExportTarget  `json:"target,omitempty"`
	Reason        string         `json:"reason,omitempty"`
	DisplayName   string         `json:"displayName,omitempty"`
	CreateTime    string         `json:"createTime,omitempty"`
	CompleteTime  string         `json:"completeTime,omitempty"`
	SnapshotTime  string         `json:"snapshotTime,omitempty"`
	// SnapshotTSO is the effective TSO of the snapshot. Use this exact value as
	// the changefeed start position (FROM_TSO) for a consistent handoff.
	SnapshotTSO string `json:"snapshotTso,omitempty"`
}

// ExportOptions configures what and how to export.
type ExportOptions struct {
	FileType    ExportFileType    `json:"fileType,omitempty"`
	Compression ExportCompression `json:"compression,omitempty"`
	Filter      *ExportFilter     `json:"filter,omitempty"`
	CSVFormat   *ExportCSVFormat  `json:"csvFormat,omitempty"`
	// EscapeBackslash controls dumpling backslash escaping. Tri-state: nil keeps
	// the server default, an explicit true/false forces it.
	EscapeBackslash *bool `json:"escapeBackslash,omitempty"`
	// SnapshotTSO pins the snapshot to a specific TiDB TSO. When empty, the
	// export is taken at create time and the effective TSO is returned on the
	// Export resource.
	SnapshotTSO string `json:"snapshotTso,omitempty"`
}

// ExportFilter selects which tables to export.
type ExportFilter struct {
	Table *ExportFilterTable `json:"table,omitempty"`
	SQL   string             `json:"sql,omitempty"`
}

// ExportFilterTable selects tables by filter pattern.
type ExportFilterTable struct {
	Patterns []string `json:"patterns,omitempty"`
	Where    string   `json:"where,omitempty"`
}

// ExportCSVFormat is the CSV format specification.
type ExportCSVFormat struct {
	Separator  string  `json:"separator,omitempty"`
	Delimiter  *string `json:"delimiter,omitempty"`
	NullValue  *string `json:"nullValue,omitempty"`
	SkipHeader bool    `json:"skipHeader,omitempty"`
	// Dialect selects the dumpling CSV output dialect (e.g. SNOWFLAKE) so the
	// exported CSV is written in a form the target warehouse's COPY can parse.
	Dialect ExportCSVDialect `json:"dialect,omitempty"`
}

// ExportTarget is the export destination.
type ExportTarget struct {
	Type ExportTargetType `json:"type,omitempty"`
	S3   *S3Target        `json:"s3,omitempty"`
}

// S3Target is an Amazon S3 export destination.
type S3Target struct {
	URI       string       `json:"uri,omitempty"`
	AuthType  S3AuthType   `json:"authType,omitempty"`
	AccessKey *S3AccessKey `json:"accessKey,omitempty"`
	RoleArn   string       `json:"roleArn,omitempty"`
}

// S3AccessKey is an AWS access key credential pair.
type S3AccessKey struct {
	ID     string `json:"id,omitempty"`
	Secret string `json:"secret,omitempty"`
}

// CreateExportRequest is the body of CreateExport.
type CreateExportRequest struct {
	ExportOptions *ExportOptions `json:"exportOptions,omitempty"`
	Target        *ExportTarget  `json:"target,omitempty"`
	DisplayName   string         `json:"displayName,omitempty"`
}

type listExportsResponse struct {
	Exports       []*Export `json:"exports"`
	NextPageToken string    `json:"nextPageToken"`
}

func exportsPath(clusterID string) string {
	return fmt.Sprintf("/v1beta1/clusters/%s/exports", url.PathEscape(clusterID))
}

func exportPath(clusterID, exportID string) string {
	return exportsPath(clusterID) + "/" + url.PathEscape(exportID)
}

// CreateExport starts a new export task.
func (c *Client) CreateExport(ctx context.Context, clusterID string, req *CreateExportRequest) (*Export, error) {
	var out Export
	if err := c.do(ctx, http.MethodPost, exportsPath(clusterID), req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetExport fetches a single export task.
func (c *Client) GetExport(ctx context.Context, clusterID, exportID string) (*Export, error) {
	var out Export
	if err := c.do(ctx, http.MethodGet, exportPath(clusterID, exportID), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListExports returns the first page of export tasks for the cluster.
func (c *Client) ListExports(ctx context.Context, clusterID string) ([]*Export, error) {
	var out listExportsResponse
	if err := c.do(ctx, http.MethodGet, exportsPath(clusterID), nil, &out); err != nil {
		return nil, err
	}
	return out.Exports, nil
}

// CancelExport cancels a running export task.
func (c *Client) CancelExport(ctx context.Context, clusterID, exportID string) (*Export, error) {
	var out Export
	if err := c.do(ctx, http.MethodPost, exportPath(clusterID, exportID)+":cancel", struct{}{}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DeleteExport deletes an export task.
func (c *Client) DeleteExport(ctx context.Context, clusterID, exportID string) (*Export, error) {
	var out Export
	if err := c.do(ctx, http.MethodDelete, exportPath(clusterID, exportID), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// WaitExport polls GetExport until the task reaches a terminal state. It returns
// the export on SUCCEEDED, and an error for any failure/terminal state. A
// non-positive interval defaults to 5s.
func (c *Client) WaitExport(ctx context.Context, clusterID, exportID string, interval time.Duration) (*Export, error) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	for {
		exp, err := c.GetExport(ctx, clusterID, exportID)
		if err != nil {
			return nil, err
		}
		switch exp.State {
		case ExportStateSucceeded:
			return exp, nil
		case ExportStateFailed, ExportStateCanceled, ExportStateDeleted, ExportStateExpired:
			return exp, fmt.Errorf("tidbcloud: export %s ended in state %s: %s", exportID, exp.State, exp.Reason)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
}
