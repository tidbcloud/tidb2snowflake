package ticdc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	apiv2 "github.com/pingcap/ticdc/api/v2"
	"github.com/pingcap/ticdc/pkg/common"
	"github.com/pingcap/ticdc/pkg/config"
	putil "github.com/pingcap/ticdc/pkg/util"
)

type ChangefeedConfig = apiv2.ChangefeedConfig

type Changefeed = apiv2.ChangeFeedInfo

type ChangefeedConfigOptions struct {
	Tables        []string
	StorageURI    *url.URL
	StartTSO      uint64
	FlushInterval time.Duration
	FileSizeMiB   int
}

type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
}

type Option func(*Client)

func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

func NewClient(address string, opts ...Option) (*Client, error) {
	if strings.TrimSpace(address) == "" {
		return nil, errors.New("ticdc address is required")
	}
	if !strings.Contains(address, "://") {
		address = "http://" + address
	}
	baseURL, err := url.Parse(strings.TrimRight(address, "/"))
	if err != nil {
		return nil, errors.Annotate(err, "parse ticdc address")
	}
	if baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, errors.Errorf("invalid ticdc address %q", address)
	}
	c := &Client{
		baseURL:    baseURL,
		httpClient: http.DefaultClient,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

func BuildChangefeedConfig(opts ChangefeedConfigOptions) (*ChangefeedConfig, error) {
	if opts.StorageURI == nil {
		return nil, errors.New("storage uri is required")
	}
	flushInterval := opts.FlushInterval
	if flushInterval <= 0 {
		flushInterval = 60 * time.Second
	}
	fileSizeMiB := opts.FileSizeMiB
	if fileSizeMiB <= 0 {
		fileSizeMiB = 64
	}
	fileSizeBytes := fileSizeMiB * 1024 * 1024

	sinkURI := *opts.StorageURI
	values := sinkURI.Query()
	values.Set("protocol", "csv")
	values.Set("flush-interval", flushInterval.String())
	values.Set("file-size", strconv.Itoa(fileSizeBytes))
	sinkURI.RawQuery = values.Encode()

	protocol := "csv"
	dateSeparator := config.DateSeparatorDay.String()
	replicaCfg := apiv2.GetDefaultReplicaConfig()
	replicaCfg.Filter = &apiv2.FilterConfig{Rules: opts.Tables}
	replicaCfg.Sink.Protocol = &protocol
	replicaCfg.Sink.CSVConfig.IncludeCommitTs = true
	replicaCfg.Sink.CSVConfig.BinaryEncodingMethod = config.BinaryEncodingHex
	replicaCfg.Sink.DateSeparator = &dateSeparator
	replicaCfg.Sink.CloudStorageConfig = &apiv2.CloudStorageConfig{
		FlushInterval:  putil.AddressOf(flushInterval.String()),
		FileSize:       putil.AddressOf(fileSizeBytes),
		OutputColumnID: putil.AddressOf(true),
	}

	return &ChangefeedConfig{
		SinkURI:       sinkURI.String(),
		StartTs:       opts.StartTSO,
		ReplicaConfig: replicaCfg,
	}, nil
}

func ChangefeedID(cf *Changefeed) string {
	if cf == nil {
		return ""
	}
	return cf.ID
}

func (c *Client) CreateChangefeed(ctx context.Context, cfg *ChangefeedConfig) (*Changefeed, error) {
	var out Changefeed
	if err := c.doJSON(ctx, http.MethodPost, []string{"changefeeds"}, cfg, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) GetChangefeed(ctx context.Context, changefeedID string) (*Changefeed, error) {
	values := url.Values{}
	values.Set("namespace", common.DefaultKeyspaceName)
	var out Changefeed
	if err := c.doJSON(ctx, http.MethodGet, []string{"changefeeds", changefeedID}, nil, &out, withQuery(values)); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) WaitChangefeed(ctx context.Context, changefeedID string, interval time.Duration) (*Changefeed, error) {
	if changefeedID == "" {
		return nil, errors.New("changefeed id is required")
	}
	if interval <= 0 {
		interval = 5 * time.Second
	}
	for {
		cf, err := c.GetChangefeed(ctx, changefeedID)
		if err != nil {
			return nil, err
		}
		switch cf.State {
		case config.StateNormal, config.StateWarning:
			return cf, nil
		case config.StateFailed, config.StateStopped, config.StateRemoved, config.StateFinished:
			return cf, fmt.Errorf("ticdc: changefeed %s ended in state %s", changefeedID, cf.State)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(interval):
		}
	}
}

type requestOption func(*url.URL)

func withQuery(values url.Values) requestOption {
	return func(u *url.URL) {
		u.RawQuery = values.Encode()
	}
}

func (c *Client) doJSON(
	ctx context.Context,
	method string,
	parts []string,
	body any,
	out any,
	opts ...requestOption,
) error {
	endpoint, err := c.endpoint(parts...)
	if err != nil {
		return err
	}
	for _, opt := range opts {
		opt(endpoint)
	}

	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return errors.Trace(err)
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), reader)
	if err != nil {
		return errors.Trace(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Trace(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		respBody, _ := io.ReadAll(resp.Body)
		return errors.Errorf("ticdc api %s %s failed with status %d: %s", method, endpoint.Path, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (c *Client) endpoint(parts ...string) (*url.URL, error) {
	endpoint := *c.baseURL
	pathParts := strings.FieldsFunc(strings.Trim(endpoint.Path, "/"), func(r rune) bool { return r == '/' })
	if len(pathParts) < 2 || pathParts[len(pathParts)-2] != "api" || pathParts[len(pathParts)-1] != "v2" {
		pathParts = append(pathParts, "api", "v2")
	}
	pathParts = append(pathParts, parts...)
	joined, err := url.JoinPath("/", pathParts...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	endpoint.Path = joined
	return &endpoint, nil
}
