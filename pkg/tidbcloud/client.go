// Package tidbcloud is a small client for the TiDB Cloud Serverless OpenAPI
// (v1beta1). It covers the Export and Changefeed services that tidb2snowflake
// uses to drive a snapshot export and an incremental changefeed for a cluster.
//
// Authentication uses an API key (public/private) over HTTP Digest, matching
// the TiDB Cloud Serverless OpenAPI.
package tidbcloud

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/icholy/digest"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	// DefaultHost is the global host for the TiDB Cloud Serverless OpenAPI.
	DefaultHost = "serverless.tidbapi.com"

	defaultTimeout    = 30 * time.Second
	defaultMaxRetries = 5
	maxBackoff        = 5 * time.Second
)

// Client talks to the TiDB Cloud Serverless OpenAPI using HTTP Digest
// authentication with an API key (public/private).
type Client struct {
	baseURL      string
	publicKey    string
	privateKey   string
	timeout      time.Duration
	maxRetries   int
	retryBackoff func(int) time.Duration

	baseTransport http.RoundTripper
	httpClient    *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithHost overrides the API host (default DefaultHost). The scheme is always
// https.
func WithHost(host string) Option {
	return func(c *Client) {
		if host != "" {
			c.baseURL = "https://" + host
		}
	}
}

// WithTimeout sets the per-request timeout (default 30s).
func WithTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.timeout = d
		}
	}
}

// WithMaxRetries sets how many times retriable failures (network errors and
// 401/429/5xx responses) are retried (default 5).
func WithMaxRetries(n int) Option {
	return func(c *Client) {
		if n >= 0 {
			c.maxRetries = n
		}
	}
}

// WithBaseTransport sets the underlying RoundTripper that the digest auth
// transport wraps. Useful for tests and custom proxies.
func WithBaseTransport(rt http.RoundTripper) Option {
	return func(c *Client) {
		c.baseTransport = rt
	}
}

// NewClient creates a Client authenticated with the given public/private API key.
func NewClient(publicKey, privateKey string, opts ...Option) (*Client, error) {
	if publicKey == "" || privateKey == "" {
		return nil, errors.New("tidbcloud: public and private API key are required")
	}
	c := &Client{
		baseURL:      "https://" + DefaultHost,
		publicKey:    publicKey,
		privateKey:   privateKey,
		timeout:      defaultTimeout,
		maxRetries:   defaultMaxRetries,
		retryBackoff: backoff,
	}
	for _, o := range opts {
		o(c)
	}
	base := c.baseTransport
	if base == nil {
		base = http.DefaultTransport
	}
	c.httpClient = &http.Client{
		Timeout: c.timeout,
		Transport: &digest.Transport{
			Username:  c.publicKey,
			Password:  c.privateKey,
			Transport: base,
		},
	}
	return c, nil
}

// do executes an HTTP request against the API, JSON-encoding body (if non-nil)
// and JSON-decoding the success response into out (if non-nil). It retries
// network errors and retriable status codes with exponential backoff.
func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var reqBody []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("tidbcloud: marshal request body: %w", err)
		}
		reqBody = b
	}

	url := c.baseURL + path
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			backoffDelay := backoff(attempt)
			if c.retryBackoff != nil {
				backoffDelay = c.retryBackoff(attempt)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoffDelay):
			}
		}

		var reader io.Reader
		if reqBody != nil {
			reader = bytes.NewReader(reqBody)
		}
		req, err := http.NewRequestWithContext(ctx, method, url, reader)
		if err != nil {
			return fmt.Errorf("tidbcloud: build request: %w", err)
		}
		req.Header.Set("Accept", "application/json")
		if reqBody != nil {
			req.Header.Set("Content-Type", "application/json")
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			// Context cancellation is not retriable.
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = fmt.Errorf("tidbcloud: %s %s: %s", method, sanitizeLogText(path), sanitizeLogText(err.Error()))
			if attempt == c.maxRetries {
				logTiDBCloudRequestFailed(method, path, url, attempt, c.maxRetries, reqBody, req, nil, nil, lastErr)
				return lastErr
			}
			continue
		}

		respBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("tidbcloud: read response body: %s", sanitizeLogText(readErr.Error()))
			if attempt == c.maxRetries {
				logTiDBCloudRequestFailed(method, path, url, attempt, c.maxRetries, reqBody, req, resp, nil, lastErr)
				return lastErr
			}
			continue
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if out != nil && len(respBody) > 0 {
				if err := json.Unmarshal(respBody, out); err != nil {
					decodeErr := fmt.Errorf("tidbcloud: decode response: %w", err)
					logTiDBCloudResponseDecodeFailed(method, path, url, attempt, c.maxRetries, reqBody, req, resp, respBody, decodeErr)
					return decodeErr
				}
			}
			return nil
		}

		apiErr := parseAPIError(resp.StatusCode, respBody)
		if isRetriableStatus(resp.StatusCode) && attempt < c.maxRetries {
			lastErr = apiErr
			continue
		}
		logTiDBCloudRequestFailed(method, path, url, attempt, c.maxRetries, reqBody, req, resp, respBody, apiErr)
		return apiErr
	}
	return lastErr
}

func logTiDBCloudRequestFailed(method, path, rawURL string, attempt, maxRetries int, reqBody []byte, req *http.Request, resp *http.Response, respBody []byte, err error) {
	log.Error("TiDB Cloud request failed", tidbCloudRequestResponseLogFields(method, path, rawURL, attempt, maxRetries, reqBody, req, resp, respBody, err)...)
}

func logTiDBCloudResponseDecodeFailed(method, path, rawURL string, attempt, maxRetries int, reqBody []byte, req *http.Request, resp *http.Response, respBody []byte, err error) {
	log.Error("TiDB Cloud response decode failed", tidbCloudRequestResponseLogFields(method, path, rawURL, attempt, maxRetries, reqBody, req, resp, respBody, err)...)
}

func tidbCloudRequestResponseLogFields(method, path, rawURL string, attempt, maxRetries int, reqBody []byte, req *http.Request, resp *http.Response, respBody []byte, err error) []zap.Field {
	fields := []zap.Field{
		zap.String("method", method),
		zap.String("path", sanitizeLogText(path)),
		zap.String("url", sanitizeURLString(rawURL)),
		zap.Int("attempt", attempt+1),
		zap.Int("maxAttempts", maxRetries+1),
		zap.String("requestBody", sanitizeHTTPBody(reqBody)),
	}
	requestForHeaders := req
	if resp != nil && resp.Request != nil {
		requestForHeaders = resp.Request
	}
	if requestForHeaders != nil {
		fields = append(fields, zap.Any("requestHeaders", sanitizeHTTPHeaders(requestForHeaders.Header)))
	}
	if resp != nil {
		fields = append(fields,
			zap.Int("statusCode", resp.StatusCode),
			zap.String("status", resp.Status),
			zap.Any("responseHeaders", sanitizeHTTPHeaders(resp.Header)),
			zap.String("responseBody", sanitizeHTTPBody(respBody)),
		)
	}
	if err != nil {
		fields = append(fields, zap.String("error", sanitizeLogText(err.Error())))
	}
	return fields
}

func backoff(attempt int) time.Duration {
	d := time.Duration(200*(1<<(attempt-1))) * time.Millisecond
	if d > maxBackoff {
		return maxBackoff
	}
	return d
}

func isRetriableStatus(code int) bool {
	switch code {
	case http.StatusUnauthorized,
		http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}
