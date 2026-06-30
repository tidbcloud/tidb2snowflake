package tidbcloud

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// newTestClient builds a Client pointed at the given test server URL.
func newTestClient(t *testing.T, serverURL string, opts ...Option) *Client {
	t.Helper()
	c, err := NewClient("public-key", "private-key", opts...)
	require.NoError(t, err)
	c.baseURL = serverURL
	return c
}

func TestNewClient_RequiresKeys(t *testing.T) {
	_, err := NewClient("", "priv")
	require.Error(t, err)
	_, err = NewClient("pub", "")
	require.Error(t, err)

	c, err := NewClient("pub", "priv")
	require.NoError(t, err)
	require.Equal(t, "https://"+DefaultHost, c.baseURL)
}

func TestWithHost(t *testing.T) {
	c, err := NewClient("pub", "priv", WithHost("eu-central-1.serverless.tidbapi.com"))
	require.NoError(t, err)
	require.Equal(t, "https://eu-central-1.serverless.tidbapi.com", c.baseURL)
}

// TestDigestAuth verifies the client answers an HTTP Digest challenge: the first
// request arrives without credentials and gets a 401 + WWW-Authenticate, and the
// retried request carries a Digest Authorization header with the public key.
func TestDigestAuth(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		auth := r.Header.Get("Authorization")
		if auth == "" {
			w.Header().Set("WWW-Authenticate", `Digest realm="tidbcloud", qop="auth", nonce="abc123", opaque="xyz"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		require.True(t, strings.HasPrefix(auth, "Digest "), "expected Digest auth, got %q", auth)
		require.Contains(t, auth, `username="public-key"`)
		_ = n
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"exportId":"exp-1","state":"RUNNING"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	exp, err := c.GetExport(context.Background(), "10", "exp-1")
	require.NoError(t, err)
	require.Equal(t, "exp-1", exp.ExportID)
	require.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(2), "digest should require a challenge round-trip")
}

func TestDo_APIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":3,"message":"invalid snapshot_tso"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL)
	_, err := c.GetExport(context.Background(), "10", "exp-1")
	require.Error(t, err)
	apiErr, ok := err.(*APIError)
	require.True(t, ok, "expected *APIError, got %T", err)
	require.Equal(t, http.StatusBadRequest, apiErr.StatusCode)
	require.Equal(t, 3, apiErr.Code)
	require.Contains(t, apiErr.Message, "invalid snapshot_tso")
}

func TestDoLogsSanitizedRequestAndResponseOnAPIError(t *testing.T) {
	core, observed := observer.New(zapcore.ErrorLevel)
	restore := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{})
	defer restore()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Set-Cookie", "tidbcloud_session=response-secret")
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Tidb-Trace-Id", "trace-1")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":3,"message":"bad sink secret=message-secret","details":[{"secret":"server-secret","table":"db1.t1"}]}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL, WithMaxRetries(0))
	_, err := c.CreateChangefeed(context.Background(), "10", &CreateChangefeedRequest{
		DisplayName: "tidb2snowflake-1780000000123",
		Sink: &Sink{
			Type: ChangefeedTypeCloudStorage,
			CloudStorage: &CloudStorageSink{
				Storage: &CloudStorage{
					Type: CloudStorageTypeS3,
					S3: &S3CloudStorage{
						URI:      "s3://bucket/increment?secret-access-key=query-secret",
						AuthType: S3AuthTypeAccessKey,
						AccessKey: &S3AccessKey{
							ID:     "AKIASECRETID",
							Secret: "aws-secret",
						},
					},
				},
			},
		},
	})
	require.Error(t, err)

	entries := observed.FilterMessage("tidbcloud request failed").All()
	require.Len(t, entries, 1)
	fields := entries[0].ContextMap()
	require.Equal(t, http.MethodPost, fields["method"])
	require.Equal(t, "/v1beta1/clusters/10/changefeeds", fields["path"])
	require.Equal(t, int64(http.StatusBadRequest), fields["responseStatus"])

	requestHeaders := fields["requestHeaders"].(string)
	require.Contains(t, requestHeaders, `"Accept":["application/json"]`)
	require.Contains(t, requestHeaders, `"Content-Type":["application/json"]`)

	requestBody := fields["requestBody"].(string)
	require.Contains(t, requestBody, `"displayName":"tidb2snowflake-1780000000123"`)
	require.Contains(t, requestBody, `"secret":"<redacted>"`)
	require.NotContains(t, requestBody, "AKIASECRETID")
	require.NotContains(t, requestBody, "aws-secret")
	require.NotContains(t, requestBody, "query-secret")

	responseHeaders := fields["responseHeaders"].(string)
	require.Contains(t, responseHeaders, `"Content-Type":["application/json"]`)
	require.Contains(t, responseHeaders, `"X-Tidb-Trace-Id":["trace-1"]`)
	require.Contains(t, responseHeaders, `"Set-Cookie":["<redacted>"]`)
	require.NotContains(t, responseHeaders, "response-secret")

	responseBody := fields["responseBody"].(string)
	require.Contains(t, responseBody, `"message":"bad sink secret=<redacted>"`)
	require.Contains(t, responseBody, `"table":"db1.t1"`)
	require.Contains(t, responseBody, `"secret":"<redacted>"`)
	require.NotContains(t, responseBody, "message-secret")
	require.NotContains(t, responseBody, "server-secret")

	errorText := fields["error"].(string)
	require.Contains(t, errorText, "bad sink secret=<redacted>")
	require.NotContains(t, errorText, "message-secret")
}

func TestDoLogsResponseOnDecodeError(t *testing.T) {
	core, observed := observer.New(zapcore.ErrorLevel)
	restore := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{})
	defer restore()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"secret":"decode-secret","message":"bad"`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL, WithMaxRetries(0))
	_, err := c.GetExport(context.Background(), "10", "exp-1")
	require.Error(t, err)

	entries := observed.FilterMessage("tidbcloud request failed").All()
	require.Len(t, entries, 1)
	fields := entries[0].ContextMap()
	require.Equal(t, http.MethodGet, fields["method"])
	require.Equal(t, "/v1beta1/clusters/10/exports/exp-1", fields["path"])
	require.Equal(t, int64(http.StatusOK), fields["responseStatus"])
	require.Equal(t, `{"secret":"<redacted>","message":"bad"`, fields["responseBody"])
	require.NotContains(t, fields["responseBody"], "decode-secret")
}

func TestDo_RetriesOn503ThenSucceeds(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"exportId":"exp-1","state":"SUCCEEDED"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL, WithMaxRetries(2))
	exp, err := c.GetExport(context.Background(), "10", "exp-1")
	require.NoError(t, err)
	require.Equal(t, ExportStateSucceeded, exp.State)
	require.Equal(t, int32(2), atomic.LoadInt32(&calls))
}

func TestDo_DoesNotRetryOn400(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":3,"message":"bad"}`))
	}))
	defer srv.Close()

	c := newTestClient(t, srv.URL, WithMaxRetries(3))
	_, err := c.GetExport(context.Background(), "10", "exp-1")
	require.Error(t, err)
	require.Equal(t, int32(1), atomic.LoadInt32(&calls), "4xx must not be retried")
}

func TestDo_ContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	c := newTestClient(t, srv.URL, WithMaxRetries(100))
	_, err := c.GetExport(ctx, "10", "exp-1")
	require.Error(t, err)
}
