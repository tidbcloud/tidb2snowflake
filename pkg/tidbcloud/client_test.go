package tidbcloud

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// newTestClient builds a Client pointed at the given test server URL.
func newTestClient(t *testing.T, serverURL string, opts ...Option) *Client {
	t.Helper()
	c, err := NewClient("public-key", "private-key", opts...)
	require.NoError(t, err)
	c.baseURL = serverURL
	return c
}

func TestNewClientRequiresKeys(t *testing.T) {
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

func TestDoAPIError(t *testing.T) {
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

func TestDoRetriesOn503ThenSucceeds(t *testing.T) {
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

func TestDoDoesNotRetryOn400(t *testing.T) {
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

func TestDoContextCancel(t *testing.T) {
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
