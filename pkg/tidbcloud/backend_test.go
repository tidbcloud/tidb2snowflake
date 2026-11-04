package tidbcloud

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const largeID = "10000000000000000001"

func apiResponse(req *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: req}
}
func resolvedClient(t *testing.T, handler func(*http.Request) (int, string), opts ...Option) *Client {
	t.Helper()
	opts = append(opts, WithBaseTransport(roundTripFunc(func(r *http.Request) (*http.Response, error) {
		status, body := handler(r)
		return apiResponse(r, status, body), nil
	})), WithMaxRetries(0))
	c, err := NewClient("public", "private", opts...)
	require.NoError(t, err)
	return c
}
func clusterJSON(f APIFamily, plan string) string {
	if f == Serverless {
		return fmt.Sprintf(`{"clusterId":%q,"servicePlan":%q,"state":"ACTIVE","region":{"name":"regions/aws-us-west-2"}}`, largeID, plan)
	}
	return fmt.Sprintf(`{"tidbId":%q,"servicePlan":%q,"state":"ACTIVE","regionId":"aws-us-west-2","cloudProvider":"aws"}`, largeID, plan)
}
func TestResolvePlansAndEnvironment(t *testing.T) {
	for _, plan := range []string{"Starter", "Essential", "Premium", "Essential_V2", "Premium_Reserved", "BYOC"} {
		t.Run(plan, func(t *testing.T) {
			family := Premium
			if plan == "Starter" || plan == "Essential" {
				family = Serverless
			}
			var hosts []string
			c := resolvedClient(t, func(r *http.Request) (int, string) {
				hosts = append(hosts, r.URL.Host)
				if r.URL.Host == "serverless.staging.tidbapi.com" && family == Premium {
					return 404, `{"code":49900004}`
				}
				return 200, clusterJSON(family, plan)
			}, WithHost("cloud.staging.tidbapi.com"))
			b, err := c.OpenBackend(context.Background(), largeID, nil, false)
			require.NoError(t, err)
			require.Equal(t, family, b.Cluster.Family)
			require.Equal(t, plan, b.Cluster.ServicePlan)
			require.Equal(t, "staging", b.Cluster.Environment)
			require.Equal(t, "us-west-2", b.Cluster.Region)
			require.Equal(t, "serverless.staging.tidbapi.com", hosts[0])
			if family == Premium {
				require.Equal(t, []string{"serverless.staging.tidbapi.com", "cloud.staging.tidbapi.com"}, hosts)
			}
		})
	}
}
func TestResolverFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		body   string
	}{
		{"auth", 401, `{"code":16}`}, {"permission", 403, `{"code":7}`}, {"HTML404", 404, `<html>not found</html>`}, {"unknown404", 404, `{"code":5}`}, {"missing plan", 200, `{"clusterId":"10000000000000000001","state":"ACTIVE"}`}, {"conflict", 200, clusterJSON(Serverless, "Premium")}, {"mismatchedID", 200, `{"clusterId":"11","servicePlan":"Essential","state":"ACTIVE"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			c := resolvedClient(t, func(r *http.Request) (int, string) { calls++; return test.status, test.body })
			_, err := c.OpenBackend(context.Background(), largeID, nil, false)
			require.Error(t, err)
			require.Equal(t, 1, calls)
		})
	}
}

func TestResolverGatewayNotFound(t *testing.T) {
	const gatewayNotFound = `{"code":404,"details":[{"@type":"type.tidbapi.com/tidb.rpc.RequestInfo","requestId":"test-request","servingData":"49900004"}],"message":"Cluster not found."}`
	var hosts []string
	c := resolvedClient(t, func(r *http.Request) (int, string) {
		hosts = append(hosts, r.URL.Host)
		if r.URL.Host == "serverless.tidbapi.com" {
			return 404, gatewayNotFound
		}
		return 200, clusterJSON(Premium, "Premium")
	})
	b, err := c.OpenBackend(context.Background(), largeID, nil, false)
	require.NoError(t, err)
	require.Equal(t, Premium, b.Cluster.Family)
	require.Equal(t, []string{"serverless.tidbapi.com", "cloud.tidbapi.com"}, hosts)
	for _, body := range []string{
		`{"code":404,"message":"Cluster not found."}`,
		`{"code":404,"details":[{"@type":"other","servingData":"49900004"}]}`,
		`{"code":404,"details":[{"@type":"type.tidbapi.com/tidb.rpc.RequestInfo","servingData":"49900003"}]}`,
	} {
		require.False(t, IsNotFound(parseAPIError(404, []byte(body))))
	}
	require.False(t, IsNotFound(parseAPIError(403, []byte(gatewayNotFound))))
}

func TestLegacyServerlessJobNotFound(t *testing.T) {
	for _, message := range []string{
		"fail to get cdc changefeed: record not found",
		"fail to get export: record not found",
	} {
		body, err := json.Marshal(map[string]any{"code": 404, "message": message})
		require.NoError(t, err)
		require.True(t, IsNotFound(parseAPIError(404, body)))
		require.False(t, IsNotFound(parseAPIError(500, body)))
		require.False(t, IsNotFound(parseAPIError(403, body)))
	}
	require.False(t, IsNotFound(parseAPIError(404, []byte(`{"code":404,"message":"record not found"}`))))
}
func TestResolverDedicatedLegacyAndCustom(t *testing.T) {
	c := resolvedClient(t, func(r *http.Request) (int, string) {
		if r.URL.Host == "dedicated.tidbapi.com" {
			return 200, `{"clusterId":"10000000000000000001"}`
		}
		return 404, `{"code":49900004}`
	})
	_, err := c.OpenBackend(context.Background(), largeID, nil, false)
	require.ErrorContains(t, err, "Dedicated")
	_, err = c.OpenBackend(context.Background(), largeID, nil, true)
	require.ErrorContains(t, err, "legacy")
	calls := 0
	c = resolvedClient(t, func(r *http.Request) (int, string) {
		calls++
		require.Equal(t, "example.test", r.URL.Host)
		return 404, `{"code":49900004}`
	}, WithHost("example.test"))
	_, err = c.OpenBackend(context.Background(), largeID, nil, false)
	require.Error(t, err)
	require.Equal(t, 1, calls)
}
func TestBoundBackendDoesNotProbeAndChecksIdentity(t *testing.T) {
	c := resolvedClient(t, func(r *http.Request) (int, string) { t.Fatal("unexpected cluster request"); return 500, "" })
	binding := ClusterInfo{ClusterID: largeID, Family: Premium, Environment: "prod", ServicePlan: "Premium"}
	b, err := c.BoundBackend(largeID, binding)
	require.NoError(t, err)
	require.Equal(t, Premium, b.Cluster.Family)
	binding.Environment = "dev"
	_, err = c.BoundBackend(largeID, binding)
	require.Error(t, err)
}
func TestPremiumWireContract(t *testing.T) {
	var exportBody, cfBody map[string]any
	c := resolvedClient(t, func(r *http.Request) (int, string) {
		require.Equal(t, "cloud.tidbapi.com", r.URL.Host)
		if strings.HasPrefix(r.URL.Path, "/v1beta2/changefeeds") {
			require.Equal(t, "application/json+camel", r.Header.Get("Accept"))
		} else {
			require.Equal(t, "application/json", r.Header.Get("Accept"))
		}
		switch r.Method + " " + r.URL.Path {
		case "POST /v1beta2/tidbs/" + largeID + "/exports":
			require.NoError(t, json.NewDecoder(r.Body).Decode(&exportBody))
			return 200, `{"exportId":"export-1","tidbId":"10000000000000000001","state":"RUNNING","snapshotTso":"466924115091783691"}`
		case "POST /v1beta2/changefeeds":
			require.NoError(t, json.NewDecoder(r.Body).Decode(&cfBody))
			return 200, `{"id":"cf-1","tidbId":"10000000000000000001","state":"CREATING"}`
		case "GET /v1beta2/changefeeds:listCCUs":
			return 200, `{"ccus":[{"name":"4ccu","ccuNum":4}]}`
		default:
			t.Errorf("unexpected %s", r.URL)
			return 500, ""
		}
	})
	b, err := c.BoundBackend(largeID, ClusterInfo{ClusterID: largeID, Family: Premium, Environment: "prod"})
	require.NoError(t, err)
	b.Cluster.State = "ACTIVE"
	key := &S3AccessKey{ID: "key", Secret: "storage-secret"}
	escape := false
	quote := "\""
	null := "\\N"
	req := &CreateExportRequest{DisplayName: "task-snapshot", Target: &ExportTarget{Type: ExportTargetTypeS3, S3: &S3Target{URI: "s3://bucket/task/snapshot/", AuthType: S3AuthTypeAccessKey, AccessKey: key}}, ExportOptions: &ExportOptions{FileType: ExportFileTypeCSV, Compression: ExportCompressionNone, EscapeBackslash: &escape, CSVFormat: &ExportCSVFormat{Separator: ",", Delimiter: &quote, NullValue: &null, SkipHeader: true, Dialect: ExportCSVDialectSnowflake}, Filter: &ExportFilter{Table: &ExportFilterTable{Patterns: []string{"db.t"}}}}}
	exp, err := b.CreateExport(context.Background(), ExportSpec{Request: req, User: "export-user", Password: " password "})
	require.NoError(t, err)
	require.Equal(t, "466924115091783691", exp.SnapshotTSO)
	require.Equal(t, " password ", exportBody["password"])
	require.NotContains(t, exportBody, "export")
	require.NotContains(t, exportBody, "target")
	require.Equal(t, "NONE", exportBody["exportOptions"].(map[string]any)["includeGeneratedColumns"])
	opts := exportBody["exportOptions"].(map[string]any)
	require.Equal(t, map[string]any{"separator": ",", "delimiter": "\"", "nullValue": "\\N", "skipHeader": true, "dialect": "SNOWFLAKE"}, opts["csvFormat"])
	cfReq := &CreateChangefeedRequest{DisplayName: "task-increment", RCU: 4, Filter: &ChangefeedFilter{FilterRule: []string{"db.t"}, Mode: TableModeForceSync}, StartPosition: &StartPosition{Mode: StartModeFromTSO, TSO: "466924115091783691"}, Sink: &Sink{Type: ChangefeedTypeCloudStorage, CloudStorage: &CloudStorageSink{Storage: &CloudStorage{Type: CloudStorageTypeS3, S3: &S3CloudStorage{URI: "s3://bucket/task/increment/", AccessKey: key}}, DataFormat: &CloudStorageDataFormat{Protocol: CloudStorageProtocolCSV, CSVConfig: &CSVConfig{IncludeCommitTs: true, Encoding: BinaryEncodingHex}}, DateSeparator: DateSeparatorDay, IntervalInSeconds: 60, SizeInMiB: 64, ColumnSelectors: []ColumnSelector{{Matcher: []string{"db.t"}, Columns: []string{"*", "!secret"}}}}}}
	cf, err := b.CreateChangefeed(context.Background(), cfReq)
	require.NoError(t, err)
	require.Equal(t, "cf-1", cf.ChangefeedID)
	wire := cfBody["changefeed"].(map[string]any)
	require.Equal(t, "4ccu", wire["changefeedCapacity"])
	require.Equal(t, largeID, wire["tidbId"])
	require.NotContains(t, wire, "sink")
	require.Equal(t, map[string]any{"mode": "FROM_TSO", "startTso": "466924115091783691"}, wire["startPosition"])
	s3 := wire["s3"].(map[string]any)
	require.Equal(t, map[string]any{"interval": "1m0s", "fileSize": "67108864"}, s3["storageFlushConfig"])
	require.Equal(t, map[string]any{"protocol": "PROTOCOL_CSV", "delimiter": ",", "quote": "\"", "null": "\\N", "dateSeparator": "DAY", "includeCommitTs": true, "binaryEncodingMethod": "HEX", "outputRawChangeEvent": false, "outputFieldHeader": false}, s3["dataFormat"])
	capacities, err := b.Capacities(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{4}, capacities)
	require.NoError(t, b.ValidateChangefeed(time.Minute, 64))
	require.Error(t, b.ValidateChangefeed(time.Second, 64))
	require.Error(t, b.ValidateChangefeed(time.Minute, 513))
}
func TestCreatePostNeverBusinessRetries(t *testing.T) {
	for _, response := range []string{`{"code":13}`, `{"exportId":`} {
		t.Run(response, func(t *testing.T) {
			calls := 0
			c := resolvedClient(t, func(r *http.Request) (int, string) {
				calls++
				if response == `{"code":13}` {
					return 503, response
				}
				return 200, response
			})
			c.maxRetries = 5
			_, err := c.CreateExport(context.Background(), "10", &CreateExportRequest{})
			require.Error(t, err)
			require.Equal(t, 1, calls)
		})
	}
}

func TestPremiumCapacityNegotiatesJSONNames(t *testing.T) {
	c := resolvedClient(t, func(r *http.Request) (int, string) {
		require.Equal(t, "/v1beta2/changefeeds:listCCUs", r.URL.Path)
		if r.Header.Get("Accept") == "application/json+camel" {
			return 200, `{"ccus":[{"name":"2ccu","ccuNum":2},{"name":"4ccu","ccuNum":4}]}`
		}
		return 200, `{"ccus":[{"name":"2ccu","ccu_num":2},{"name":"4ccu","ccu_num":4}]}`
	})
	b, err := c.BoundBackend(largeID, ClusterInfo{ClusterID: largeID, Family: Premium, Environment: "prod"})
	require.NoError(t, err)
	values, err := b.Capacities(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{2, 4}, values)
}
func TestDeleteUsesVerifiedOwnershipAndWaitsForConfirmation(t *testing.T) {
	events := []string{}
	c := resolvedClient(t, func(r *http.Request) (int, string) {
		events = append(events, r.Method)
		if len(events) == 1 {
			return 200, `{"id":"cf","tidbId":"10000000000000000001","state":"RUNNING"}`
		}
		if r.Method == "DELETE" {
			return 200, `{}`
		}
		return 404, `{"code":49900004}`
	})
	b, _ := c.BoundBackend(largeID, ClusterInfo{ClusterID: largeID, Family: Premium, Environment: "prod"})
	require.NoError(t, b.DeleteChangefeed(context.Background(), "cf", true))
	require.Equal(t, []string{"GET", "DELETE", "GET"}, events)
	c = resolvedClient(t, func(r *http.Request) (int, string) {
		require.Equal(t, "GET", r.Method)
		return 200, `{"id":"cf","tidbId":"wrong","state":"RUNNING"}`
	})
	b, _ = c.BoundBackend(largeID, ClusterInfo{ClusterID: largeID, Family: Premium, Environment: "prod"})
	require.ErrorContains(t, b.DeleteChangefeed(context.Background(), "cf", true), "identity")
}
func TestDecimalRejectsPrecisionLoss(t *testing.T) {
	var n decimal
	require.NoError(t, json.Unmarshal([]byte(`18446744073709551615`), &n))
	require.Equal(t, uint64(^uint64(0)), uint64(n))
	require.Error(t, json.Unmarshal([]byte(`1.1`), &n))
	require.Error(t, json.Unmarshal([]byte(`18446744073709551616`), &n))
}

func TestVerificationFailureAfterSuccessfulPostRemainsUncertain(t *testing.T) {
	c := resolvedClient(t, func(r *http.Request) (int, string) {
		if r.Method == "POST" {
			return 200, `{"id":"cf-1","state":"CREATING"}`
		}
		return 403, `{"code":7,"message":"verification forbidden"}`
	})
	b, _ := c.BoundBackend(largeID, ClusterInfo{ClusterID: largeID, Family: Premium, Environment: "prod"})
	// Exercise the original Serverless request shape with a successful POST that
	// omits ownership, forcing the adapter to verify through GET.
	b.Cluster.Family = Serverless
	_, err := b.CreateChangefeed(context.Background(), &CreateChangefeedRequest{})
	require.Error(t, err)
	require.False(t, DefinitelyRejected(err))
}
func TestPremiumPreservesFractionalFlushInterval(t *testing.T) {
	req := &CreateChangefeedRequest{FlushInterval: 2500 * time.Millisecond, RCU: 4, Filter: &ChangefeedFilter{}, StartPosition: &StartPosition{TSO: "449"}, Sink: &Sink{CloudStorage: &CloudStorageSink{Storage: &CloudStorage{S3: &S3CloudStorage{AccessKey: &S3AccessKey{}}}, DataFormat: &CloudStorageDataFormat{CSVConfig: &CSVConfig{}}, SizeInMiB: 64}}}
	wire := premiumChangefeedFrom(req, largeID)
	require.Equal(t, "2.5s", wire.S3.Flush.Interval)
}
