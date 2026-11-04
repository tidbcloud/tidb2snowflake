package tidbcloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type APIFamily string

const (
	Serverless APIFamily = "serverless-v1beta1"
	Premium    APIFamily = "cloud-v1beta2"
)

type ClusterInfo struct {
	ClusterID     string    `json:"cluster_id"`
	ServicePlan   string    `json:"service_plan"`
	Family        APIFamily `json:"api_family"`
	Environment   string    `json:"environment_id"`
	Region        string    `json:"-"`
	CloudProvider string    `json:"-"`
	State         string    `json:"-"`
}

// endpoints never sends credentials to a different environment on fallback.
func endpoints(host string) (map[APIFamily]string, string, string, error) {
	if host == "" {
		host = DefaultHost
	}
	for _, env := range []string{"", "staging.", "dev."} {
		serverless, cloud, dedicated := "serverless."+env+"tidbapi.com", "cloud."+env+"tidbapi.com", "dedicated."+env+"tidbapi.com"
		if host == serverless || host == cloud || host == dedicated {
			name := strings.TrimSuffix(env, ".")
			if name == "" {
				name = "prod"
			}
			return map[APIFamily]string{Serverless: serverless, Premium: cloud}, dedicated, name, nil
		}
	}
	u, err := url.Parse("https://" + host)
	if err != nil || u.Host != host || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, "", "", fmt.Errorf("invalid TiDB Cloud host")
	}
	return map[APIFamily]string{Serverless: host}, "", "custom:" + host, nil
}

func CanonicalClusterID(id string) (string, error) {
	n, err := strconv.ParseUint(id, 10, 64)
	if err != nil || n == 0 {
		return "", fmt.Errorf("invalid TiDB Cloud cluster ID: expected a nonzero uint64")
	}
	return strconv.FormatUint(n, 10), nil
}

func IsNotFound(err error) bool {
	var e *APIError
	if !errors.As(err, &e) || e.StatusCode != http.StatusNotFound {
		return false
	}
	if e.Code == 49900004 {
		return true
	}
	// The public gateway can put the HTTP code at the top level and the
	// management business code in RequestInfo. A generic 404 is not enough.
	if e.Code != http.StatusNotFound {
		return false
	}
	// The legacy Serverless job endpoints return these service-level errors
	// without RequestInfo. Keep generic gateway/route 404s unclassified.
	if len(e.Details) == 0 && (e.Message == "fail to get cdc changefeed: record not found" || e.Message == "fail to get export: record not found") {
		return true
	}
	for _, raw := range e.Details {
		var detail struct {
			Type        string `json:"@type"`
			ServingData string `json:"servingData"`
		}
		if json.Unmarshal(raw, &detail) == nil && detail.Type == "type.tidbapi.com/tidb.rpc.RequestInfo" && detail.ServingData == "49900004" {
			return true
		}
	}
	return false
}

// OpenBackend resolves a new cluster, or verifies a persisted source identity.
// Legacy task IDs must only be interpreted by the historical Serverless API.
func (c *Client) OpenBackend(ctx context.Context, id string, bound *ClusterInfo, legacy bool) (*Backend, error) {
	id, err := CanonicalClusterID(id)
	if err != nil {
		return nil, err
	}
	host := strings.TrimPrefix(c.baseURL, "https://")
	hosts, dedicated, env, err := endpoints(host)
	if err != nil {
		return nil, err
	}
	if bound != nil {
		if bound.ClusterID != id || bound.Environment != env {
			return nil, fmt.Errorf("source identity does not match configured cluster/environment")
		}
		target, ok := hosts[bound.Family]
		if !ok {
			return nil, fmt.Errorf("unsupported saved API family %q", bound.Family)
		}
		info, err := c.atHost(target).cluster(ctx, id, bound.Family, env)
		if err != nil {
			return nil, err
		}
		return &Backend{client: c.atHost(target), Cluster: info}, nil
	}
	families := []APIFamily{Serverless, Premium}
	if legacy {
		families = families[:1]
	}
	for _, family := range families {
		target, ok := hosts[family]
		if !ok {
			continue
		}
		client := c.atHost(target)
		info, err := client.cluster(ctx, id, family, env)
		if IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return &Backend{client: client, Cluster: info}, nil
	}
	if legacy {
		return nil, fmt.Errorf("legacy task cluster is not accessible through the Serverless API; verify original host and cluster ID")
	}
	if dedicated != "" {
		var out struct {
			ClusterID string `json:"clusterId"`
		}
		err = c.atHost(dedicated).probe(ctx, "/v1beta1/clusters/"+url.PathEscape(id), nil, &out)
		if err == nil {
			return nil, fmt.Errorf("Dedicated tier is not supported by tidb2snowflake")
		}
		if !IsNotFound(err) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("no accessible cluster found for this API key (custom hosts only support the Serverless API)")
}

func (c *Client) atHost(host string) *Client { n := *c; n.baseURL = "https://" + host; return &n }

func (c *Client) cluster(ctx context.Context, id string, family APIFamily, env string) (ClusterInfo, error) {
	var out struct {
		ClusterID     string `json:"clusterId"`
		TiDBID        string `json:"tidbId"`
		ServicePlan   string `json:"servicePlan"`
		State         string `json:"state"`
		CloudProvider string `json:"cloudProvider"`
		RegionID      string `json:"regionId"`
		Region        struct {
			Name          string `json:"name"`
			CloudProvider string `json:"cloudProvider"`
			RegionID      string `json:"regionId"`
		} `json:"region"`
	}
	path := "/v1beta1/clusters/" + url.PathEscape(id) + "?view=BASIC"
	if family == Premium {
		path = "/v1beta2/tidbs/" + url.PathEscape(id)
	}
	if err := c.probe(ctx, path, nil, &out); err != nil {
		return ClusterInfo{}, err
	}
	got := out.ClusterID
	if family == Premium {
		got = out.TiDBID
	}
	canonical, err := CanonicalClusterID(got)
	if err != nil || canonical != id || out.State == "" || out.State == "CLUSTER_STATE_UNSPECIFIED" {
		return ClusterInfo{}, fmt.Errorf("invalid cluster response: missing/mismatched identity or state")
	}
	valid := family == Serverless && (out.ServicePlan == "Starter" || out.ServicePlan == "Essential") || family == Premium && (out.ServicePlan == "Premium" || out.ServicePlan == "Essential_V2" || out.ServicePlan == "Premium_Reserved" || out.ServicePlan == "BYOC")
	if !valid {
		return ClusterInfo{}, fmt.Errorf("unsupported or conflicting servicePlan %q for %s", out.ServicePlan, family)
	}
	region := out.RegionID
	if family == Serverless {
		region = strings.TrimPrefix(out.Region.Name, "regions/")
		if region == "" {
			region = out.Region.RegionID
		}
	}
	provider := out.CloudProvider
	if family == Serverless {
		provider = out.Region.CloudProvider
	}
	for _, p := range []string{"aws", "alicloud", "azure", "gcp"} {
		if strings.HasPrefix(region, p+"-") {
			region = strings.TrimPrefix(region, p+"-")
			if provider == "" {
				provider = p
			}
			break
		}
	}
	if region == "" || provider == "" {
		return ClusterInfo{}, fmt.Errorf("invalid cluster response: missing region/provider")
	}
	return ClusterInfo{ClusterID: id, Family: family, Environment: env, ServicePlan: out.ServicePlan, State: out.State, Region: region, CloudProvider: provider}, nil
}

// BoundBackend is used only for already verified jobs, including cleanup after
// the source cluster is paused or gone. It does not require a cluster GET.
func (c *Client) BoundBackend(id string, bound ClusterInfo) (*Backend, error) {
	id, err := CanonicalClusterID(id)
	if err != nil {
		return nil, err
	}
	hosts, _, env, err := endpoints(strings.TrimPrefix(c.baseURL, "https://"))
	if err != nil {
		return nil, err
	}
	target, ok := hosts[bound.Family]
	if !ok || id != bound.ClusterID || env != bound.Environment {
		return nil, fmt.Errorf("source identity does not match configured cluster/environment")
	}
	return &Backend{client: c.atHost(target), Cluster: bound}, nil
}

// A not-found cluster is expected while discovering the API family. Other
// failures retain the normal request diagnostics.
func (c *Client) probe(ctx context.Context, path string, body, out any) error {
	probe := *c
	probe.quietNotFound = true
	return probe.do(ctx, http.MethodGet, path, body, out)
}
