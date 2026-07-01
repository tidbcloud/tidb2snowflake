package storage

import (
	"context"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
)

const (
	SnapshotDirName  = "snapshot"
	IncrementDirName = "increment"
	CSVFileExtension = ".csv"
	defaultS3Region  = "us-east-1"
)

type Storage struct {
	storeapi.Storage
	uri      *url.URL
	s3Client *s3.Client
}

func New(ctx context.Context, uri *url.URL) (*Storage, error) {
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, uri.String())
	if err != nil {
		return nil, errors.Annotate(err, "open storage")
	}
	storage := &Storage{
		Storage: store,
		uri:     uri,
	}
	if uri.Scheme == "s3" {
		client, err := newS3Client(ctx, uri)
		if err != nil {
			store.Close()
			return nil, errors.Trace(err)
		}
		storage.s3Client = client
	}
	return storage, nil
}

func GetS3URIWithCredentials(storagePath string, cred *aws.Credentials) (*url.URL, error) {
	uri, err := url.Parse(storagePath)
	if err != nil {
		return nil, errors.Annotate(err, "parse storage path")
	}
	if uri.Scheme != "s3" {
		return nil, errors.New("only s3 storage is supported")
	}
	values := uri.Query()
	values.Set("access-key", cred.AccessKeyID)
	values.Set("secret-access-key", cred.SecretAccessKey)
	if cred.SessionToken != "" {
		values.Set("session-token", cred.SessionToken)
	}
	uri.RawQuery = values.Encode()
	return uri, nil
}

func (s *Storage) CleanSubURI(subDir string) string {
	uri := *s.uri
	cleanURI := uri.JoinPath(subDir)
	cleanURI.RawQuery = ""
	return s3URIWithTrailingSlash(cleanURI)
}

func (s *Storage) DirHasObjects(ctx context.Context, subDir string) (bool, error) {
	found := false
	err := s.WalkDir(ctx, &storeapi.WalkOption{SubDir: subDir, ListCount: 1}, func(string, int64) error {
		found = true
		return errWalkStop
	})
	if found {
		return true, nil
	}
	if err != nil && errors.Cause(err) != errWalkStop {
		return false, errors.Trace(err)
	}
	return false, nil
}

func (s *Storage) ListDirs(ctx context.Context, subDir string) ([]string, error) {
	switch s.uri.Scheme {
	case "file", "local":
		return s.listLocalDirs(subDir)
	case "s3":
		return s.listS3Dirs(ctx, subDir)
	default:
	}
	log.Panic("unsupported storage scheme", zap.String("scheme", s.uri.Scheme))
	return nil, nil
}

func (s *Storage) listLocalDirs(subDir string) ([]string, error) {
	dir := filepath.Join(s.uri.Path, filepath.FromSlash(subDir))
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	dirs := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, entry.Name())
		}
	}
	slices.Sort(dirs)
	return dirs, nil
}

func (s *Storage) listS3Dirs(ctx context.Context, subDir string) ([]string, error) {
	prefix := s.s3Prefix(subDir)
	input := &s3.ListObjectsV2Input{
		Bucket:    aws.String(s.uri.Host),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}
	dirs := make([]string, 0)
	for {
		output, err := s.s3Client.ListObjectsV2(ctx, input)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, commonPrefix := range output.CommonPrefixes {
			dir := strings.TrimPrefix(aws.ToString(commonPrefix.Prefix), prefix)
			dir = strings.Trim(dir, "/")
			if dir != "" {
				dirs = append(dirs, dir)
			}
		}
		if !aws.ToBool(output.IsTruncated) {
			break
		}
		input.ContinuationToken = output.NextContinuationToken
	}
	slices.Sort(dirs)
	return dirs, nil
}

func (s *Storage) s3Prefix(subDir string) string {
	base := strings.Trim(s.uri.EscapedPath(), "/")
	subDir = strings.Trim(subDir, "/")
	prefix := path.Join(base, subDir)
	if prefix != "" {
		prefix += "/"
	}
	return prefix
}

func newS3Client(ctx context.Context, uri *url.URL) (*s3.Client, error) {
	values := uri.Query()
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(s3Region(values)),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			values.Get("access-key"),
			values.Get("secret-access-key"),
			values.Get("session-token"),
		)),
	)
	if err != nil {
		return nil, errors.Trace(err)
	}

	opts := s3ClientOptions(values)
	client := s3.NewFromConfig(cfg, opts...)
	if configuredS3Region(values) != "" || !shouldDetectS3BucketRegion(values) {
		return client, nil
	}

	region, err := manager.GetBucketRegion(ctx, client, uri.Host, func(o *s3.Options) {
		if client.Options().Credentials != nil {
			o.Credentials = client.Options().Credentials
		}
		if s3Endpoint(values) != "" {
			o.UsePathStyle = client.Options().UsePathStyle
		}
	})
	if err != nil {
		return nil, errors.Annotatef(err, "detect s3 bucket region %s", uri.Host)
	}
	if region == "" {
		region = defaultS3Region
	}
	if region != cfg.Region {
		cfg.Region = region
		client = s3.NewFromConfig(cfg, opts...)
	}
	return client, nil
}

func s3ClientOptions(values url.Values) []func(*s3.Options) {
	var opts []func(*s3.Options)
	if endpoint := s3Endpoint(values); endpoint != "" {
		opts = append(opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}
	return opts
}

func configuredS3Region(values url.Values) string {
	if region := values.Get("s3.region"); region != "" {
		return region
	}
	return values.Get("region")
}

func s3Region(values url.Values) string {
	if region := configuredS3Region(values); region != "" {
		return region
	}
	return defaultS3Region
}

func shouldDetectS3BucketRegion(values url.Values) bool {
	provider := values.Get("s3.provider")
	if provider == "" {
		provider = values.Get("provider")
	}
	if provider != "" && provider != "aws" {
		return false
	}
	return s3Endpoint(values) == "" || provider == "aws"
}

func s3Endpoint(values url.Values) string {
	if endpoint := values.Get("s3.endpoint"); endpoint != "" {
		return endpoint
	}
	return values.Get("endpoint")
}

func s3URIWithTrailingSlash(uri *url.URL) string {
	out := uri.String()
	if !strings.HasSuffix(out, "/") {
		out += "/"
	}
	return out
}

var errWalkStop = errors.New("stop walk")
