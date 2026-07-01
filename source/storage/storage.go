package storage

import (
	"context"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
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

	defaultS3RegionHint = "us-east-1"
)

type Storage struct {
	storeapi.Storage
	uri      *url.URL
	s3Client *s3.S3
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

func GetS3URIWithCredentials(storagePath string, cred *credentials.Value) (*url.URL, error) {
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
		output, err := s.s3Client.ListObjectsV2WithContext(ctx, input)
		if err != nil {
			return nil, errors.Trace(err)
		}
		for _, commonPrefix := range output.CommonPrefixes {
			dir := strings.TrimPrefix(aws.StringValue(commonPrefix.Prefix), prefix)
			dir = strings.Trim(dir, "/")
			if dir != "" {
				dirs = append(dirs, dir)
			}
		}
		if !aws.BoolValue(output.IsTruncated) {
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

type s3BucketRegionDetector func(aws.Context, client.ConfigProvider, string, string, ...request.Option) (string, error)

var getS3BucketRegion s3BucketRegionDetector = s3manager.GetBucketRegion

func newS3Client(ctx context.Context, uri *url.URL) (*s3.S3, error) {
	values := uri.Query()
	config := aws.NewConfig().WithCredentials(credentials.NewStaticCredentials(
		values.Get("access-key"),
		values.Get("secret-access-key"),
		values.Get("session-token"),
	))
	endpoint := s3Endpoint(values)
	region := s3Region(values)
	if region == "" {
		if endpoint != "" {
			region = defaultS3RegionHint
		} else {
			detected, err := detectS3BucketRegion(ctx, config, uri.Host)
			if err != nil {
				return nil, errors.Annotate(err, "detect s3 bucket region")
			}
			region = detected
		}
	}
	config.WithRegion(region)
	if endpoint != "" {
		config.WithEndpoint(endpoint).WithS3ForcePathStyle(true)
	}
	sess, err := session.NewSession(config)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return s3.New(sess), nil
}

func detectS3BucketRegion(ctx context.Context, config *aws.Config, bucket string) (string, error) {
	discoveryConfig := config.Copy().WithRegion(defaultS3RegionHint)
	sess, err := session.NewSession(discoveryConfig)
	if err != nil {
		return "", errors.Trace(err)
	}
	region, err := getS3BucketRegion(ctx, sess, bucket, defaultS3RegionHint)
	if err != nil {
		return "", errors.Trace(err)
	}
	return region, nil
}

func s3Region(values url.Values) string {
	if region := values.Get("s3.region"); region != "" {
		return region
	}
	return values.Get("region")
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
