package storage

import (
	"context"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/stretchr/testify/require"
)

func testCred() *credentials.Value {
	return &credentials.Value{AccessKeyID: "AKIA", SecretAccessKey: "secret"}
}

func TestCleanSubURI(t *testing.T) {
	uri, err := url.Parse("s3://bucket/path%25?access-key=AKIA&secret-access-key=secret")
	require.NoError(t, err)
	store := &Storage{uri: uri}
	snap := store.CleanSubURI(SnapshotDirName)
	incr := store.CleanSubURI(IncrementDirName)
	require.Equal(t, "s3://bucket/path%25/snapshot/", snap)
	require.Equal(t, "s3://bucket/path%25/increment/", incr)
	require.NotContains(t, snap, "access-key")
	require.NotContains(t, incr, "secret")
}

func TestGetS3URIWithCredentials(t *testing.T) {
	uri, err := GetS3URIWithCredentials("s3://bucket/path?region=us-west-2", testCred())
	require.NoError(t, err)
	require.Equal(t, "s3", uri.Scheme)
	require.Equal(t, "us-west-2", uri.Query().Get("region"))
	require.Contains(t, uri.RawQuery, "access-key=AKIA")
	require.Contains(t, uri.RawQuery, "secret-access-key=secret")

	_, err = GetS3URIWithCredentials("gcs://bucket/path", testCred())
	require.Error(t, err)
}

func TestNewS3ClientDetectsRegionWhenMissing(t *testing.T) {
	old := getS3BucketRegion
	defer func() {
		getS3BucketRegion = old
	}()
	called := false
	getS3BucketRegion = func(_ aws.Context, _ client.ConfigProvider, bucket, hint string, _ ...request.Option) (string, error) {
		called = true
		require.Equal(t, "bucket", bucket)
		require.Equal(t, defaultS3RegionHint, hint)
		return "us-west-2", nil
	}

	uri, err := url.Parse("s3://bucket/path?access-key=AKIA&secret-access-key=secret")
	require.NoError(t, err)

	client, err := newS3Client(context.Background(), uri)
	require.NoError(t, err)
	require.True(t, called)
	require.Equal(t, "us-west-2", aws.StringValue(client.Config.Region))
}

func TestNewS3ClientUsesConfiguredRegion(t *testing.T) {
	old := getS3BucketRegion
	defer func() {
		getS3BucketRegion = old
	}()
	getS3BucketRegion = func(aws.Context, client.ConfigProvider, string, string, ...request.Option) (string, error) {
		t.Fatal("region detector should not be called when region is configured")
		return "", nil
	}

	uri, err := url.Parse("s3://bucket/path?region=eu-central-1&access-key=AKIA&secret-access-key=secret")
	require.NoError(t, err)

	client, err := newS3Client(context.Background(), uri)
	require.NoError(t, err)
	require.Equal(t, "eu-central-1", aws.StringValue(client.Config.Region))
}

func TestNewS3ClientUsesDefaultRegionForCustomEndpoint(t *testing.T) {
	old := getS3BucketRegion
	defer func() {
		getS3BucketRegion = old
	}()
	getS3BucketRegion = func(aws.Context, client.ConfigProvider, string, string, ...request.Option) (string, error) {
		t.Fatal("region detector should not be called for custom endpoints")
		return "", nil
	}

	uri, err := url.Parse("s3://bucket/path?endpoint=http%3A%2F%2F127.0.0.1%3A9000&access-key=AKIA&secret-access-key=secret")
	require.NoError(t, err)

	client, err := newS3Client(context.Background(), uri)
	require.NoError(t, err)
	require.Equal(t, defaultS3RegionHint, aws.StringValue(client.Config.Region))
	require.Equal(t, "http://127.0.0.1:9000", aws.StringValue(client.Config.Endpoint))
	require.True(t, aws.BoolValue(client.Config.S3ForcePathStyle))
}

func TestDirHasObjects(t *testing.T) {
	ctx := context.Background()
	store, err := New(ctx, &url.URL{Scheme: "file", Path: t.TempDir()})
	require.NoError(t, err)
	defer store.Close()

	has, err := store.DirHasObjects(ctx, SnapshotDirName)
	require.NoError(t, err)
	require.False(t, has)

	require.NoError(t, store.WriteFile(ctx, SnapshotDirName+"/db1.t1.000000.csv", []byte("a,b\n")))

	has, err = store.DirHasObjects(ctx, SnapshotDirName)
	require.NoError(t, err)
	require.True(t, has)

	has, err = store.DirHasObjects(ctx, IncrementDirName)
	require.NoError(t, err)
	require.False(t, has)

	require.NoError(t, store.WriteFile(ctx, "metadata.json", []byte("{}")))
	has, err = store.DirHasObjects(ctx, IncrementDirName)
	require.NoError(t, err)
	require.False(t, has)
}

func TestListDirs(t *testing.T) {
	ctx := context.Background()
	store, err := New(ctx, &url.URL{Scheme: "file", Path: t.TempDir()})
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.WriteFile(ctx, "increment/db/t/100/2026-06-29/meta/CDC.index", []byte("CDC000000000000001.csv")))
	require.NoError(t, store.WriteFile(ctx, "increment/db/t/100/2026-06-30/meta/CDC.index", []byte("CDC000000000000002.csv")))
	require.NoError(t, store.WriteFile(ctx, "increment/db/t/100/CDC000000000000003.csv", []byte("ignored")))

	dirs, err := store.ListDirs(ctx, "increment/db/t/100")
	require.NoError(t, err)
	require.Equal(t, []string{"2026-06-29", "2026-06-30"}, dirs)
}
