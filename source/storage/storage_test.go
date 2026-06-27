package storage

import (
	"context"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/stretchr/testify/require"
)

func testCred() *credentials.Value {
	return &credentials.Value{AccessKeyID: "AKIA", SecretAccessKey: "secret"}
}

func TestCleanSubURI(t *testing.T) {
	snap, err := CleanSubURI("s3://bucket/path%25?access-key=AKIA&secret-access-key=secret", SnapshotDirName)
	require.NoError(t, err)
	incr, err := CleanSubURI("s3://bucket/path%25?access-key=AKIA&secret-access-key=secret", IncrementDirName)
	require.NoError(t, err)
	require.Equal(t, "s3://bucket/path%25/snapshot/", snap)
	require.Equal(t, "s3://bucket/path%25/increment/", incr)
	require.NotContains(t, snap, "access-key")
	require.NotContains(t, incr, "secret")
}

func TestGetS3URIWithCredentials(t *testing.T) {
	uri, err := GetS3URIWithCredentials("s3://bucket/path", testCred())
	require.NoError(t, err)
	require.Equal(t, "s3", uri.Scheme)
	require.Contains(t, uri.RawQuery, "access-key=AKIA")
	require.Contains(t, uri.RawQuery, "secret-access-key=secret")

	_, err = GetS3URIWithCredentials("gcs://bucket/path", testCred())
	require.Error(t, err)
}

func TestDirHasObjects(t *testing.T) {
	ctx := context.Background()
	store, err := util.GetExternalStorageWithDefaultTimeout(ctx, (&url.URL{Scheme: "file", Path: t.TempDir()}).String())
	require.NoError(t, err)

	has, err := DirHasObjects(ctx, store, SnapshotDirName)
	require.NoError(t, err)
	require.False(t, has)

	require.NoError(t, store.WriteFile(ctx, SnapshotDirName+"/db1.t1.000000.csv", []byte("a,b\n")))

	has, err = DirHasObjects(ctx, store, SnapshotDirName)
	require.NoError(t, err)
	require.True(t, has)

	has, err = DirHasObjects(ctx, store, IncrementDirName)
	require.NoError(t, err)
	require.False(t, has)

	require.NoError(t, store.WriteFile(ctx, "metadata.json", []byte("{}")))
	has, err = DirHasObjects(ctx, store, IncrementDirName)
	require.NoError(t, err)
	require.False(t, has)
}
