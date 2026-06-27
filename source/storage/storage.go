package storage

import (
	"context"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

const (
	SnapshotDirName  = "snapshot"
	IncrementDirName = "increment"
)

func GetS3URIWithCredentials(storagePath string, cred *credentials.Value) (*url.URL, error) {
	uri, err := url.Parse(storagePath)
	if err != nil {
		return nil, errors.Annotate(err, "parse storage path")
	}
	if uri.Scheme != "s3" {
		return nil, errors.New("only s3 storage is supported")
	}
	values := url.Values{}
	values.Add("access-key", cred.AccessKeyID)
	values.Add("secret-access-key", cred.SecretAccessKey)
	if cred.SessionToken != "" {
		values.Add("session-token", cred.SessionToken)
	}
	uri.RawQuery = values.Encode()
	return uri, nil
}

func CleanSubURI(storagePath, subDir string) (string, error) {
	uri, err := url.Parse(storagePath)
	if err != nil {
		return "", errors.Annotate(err, "parse storage path")
	}
	uri = uri.JoinPath(subDir)
	uri.RawQuery = ""
	return s3URIWithTrailingSlash(uri), nil
}

func s3URIWithTrailingSlash(uri *url.URL) string {
	out := uri.String()
	if !strings.HasSuffix(out, "/") {
		out += "/"
	}
	return out
}

var errWalkStop = errors.New("stop walk")

func DirHasObjects(ctx context.Context, store storeapi.Storage, subDir string) (bool, error) {
	found := false
	err := store.WalkDir(ctx, &storeapi.WalkOption{SubDir: subDir, ListCount: 1}, func(string, int64) error {
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
