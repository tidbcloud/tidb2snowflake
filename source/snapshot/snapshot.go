package snapshot

import (
	"context"
	"path"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/tidbcloud/tidb2snowflake/pkg/state"
	"github.com/tidbcloud/tidb2snowflake/source/storage"
)

func LoadExistingTSO(ctx context.Context, store storeapi.Storage, manager state.Manager) error {
	exists, err := storage.DirHasObjects(ctx, store, storage.SnapshotDirName)
	if err != nil {
		return errors.Annotate(err, "check snapshot directory")
	}
	if !exists {
		return nil
	}
	return LoadTSOFromMetadata(ctx, store, manager)
}

func LoadTSOFromMetadata(ctx context.Context, store storeapi.Storage, manager state.Manager) error {
	metadataPath := path.Join(storage.SnapshotDirName, "metadata")
	exists, err := store.FileExists(ctx, metadataPath)
	if err != nil {
		return errors.Annotatef(err, "check snapshot metadata %s", metadataPath)
	}
	if !exists {
		return errors.Errorf("snapshot data exists but %s is missing", metadataPath)
	}
	data, err := store.ReadFile(ctx, metadataPath)
	if err != nil {
		return errors.Annotatef(err, "read snapshot metadata %s", metadataPath)
	}
	tso, err := TSOFromMetadata(data)
	if err != nil {
		return errors.Annotatef(err, "parse snapshot metadata %s", metadataPath)
	}
	return manager.SetSnapshotTSO(ctx, tso)
}

func TSOFromMetadata(data []byte) (string, error) {
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "Pos:") {
			continue
		}
		tso := strings.TrimSpace(strings.TrimPrefix(line, "Pos:"))
		if tso == "" {
			return "", errors.New("metadata Pos is empty")
		}
		if _, err := strconv.ParseUint(tso, 10, 64); err != nil {
			return "", errors.Annotatef(err, "parse metadata Pos %q", tso)
		}
		return tso, nil
	}
	return "", errors.New("metadata Pos is missing")
}

func ParseTSO(tso string) (uint64, error) {
	if tso == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseUint(tso, 10, 64)
	if err != nil {
		return 0, errors.Annotatef(err, "parse snapshot TSO %q", tso)
	}
	return parsed, nil
}
