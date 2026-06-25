package incremental

import (
	"testing"

	"github.com/pingcap/ticdc/pkg/cloudstorage"
	"github.com/stretchr/testify/require"
)

func TestDiffDMLMaps(t *testing.T) {
	key := cloudstorage.DMLPathKey{
		SchemaPathKey: cloudstorage.SchemaPathKey{
			Schema:       "db",
			Table:        "tbl",
			TableVersion: 42,
		},
		PartitionNum: 7,
		Date:         "2026-06-19",
	}
	fileIndexKey := cloudstorage.FileIndexKey{}

	got := diffDMLMaps(
		map[cloudstorage.DMLPathKey]fileIndexKeyMap{key: {fileIndexKey: 9}},
		map[cloudstorage.DMLPathKey]fileIndexKeyMap{key: {fileIndexKey: 6}},
	)

	require.Equal(t, fileIndexRange{fileIndexKey: {start: 7, end: 9}}, got[key])
}

func TestCountFilesInRangesSkipsSchemaKeys(t *testing.T) {
	schemaKey := cloudstorage.SchemaPathKey{
		Schema:       "db",
		Table:        "tbl",
		TableVersion: 42,
	}
	dmlKey := cloudstorage.DMLPathKey{
		SchemaPathKey: schemaKey,
		PartitionNum:  7,
		Date:          "2026-06-19",
	}
	fileIndexKey := cloudstorage.FileIndexKey{}

	got := countFilesInRanges(map[cloudstorage.DMLPathKey]fileIndexRange{
		cloudstorage.NewSchemaFileDMLPathKey(schemaKey): {fileIndexKey: {start: 1, end: 1}},
		dmlKey: {fileIndexKey: {start: 2, end: 4}},
	})

	require.Equal(t, uint64(3), got)
}
