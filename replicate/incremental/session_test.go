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

	got := diffDMLMaps(
		map[cloudstorage.DMLPathKey]uint64{key: 9},
		map[cloudstorage.DMLPathKey]uint64{key: 6},
	)

	require.Equal(t, fileIndexRange{start: 7, end: 9}, got[key])
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

	got := countFilesInRanges(map[cloudstorage.DMLPathKey]fileIndexRange{
		cloudstorage.NewSchemaFileDMLPathKey(schemaKey): {start: 1, end: 1},
		dmlKey: {start: 2, end: 4},
	})

	require.Equal(t, uint64(3), got)
}
