package replicate

import (
	"testing"

	"github.com/pingcap/tiflow/pkg/sink/cloudstorage"
	"github.com/stretchr/testify/require"
)

func TestCheckpointPath(t *testing.T) {
	require.Equal(t, "db/t/1/2026-06-19/CDC000001.checkpoint",
		checkpointPath("db/t/1/2026-06-19/CDC000001.csv", CSVFileExtension))
}

func TestCheckpointExistsInSet(t *testing.T) {
	checkpoints := map[string]struct{}{
		"db/t/1/2026-06-19/CDC000001.checkpoint": {},
	}
	require.True(t, checkpointExistsInSet("db/t/1/2026-06-19/CDC000001.csv", CSVFileExtension, checkpoints))
	require.False(t, checkpointExistsInSet("db/t/1/2026-06-19/CDC000002.csv", CSVFileExtension, checkpoints))
}

func TestProgressEntryRoundTrip(t *testing.T) {
	key := cloudstorage.DmlPathKey{
		SchemaPathKey: cloudstorage.SchemaPathKey{
			Schema:       "db",
			Table:        "tbl",
			TableVersion: 42,
		},
		PartitionNum: 7,
		Date:         "2026-06-19",
	}
	entry := progressEntryFromDMLKey(key, 9)
	require.Equal(t, uint64(9), entry.FileIndex)
	require.Equal(t, key, progressEntryToDMLKey(entry))
}
