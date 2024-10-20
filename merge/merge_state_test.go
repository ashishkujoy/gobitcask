package merge

import (
	log "ashishkujoy/bitcask/kv/log"
	"testing"

	"github.com/stretchr/testify/require"
)

type serializableKey string

func (s serializableKey) Serialize() []byte {
	return []byte(s)
}

func TestMergeDistinctKeyValues(t *testing.T) {
	mergedState := NewMergedState[serializableKey]()
	entry := &log.MappedStoredEntry[serializableKey]{
		Key:     "topic",
		Value:   []byte("Database Systems"),
		Deleted: false,
	}
	otherEntry := &log.MappedStoredEntry[serializableKey]{
		Key:     "disk",
		Value:   []byte("ssd"),
		Deleted: false,
	}
	mergedState.merge(
		[]*log.MappedStoredEntry[serializableKey]{entry},
		[]*log.MappedStoredEntry[serializableKey]{otherEntry},
	)
	_, ok := mergedState.valueByKey["topic"]
	require.True(t, ok)

	_, ok = mergedState.valueByKey["disk"]
	require.True(t, ok)
}

func TestMergeDeleteEntryOfHigherTimestampValue(t *testing.T) {
	mergedState := NewMergedState[serializableKey]()
	entry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte("Database Systems"),
		Deleted:   false,
		Timestamp: 0,
	}
	deleteEntry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte(""),
		Deleted:   true,
		Timestamp: 1,
	}
	mergedState.merge(
		[]*log.MappedStoredEntry[serializableKey]{entry},
		[]*log.MappedStoredEntry[serializableKey]{deleteEntry},
	)
	_, ok := mergedState.valueByKey["topic"]
	require.False(t, ok)
}

func TestMergeWithDeletionInTheFirstSet(t *testing.T) {
	mergedState := NewMergedState[serializableKey]()
	entry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte("Database Systems"),
		Deleted:   true,
		Timestamp: 0,
	}
	deleteEntry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte(""),
		Deleted:   false,
		Timestamp: 1,
	}

	mergedState.merge(
		[]*log.MappedStoredEntry[serializableKey]{entry},
		[]*log.MappedStoredEntry[serializableKey]{deleteEntry},
	)

	_, ok := mergedState.valueByKey["topic"]
	require.True(t, ok)

	_, ok = mergedState.deletedKeys["topic"]
	require.False(t, ok)
}

func TestMergeWithDeletionInTheFirstSetHavingHighTimestamp(t *testing.T) {
	mergedState := NewMergedState[serializableKey]()
	entry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte("Database Systems"),
		Deleted:   true,
		Timestamp: 10,
	}
	deleteEntry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte(""),
		Deleted:   false,
		Timestamp: 1,
	}

	mergedState.merge(
		[]*log.MappedStoredEntry[serializableKey]{entry},
		[]*log.MappedStoredEntry[serializableKey]{deleteEntry},
	)

	_, ok := mergedState.valueByKey["topic"]
	require.False(t, ok)
}
func TestMergeWithDeletionWithoutSameEntry(t *testing.T) {
	mergedState := NewMergedState[serializableKey]()
	entry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte("Database Systems"),
		Deleted:   false,
		Timestamp: 10,
	}
	deleteEntry := &log.MappedStoredEntry[serializableKey]{
		Key:       "disk",
		Value:     []byte(""),
		Deleted:   true,
		Timestamp: 1,
	}

	mergedState.merge(
		[]*log.MappedStoredEntry[serializableKey]{entry},
		[]*log.MappedStoredEntry[serializableKey]{deleteEntry},
	)

	topicEntry := mergedState.valueByKey["topic"]
	require.False(t, topicEntry.Deleted)
	diskEntry := mergedState.valueByKey["disk"]
	require.True(t, diskEntry.Deleted)
}

func TestMergeWithUpdate(t *testing.T) {
	mergedState := NewMergedState[serializableKey]()
	entry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte("microservices"),
		Deleted:   false,
		Timestamp: 0,
	}
	otherEntry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte("bitcask"),
		Deleted:   false,
		Timestamp: 1,
	}

	mergedState.merge([]*log.MappedStoredEntry[serializableKey]{entry}, []*log.MappedStoredEntry[serializableKey]{otherEntry})

	topicEntry := mergedState.valueByKey["topic"]
	require.Equal(t, string("bitcask"), string(topicEntry.Value))
}

func TestMergeWithUpdateInTheFirstSetHavingHighTimestamp(t *testing.T) {
	mergedState := NewMergedState[serializableKey]()
	entry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte("microservices"),
		Deleted:   false,
		Timestamp: 1,
	}
	otherEntry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte("bitcask"),
		Deleted:   false,
		Timestamp: 0,
	}
	mergedState.merge([]*log.MappedStoredEntry[serializableKey]{entry}, []*log.MappedStoredEntry[serializableKey]{otherEntry})

	topicEntry := mergedState.valueByKey["topic"]
	require.Equal(t, string("microservices"), string(topicEntry.Value))
}
