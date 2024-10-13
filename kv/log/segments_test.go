package kv

import (
	"ashishkujoy/bitcask/clock"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendAndReadOnActiveSegment(t *testing.T) {
	segments, _ := NewSegments[serializableKey](os.TempDir(), 100, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	appendResponse, err := segments.Append("Key1", []byte("Value 1"))
	require.NoError(t, err)

	storedEntry, err := segments.Read(appendResponse.FileId, appendResponse.Offset, appendResponse.EntryLength)
	require.NoError(t, err)
	require.Equal(t, string(storedEntry.Key), "Key1")
	require.Equal(t, string(storedEntry.Value), "Value 1")
}

func TestAppendSegmentInvolvingRollover(t *testing.T) {
	segments, _ := NewSegments[serializableKey](os.TempDir(), 30, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	appendResponse1, _ := segments.Append("Key1", []byte("This is a long value to store in the segment"))
	appendResponse2, _ := segments.Append("Key2", []byte("V2"))
	appendResponse3, _ := segments.Append("Key3", []byte("V3"))

	require.NotEqual(t, appendResponse1.FileId, appendResponse2.FileId)
	require.Equal(t, appendResponse3.FileId, appendResponse2.FileId)

	storeEntry1, _ := segments.Read(appendResponse1.FileId, appendResponse1.Offset, appendResponse1.EntryLength)
	storeEntry2, _ := segments.Read(appendResponse2.FileId, appendResponse2.Offset, appendResponse2.EntryLength)
	storeEntry3, _ := segments.Read(appendResponse3.FileId, appendResponse3.Offset, appendResponse3.EntryLength)

	require.Equal(t, string(storeEntry1.Key), "Key1")
	require.Equal(t, string(storeEntry2.Key), "Key2")
	require.Equal(t, string(storeEntry3.Key), "Key3")
}

func TestAttemptToReadNonExistingSegment(t *testing.T) {
	segments, _ := NewSegments[serializableKey](os.TempDir(), 100, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	_, err := segments.Read(212, 0, 10)

	require.EqualError(t, err, "invalid fileId 212")
}

func TestReadASegmentWithDeleteEntry(t *testing.T) {
	segments, _ := NewSegments[serializableKey](os.TempDir(), 100, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	response, _ := segments.AppendDelete("Key1")
	storedEntry, _ := segments.Read(response.FileId, response.Offset, response.EntryLength)

	require.True(t, storedEntry.Deleted)
	require.Equal(t, string(storedEntry.Key), "Key1")
}

func TestReadsAPairOfInactiveSegmentFull(t *testing.T) {
	segments, _ := NewSegments[serializableKey](os.TempDir(), 8, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	_, _ = segments.Append("topic", []byte("microservices"))
	_, _ = segments.Append("diskType", []byte("solid state drive"))
	_, _ = segments.Append("engine", []byte("bitcask"))

	_, pair, _ := segments.ReadInactiveSegments(2, func(b []byte) serializableKey {
		return serializableKey(string(b))
	})

	require.Equal(t, string(pair[0][0].Key), "topic")
	require.Equal(t, string(pair[1][0].Key), "diskType")
}

func TestReadAllInactiveSegmentsFull(t *testing.T) {
	segments, _ := NewSegments[serializableKey](os.TempDir(), 8, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	_, _ = segments.Append("topic", []byte("microservices"))
	_, _ = segments.Append("diskType", []byte("solid state drive"))
	_, _ = segments.Append("engine", []byte("bitcask"))
	_, _ = segments.Append("language", []byte("go language"))

	_, pairs, _ := segments.ReadAllInactiveSegments(func(b []byte) serializableKey {
		return serializableKey(string(b))
	})

	require.Equal(t, 3, len(pairs))
}

func TestWriteBackInvolvingRollover(t *testing.T) {
	segments, _ := NewSegments[serializableKey](os.TempDir(), 8, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	changes := make(map[serializableKey]*MappedStoredEntry[serializableKey])
	changes["disk"] = &MappedStoredEntry[serializableKey]{Key: "disk", Value: []byte("Solid State Drive")}
	changes["engine"] = &MappedStoredEntry[serializableKey]{Key: "engine", Value: []byte("Bitcask Dummy Engine")}
	changes["topic"] = &MappedStoredEntry[serializableKey]{Key: "topic", Value: []byte("Microservices")}

	_, _ = segments.WriteBack(changes)
	allKeys := allInactiveSegmentsKeys(segments)
	expectedKeys := []serializableKey{"disk", "engine", "topic"}

	require.Equal(t, allKeys, expectedKeys)
}

func TestWriteBackNotInvolvingRollover(t *testing.T) {
	segments, _ := NewSegments[serializableKey](os.TempDir(), 400, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	changes := make(map[serializableKey]*MappedStoredEntry[serializableKey])
	changes["disk"] = &MappedStoredEntry[serializableKey]{Key: "disk", Value: []byte("Solid State Drive")}
	changes["engine"] = &MappedStoredEntry[serializableKey]{Key: "engine", Value: []byte("Bitcask Dummy Engine")}
	changes["topic"] = &MappedStoredEntry[serializableKey]{Key: "topic", Value: []byte("Microservices")}

	_, _ = segments.WriteBack(changes)
	allKeys := allInactiveSegmentsKeys(segments)
	expectedKeys := []serializableKey{"disk", "engine", "topic"}

	require.Equal(t, allKeys, expectedKeys)
}

func TestRemoveInactiveSegmentById(t *testing.T) {
	segments, _ := NewSegments[serializableKey](os.TempDir(), 8, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	appendResponse, _ := segments.Append("topic", []byte("Databases"))
	_, _ = segments.Append("disktype", []byte("Solid State Disk"))
	_, _ = segments.Append("databaseType", []byte("KV"))

	_, ok := segments.inactiveSegments[appendResponse.FileId]
	require.True(t, ok)

	segments.Remove([]uint64{appendResponse.FileId})

	_, ok = segments.inactiveSegments[appendResponse.FileId]
	require.False(t, ok)
}

func allInactiveSegmentsKeys(segments *Segments[serializableKey]) []serializableKey {
	var allKeys []serializableKey
	for _, segment := range segments.inactiveSegments {
		entries, _ := segment.ReadFull(func(b []byte) serializableKey { return serializableKey(b) })
		for _, entry := range entries {
			allKeys = append(allKeys, entry.Key)
		}
	}
	sort.SliceStable(allKeys, func(i, j int) bool {
		return allKeys[i] < allKeys[j]
	})
	return allKeys
}
