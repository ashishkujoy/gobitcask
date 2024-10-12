package kv

import (
	"ashishkujoy/bitcask/clock"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewSegmentWithOneEntry(t *testing.T) {
	segment, err := NewSegment[serializableKey](0, os.TempDir())

	require.NoError(t, err)
	defer func() {
		segment.remove()
	}()

	entry := NewEntry[serializableKey]("Topic", []byte("Bitcask DB"), clock.NewSystemClock())
	appendEntryResponse, err := segment.append(entry)
	require.NoError(t, err)

	storedEntry, err := segment.read(appendEntryResponse.Offset, appendEntryResponse.EntryLength)
	require.NoError(t, err)
	require.Equal(t, string(storedEntry.Key), "Topic")
	require.Equal(t, string(storedEntry.Value), "Bitcask DB")
	require.False(t, storedEntry.Deleted)
}

func TestNewSegmentWithOneEntryAndPerformSync(t *testing.T) {
	segment, _ := NewSegment[serializableKey](1, os.TempDir())
	defer func() {
		segment.remove()
	}()

	entry := NewEntry[serializableKey]("Topic", []byte("Bitcask DB"), clock.NewSystemClock())
	appendEntryResponse, _ := segment.append(entry)
	segment.sync()

	storedEntry, err := segment.read(appendEntryResponse.Offset, appendEntryResponse.EntryLength)
	require.NoError(t, err)
	require.Equal(t, string(storedEntry.Key), "Topic")
	require.Equal(t, string(storedEntry.Value), "Bitcask DB")
	require.False(t, storedEntry.Deleted)
}

func TestNewSegmentWithTwoEntries(t *testing.T) {
	segment, err := NewSegment[serializableKey](2, os.TempDir())

	require.NoError(t, err)
	defer func() {
		segment.remove()
	}()
	_, _ = segment.append(NewEntry[serializableKey]("Key1", []byte("Value1"), clock.NewSystemClock()))
	appendResponse, _ := segment.append(NewEntry[serializableKey]("Key2", []byte("Value2"), clock.NewSystemClock()))

	storedEntry, err := segment.read(appendResponse.Offset, appendResponse.EntryLength)
	require.NoError(t, err)
	require.Equal(t, string(storedEntry.Key), "Key2")
	require.Equal(t, string(storedEntry.Value), "Value2")
	require.False(t, storedEntry.Deleted)
}

func TestNewSegmentWithTwoEntriesAndValidateOffsets(t *testing.T) {
	segment, err := NewSegment[serializableKey](3, os.TempDir())

	require.NoError(t, err)
	defer func() {
		segment.remove()
	}()
	appendResponse1, _ := segment.append(NewEntry[serializableKey]("Key1", []byte("Value1"), clock.NewSystemClock()))
	appendResponse2, _ := segment.append(NewEntry[serializableKey]("Key2", []byte("Value2"), clock.NewSystemClock()))

	require.Equal(t, appendResponse1.Offset, int64(0))
	require.Equal(t, appendResponse2.Offset, int64(appendResponse1.EntryLength))
}

func TestNewSegmentWithDeleteEntry(t *testing.T) {
	segment, _ := NewSegment[serializableKey](4, os.TempDir())
	defer func() {
		segment.remove()
	}()
	appendResponse, _ := segment.append(NewDeleteEntry[serializableKey]("Key", clock.NewSystemClock()))

	storedEntry, _ := segment.read(appendResponse.Offset, appendResponse.EntryLength)

	require.Equal(t, "Key", string(storedEntry.Key))
	require.True(t, storedEntry.Deleted)
}

func TestNewSegmentByReadingAllEntries(t *testing.T) {
	segment, _ := NewSegment[serializableKey](5, os.TempDir())
	defer func() {
		segment.remove()
	}()

	_, _ = segment.append(NewEntry[serializableKey]("Key1", []byte("Value1"), clock.NewSystemClock()))
	_, _ = segment.append(NewEntry[serializableKey]("Key2", []byte("Value2"), clock.NewSystemClock()))

	entries, _ := segment.ReadFull(func(b []byte) serializableKey {
		return serializableKey(string(b))
	})

	require.Equal(t, string(entries[0].Key), "Key1")
	require.Equal(t, string(entries[1].Key), "Key2")
}

func TestNewSegmentAfterStoppingWrites(t *testing.T) {
	segment, _ := NewSegment[serializableKey](5, os.TempDir())
	defer func() {
		segment.remove()
	}()

	appendResponse, _ := segment.append(NewEntry[serializableKey]("Key1", []byte("Value1"), clock.NewSystemClock()))
	segment.stopWrites()
	_, err := segment.append(NewEntry[serializableKey]("Key2", []byte("Value2"), clock.NewSystemClock()))
	require.Error(t, err)

	storedEntry, err := segment.read(appendResponse.Offset, appendResponse.EntryLength)
	require.NoError(t, err)
	require.Equal(t, string(storedEntry.Key), "Key1")
}
