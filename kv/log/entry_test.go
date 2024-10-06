package kv

import (
	"ashishkujoy/bitcask/clock"
	"testing"

	"github.com/stretchr/testify/require"
)

type serializableKey string

func (key serializableKey) Serialize() []byte {
	return []byte(key)
}

type fixedClock struct{}

func (clock *fixedClock) Now() int64 {
	return 100
}

func TestEncodeAKeyValuePair(t *testing.T) {
	entry := NewEntry[serializableKey]("topic", []byte("microservices"), clock.NewSystemClock())
	encoded := entry.encode()

	storedEntry := decode(encoded)

	require.False(t, storedEntry.Deleted)
	require.Equal(t, []byte("topic"), storedEntry.Key)
}

func TestEncodesAKeyValuePairAndValidatesTimestamp(t *testing.T) {
	entry := NewEntry[serializableKey]("topic", []byte("microservices"), &fixedClock{})
	encoded := entry.encode()
	storedEntry := decode(encoded)

	require.Equal(t, uint32(100), storedEntry.Timestamp)
}

func TestEncodeADeleteKeyValuePair(t *testing.T) {
	entry := NewDeleteEntry[serializableKey]("topic", clock.NewSystemClock())
	encoded := entry.encode()
	storedEntry := decode(encoded)

	require.True(t, storedEntry.Deleted)
}
