package kv

import (
	log "ashishkujoy/bitcask/kv/log"
	"testing"

	"github.com/stretchr/testify/require"
)

type serializableKey string

func (key serializableKey) Serialize() []byte {
	return []byte(key)
}

func TestPutsAKeyInKeyDirectory(t *testing.T) {
	keyDirectory := NewKeyDirectory[serializableKey]()
	keyDirectory.Put("topic", NewEntry(1, 10, 20))

	entry, _ := keyDirectory.Get("topic")
	require.Equal(t, entry, NewEntry(1, 10, 20))
}

func TestDeletesAKeyInKeyDirectory(t *testing.T) {
	keyDirectory := NewKeyDirectory[serializableKey]()
	keyDirectory.Put("topic", NewEntry(1, 10, 20))

	entry, _ := keyDirectory.Get("topic")
	require.Equal(t, NewEntry(1, 10, 20), entry)

	keyDirectory.Delete("topic")
	_, ok := keyDirectory.Get("topic")
	require.False(t, ok)
}

func TestGetANonExistentKeyInKeyDirectory(t *testing.T) {
	keyDirectory := NewKeyDirectory[serializableKey]()

	_, ok := keyDirectory.Get("non-existing")
	require.False(t, ok)
}

func TestBulkUpdatesKeys(t *testing.T) {
	keyDirectory := NewKeyDirectory[serializableKey]()
	response := &log.WriteBackResponse[serializableKey]{
		Key: "topic",
		AppendEntryResponse: &log.AppendEntryResponse{
			FileId:      10,
			Offset:      30,
			EntryLength: 36,
		},
	}
	otherResponse := &log.WriteBackResponse[serializableKey]{
		Key: "disk",
		AppendEntryResponse: &log.AppendEntryResponse{
			FileId:      20,
			Offset:      40,
			EntryLength: 46,
		},
	}

	keyDirectory.BulkUpdate([]*log.WriteBackResponse[serializableKey]{response, otherResponse})

	entry, _ := keyDirectory.Get("topic")
	require.Equal(t, entry, NewEntry(10, 30, 36))

	entry, _ = keyDirectory.Get("disk")
	require.Equal(t, entry, NewEntry(20, 40, 46))
}
