package kv

import (
	"ashishkujoy/bitcask/clock"
	"ashishkujoy/bitcask/config"
	"encoding/binary"
	"unsafe"
)

var (
	reservedKeySize       = uint32(unsafe.Sizeof(uint32(0)))
	reservedValueSize     = uint32(unsafe.Sizeof(uint32(0)))
	reservedTimestampSize = uint32(unsafe.Sizeof(uint32(0)))
	tombstoneMarkerSize   = uint32(unsafe.Sizeof(byte(0)))
	littleEndian          = binary.LittleEndian
)

type valueReference struct {
	value     []byte
	tombstone byte
}

type Entry[Key config.Serializable] struct {
	key       Key            // Key of the entry
	value     valueReference // Value of the entry
	timestamp uint32         // timestamp
	clock     clock.Clock    // clock
}

// NewEntry creates a instance of Entry with given key and value, setting tombstone to 0
func NewEntry[Key config.Serializable](key Key, value []byte, clock clock.Clock) *Entry[Key] {
	return &Entry[Key]{
		key:       key,
		value:     valueReference{value: value, tombstone: 0},
		timestamp: 0,
		clock:     clock,
	}
}

// NewEntryPreservingTimestamp creates a new instance of Entry with tombstone byte set to 0 and keeping the provided timestamp
func NewEntryPreservingTimestamp[Key config.Serializable](key Key, value []byte, ts uint32, clock clock.Clock) *Entry[Key] {
	return &Entry[Key]{
		key:       key,
		value:     valueReference{value: value, tombstone: 0},
		timestamp: ts,
		clock:     clock,
	}
}

// NewDeleteEntry creates a instance of Entry with tombstone set to 1
func NewDeleteEntry[Key config.Serializable](key Key, clock clock.Clock) *Entry[Key] {
	return &Entry[Key]{
		key: key,
		value: valueReference{
			value:     []byte{},
			tombstone: 1,
		},
		timestamp: 0,
		clock:     clock,
	}
}

// encode convert entry to byte slice which can be written to the disk
// Encoding scheme
//
//	┌───────────┬──────────┬────────────┬─────┬───────┐
//	│ timestamp │ key_size │ value_size │ key │ value │
//	└───────────┴──────────┴────────────┴─────┴───────┘
func (entry *Entry[Key]) encode() []byte {
	serializedKey := entry.key.Serialize()
	keySize := uint32(len(serializedKey))
	valueSize := uint32(len(entry.value.value)) + tombstoneMarkerSize
	totalEntrySize := reservedTimestampSize + reservedKeySize + reservedValueSize + keySize + valueSize
	encoded := make([]byte, totalEntrySize)

	var offset uint32 = 0
	if entry.timestamp == 0 {
		littleEndian.PutUint32(encoded, uint32(int(entry.clock.Now())))
	} else {
		littleEndian.PutUint32(encoded, entry.timestamp)
	}
	offset += reservedTimestampSize

	littleEndian.PutUint32(encoded[offset:], keySize)
	offset += reservedKeySize

	littleEndian.PutUint32(encoded[offset:], valueSize)
	offset += reservedValueSize

	copy(encoded[offset:], serializedKey)
	offset += keySize

	copy(encoded[offset:], append(entry.value.value, entry.value.tombstone))

	return encoded
}

type StoredEntry struct {
	Key       []byte
	Value     []byte
	Deleted   bool
	Timestamp uint32
}

func decode(content []byte) *StoredEntry {
	storedEntry, _ := decodeFrom(content, 0)
	return storedEntry
}

func decodeFrom(content []byte, offset uint32) (*StoredEntry, uint32) {
	timestamp := littleEndian.Uint32(content[offset:])
	offset += reservedTimestampSize

	keySize := littleEndian.Uint32(content[offset:])
	offset += reservedKeySize

	valueSize := littleEndian.Uint32(content[offset:])
	offset += reservedValueSize

	key := content[offset : offset+keySize]
	offset += keySize

	value := content[offset : offset+valueSize]
	offset += valueSize

	return &StoredEntry{
		Key:       key,
		Value:     value[:valueSize-1],
		Deleted:   value[valueSize-1]&0x01 == 0x01,
		Timestamp: timestamp,
	}, offset
}
