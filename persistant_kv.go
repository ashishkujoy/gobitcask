package bitcask

import (
	"encoding/binary"
	"unsafe"
)

var (
	reservedKeySize = unsafe.Sizeof(uint32(0))
)

type PersistantKV struct {
	// key
	key []byte
	// value
	value []byte
}

func NewPersistantKV(key, value []byte) *PersistantKV {
	return &PersistantKV{
		key:   key,
		value: value,
	}
}

func (kv *PersistantKV) asBytes() []byte {
	reservedKeySize := int(reservedKeySize)
	size := (reservedKeySize * 2) + len(kv.key) + len(kv.value)
	data := make([]byte, size)
	offset := 0

	binary.BigEndian.PutUint32(data[offset:], uint32(len(kv.key)))
	offset += reservedKeySize
	binary.BigEndian.PutUint32(data[offset:], uint32(len(kv.value)))
	offset += reservedKeySize
	copy(data[offset:], kv.key)
	offset += len(kv.key)
	copy(data[offset:], kv.value)

	return data
}

func PersistantKVFromBytes(data []byte) (*PersistantKV, error) {
	u32ValueSize := int(reservedKeySize)
	index := 0

	keySize := binary.BigEndian.Uint32(data[index:u32ValueSize])
	index += u32ValueSize
	valueSize := binary.BigEndian.Uint32(data[index:])
	index += u32ValueSize

	key := data[index : index+int(keySize)]
	index += int(keySize)

	value := data[index : index+int(valueSize)]
	return NewPersistantKV(key, value), nil
}
