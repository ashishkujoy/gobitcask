package bitcask

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPersistantEmptyKeyValue(t *testing.T) {
	key := make([]byte, 0)
	value := make([]byte, 0)

	kv := NewPersistantKV(key, value)
	data := kv.asBytes()

	kv2, err := PersistantKVFromBytes(data)

	require.NoError(t, err)
	require.Equal(t, key, kv2.key)
	require.Equal(t, string(value), string(kv2.value))
}

func TestPersistantEmptyKeyNonEmptyValue(t *testing.T) {
	key := make([]byte, 0)
	value := []byte("Hello, World!")

	kv := NewPersistantKV(key, value)
	data := kv.asBytes()

	kv2, err := PersistantKVFromBytes(data)

	require.NoError(t, err)
	require.Equal(t, key, kv2.key)
	require.Equal(t, string(value), string(kv2.value))
}

func TestPersistantNonEmptyKeyEmptyValue(t *testing.T) {
	key := []byte("Hello, World!")
	value := make([]byte, 0)

	kv := NewPersistantKV(key, value)
	data := kv.asBytes()

	kv2, err := PersistantKVFromBytes(data)

	require.NoError(t, err)
	require.Equal(t, key, kv2.key)
	require.Equal(t, string(value), string(kv2.value))
}

func TestPersistantKVReadWrite(t *testing.T) {
	key := []byte("hello")
	value := []byte("Hello, World!")

	kv := NewPersistantKV(key, value)
	data := kv.asBytes()

	kv2, err := PersistantKVFromBytes(data)

	require.NoError(t, err)
	require.Equal(t, key, kv2.key)
	require.Equal(t, string(value), string(kv2.value))
}
