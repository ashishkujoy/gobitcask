package bitcask

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPutInBitcask(t *testing.T) {
	dir, err := os.MkdirTemp("", "bitcask")
	require.NoError(t, err)

	bc, err := NewBitcask(dir)
	require.NoError(t, err)
	key := Key([]byte("key"))
	value := Value([]byte("value"))
	kv := NewPersistantKV(key, value)

	err = bc.Put(kv)
	require.NoError(t, err)

	actual_kv, err := bc.Get(&key)
	require.NoError(t, err)
	require.Equal(t, string(kv.value), string(actual_kv.value))
}
