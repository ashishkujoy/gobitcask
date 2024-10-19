package kv

import (
	"ashishkujoy/bitcask/config"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReloadStore(t *testing.T) {
	config := config.NewConfig(".", 256, 16, config.NewMergeConfig(2, keyMapper))
	kv, _ := NewKVStore(config)

	for count := 1; count <= 100; count++ {
		countAsString := strconv.Itoa(count)
		_ = kv.Put(serializableKey(countAsString), []byte(countAsString))
	}

	kv.Sync()
	kv.Shutdown()

	kv, _ = NewKVStore(config)
	defer kv.Clear()

	for count := 1; count <= 100; count++ {
		countAsString := strconv.Itoa(count)
		value, _ := kv.SilentGet(serializableKey(countAsString))
		require.Equal(t, countAsString, string(value))
	}
}
