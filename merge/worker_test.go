package merge

import (
	"ashishkujoy/bitcask/config"
	"ashishkujoy/bitcask/kv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var keyMapper = func(b []byte) serializableKey {
	return serializableKey(string(b))
}

func TestMergeSegmentsWithUpdate(t *testing.T) {
	config := config.NewConfig(".", 8, config.NewMergeConfig(2, keyMapper))
	store, _ := kv.NewKVStore(config)
	defer store.Clear()

	worker := NewWorker(store, config.MergeConfig())

	_ = store.Put("topic", []byte("microservices"))
	_ = store.Put("topic", []byte("bitcask"))
	_ = store.Put("disk", []byte("ssd"))

	worker.beginMerge()
	value, _ := store.Get("topic")
	require.Equal(t, string(value), "bitcask")
}

func TestMergeSegmentsWithDeleteEntry(t *testing.T) {
	config := config.NewConfig(".", 8, config.NewMergeConfig(2, keyMapper))
	store, _ := kv.NewKVStore(config)
	defer store.Clear()

	worker := NewWorker(store, config.MergeConfig())

	_ = store.Put("topic", []byte("microservices"))
	_ = store.Delete("topic")
	_ = store.Put("ssd", []byte("disk"))

	worker.beginMerge()
	_, ok := store.SilentGet("topic")
	require.False(t, ok)
}

func TestMergeMoreThan2Segments(t *testing.T) {
	config := config.NewConfig(".", 8, config.NewMergeConfig(2, keyMapper))
	store, _ := kv.NewKVStore(config)
	defer store.Clear()

	worker := NewWorker(store, config.MergeConfig())
	_ = store.Put("topic", []byte("microservices"))
	_ = store.Put("disk", []byte("ssd"))
	_ = store.Put("engine", []byte("bitcask"))
	_ = store.Put("language", []byte("go"))

	worker.beginMerge()

	topicValue, _ := store.Get("topic")
	require.Equal(t, string(topicValue), "microservices")
	diskValue, _ := store.Get("disk")
	require.Equal(t, string(diskValue), "ssd")
	engineValue, _ := store.Get("engine")
	require.Equal(t, string(engineValue), "bitcask")
	languageValue, _ := store.Get("language")
	require.Equal(t, string(languageValue), "go")
}

func TestMergeSegmentsOnSchedule(t *testing.T) {
	mergeConfig := config.NewMergeConfigWithAllSegmentsToReadEveryFixedDuration(
		2*time.Second,
		keyMapper,
	)
	config := config.NewConfig(".", 8, mergeConfig)
	store, _ := kv.NewKVStore(config)
	defer store.Clear()

	_ = store.Put("topic", []byte("microservices"))
	_ = store.Put("topic", []byte("bitcask"))
	_ = store.Put("disk", []byte("ssd"))

	worker := NewWorker(store, config.MergeConfig())
	time.Sleep(3 * time.Second)
	worker.Stop()

	value, _ := store.Get("topic")
	require.Equal(t, string(value), "bitcask")
}
