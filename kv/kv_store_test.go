package kv

import (
	"ashishkujoy/bitcask/config"
	kv "ashishkujoy/bitcask/kv/log"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var keyMapper = func(b []byte) serializableKey {
	return serializableKey(string(b))
}

func TestPutAndASilentGet(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "testASilentGet")
	defer os.RemoveAll(tempDir)
	config := config.NewConfig(tempDir, 80, config.NewMergeConfig(2, keyMapper))
	store, _ := NewKVStore(config)
	defer store.Clear()

	err := store.Put("topic", []byte("Database Systems"))
	require.NoError(t, err)

	value, ok := store.SilentGet("topic")
	require.True(t, ok)
	require.Equal(t, value, []byte("Database Systems"))
}

func TestSilentGetANonExistentKey(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "testGetNonExistentKey")
	defer os.RemoveAll(tempDir)
	config := config.NewConfig(tempDir, 80, config.NewMergeConfig(2, keyMapper))
	store, _ := NewKVStore(config)
	defer store.Clear()

	value, ok := store.SilentGet("NonExistentKey")
	require.False(t, ok)
	require.Nil(t, value)
}

func TestPutAndDoGet(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "testPutAndGet")
	defer os.RemoveAll(tempDir)
	config := config.NewConfig(tempDir, 80, config.NewMergeConfig(2, keyMapper))
	store, _ := NewKVStore(config)
	defer store.Clear()

	store.Put("Topic", []byte("Database Systems"))
	value, err := store.Get("Topic")
	require.NoError(t, err)
	require.Equal(t, value, []byte("Database Systems"))
}

func TestGetANonExistentKey(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "testGetNonExistentKey")
	defer os.RemoveAll(tempDir)
	config := config.NewConfig(tempDir, 80, config.NewMergeConfig(2, keyMapper))
	store, _ := NewKVStore(config)
	defer store.Clear()

	value, err := store.Get("Topic")
	require.Error(t, err)
	require.Nil(t, value)
}

func TestUpdateAndDoASilentGet(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "testUpdateAndSilentGet")
	defer os.RemoveAll(tempDir)
	config := config.NewConfig(tempDir, 80, config.NewMergeConfig(2, keyMapper))
	store, _ := NewKVStore(config)
	defer store.Clear()

	store.Put("Topic", []byte("Databases"))
	store.Update("Topic", []byte("Database Systems"))

	value, ok := store.SilentGet("Topic")
	require.True(t, ok)
	require.Equal(t, value, []byte("Database Systems"))
}

func TestUpdateAndDoAGet(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "testUpdateAndGet")
	defer os.RemoveAll(tempDir)
	config := config.NewConfig(tempDir, 80, config.NewMergeConfig(2, keyMapper))
	store, _ := NewKVStore(config)
	defer store.Clear()

	store.Put("Topic", []byte("Databases"))
	store.Update("Topic", []byte("Database Systems"))

	value, err := store.Get("Topic")
	require.NoError(t, err)
	require.Equal(t, value, []byte("Database Systems"))
}

func TestDeleteAndDoASilentGet(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "testDeleteAndSilentGet")
	defer os.RemoveAll(tempDir)
	config := config.NewConfig(tempDir, 80, config.NewMergeConfig(2, keyMapper))
	store, _ := NewKVStore(config)
	defer store.Clear()

	store.Put("Topic", []byte("Databases"))
	store.Delete("Topic")
	_, ok := store.SilentGet("Topic")
	require.False(t, ok)
}

func TestDeleteAndDoAGet(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "testDeleteAndGet")
	defer os.RemoveAll(tempDir)
	config := config.NewConfig(tempDir, 80, config.NewMergeConfig(2, keyMapper))
	store, _ := NewKVStore(config)
	defer store.Clear()

	store.Put("Topic", []byte("Databases"))
	store.Delete("Topic")
	_, err := store.Get("Topic")
	require.Error(t, err)
}

func TestReadAPairOfInactiveSegments(t *testing.T) {
	config := config.NewConfig(".", 8, config.NewMergeConfig(2, keyMapper))
	store, _ := NewKVStore(config)
	defer store.Clear()

	store.Put("Topic", []byte("Databases"))
	store.Put("Disk Type", []byte("Solid State Drives"))
	store.Put("Engine", []byte("Turbo Bitcask Engine"))
	store.Put("Editor", []byte("Visual Studio Code, dark mode theme"))
	store.Sync()

	_, entries, _ := store.ReadInactiveSegments(2, config.MergeConfig().KeyMapper())
	keys := toSortedKeys(entries)

	require.Equal(t, 2, len(keys))

	require.Equal(t, "Topic", keys[0])
	require.Equal(t, "Disk Type", keys[1])
}

func TestReadAllInactiveSegments(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "testAllInactiveSegments")
	defer os.RemoveAll(tempDir)
	config := config.NewConfig(tempDir, 8, config.NewMergeConfig(2, keyMapper))
	store, _ := NewKVStore(config)
	defer store.Clear()

	store.Put("Topic", []byte("Databases"))
	store.Put("Disk Type", []byte("Solid State Drives"))
	store.Put("Engine", []byte("Turbo Bitcask Engine"))
	store.Put("Editor", []byte("Visual Studio Code, dark mode theme"))

	_, entries, _ := store.ReadAllInactiveSegments(config.MergeConfig().KeyMapper())
	keys := toSortedKeys(entries)
	require.Equal(t, 3, len(keys))

	require.Equal(t, "Topic", keys[0])
	require.Equal(t, "Engine", keys[1])
	require.Equal(t, "Disk Type", keys[2])
}

func TestWriteBacks(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "testWriteBacks")
	defer os.RemoveAll(tempDir)
	config := config.NewConfig(tempDir, 8, config.NewMergeConfig(2, keyMapper))
	store, _ := NewKVStore(config)
	defer store.Clear()

	changes := make(map[serializableKey]*kv.MappedStoredEntry[serializableKey])
	changes["disk"] = &kv.MappedStoredEntry[serializableKey]{Value: []byte("Solid State Disk")}
	changes["engine"] = &kv.MappedStoredEntry[serializableKey]{Value: []byte("bitcask")}
	changes["topic"] = &kv.MappedStoredEntry[serializableKey]{Value: []byte("Microservices")}

	err := store.WriteBack([]uint64{1}, changes)
	require.NoError(t, err)

	diskValue, _ := store.Get("disk")
	require.Equal(t, diskValue, []byte("Solid State Disk"))

	engineValue, _ := store.Get("engine")
	require.Equal(t, engineValue, []byte("bitcask"))

	topicValue, _ := store.Get("topic")
	require.Equal(t, topicValue, []byte("Microservices"))
}

func TestReload(t *testing.T) {
	tempDir, _ := os.MkdirTemp(os.TempDir(), "test")
	defer os.RemoveAll(tempDir)
	config := config.NewConfig(tempDir, 8, config.NewMergeConfig(2, keyMapper))
	store, _ := NewKVStore(config)

	store.Put("topic", []byte("microservices"))
	store.Put("diskType", []byte("solid state drive"))
	store.Put("engine", []byte("bitcask"))

	store.Sync()
	store.Shutdown()

	newStore, err := NewKVStore(config)
	defer newStore.Clear()
	require.NoError(t, err)

	topicValue, _ := newStore.Get("topic")
	require.Equal(t, topicValue, []byte("microservices"))

	diskTypeValue, _ := newStore.Get("diskType")
	require.Equal(t, diskTypeValue, []byte("solid state drive"))
}

func toSortedKeys(entries [][]*kv.MappedStoredEntry[serializableKey]) []string {
	var keys []string

	for _, row := range entries {
		for _, entry := range row {
			keys = append(keys, string(entry.Key))
		}
	}

	slices.SortFunc(keys, func(a, b string) int {
		return strings.Compare(b, a)
	})

	return keys
}
