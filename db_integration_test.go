package bitcask

import (
	"ashishkujoy/bitcask/config"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

type serializableKey string

func (key serializableKey) Serialize() []byte {
	return []byte(key)
}

var keyMapper = func(b []byte) serializableKey {
	return serializableKey(string(b))
}

func TestAPutAndDoASilentGet(t *testing.T) {
	config := config.NewConfig(".", 8, 16, config.NewMergeConfig(2, keyMapper))
	db, _ := NewDB(config)
	defer db.Shutdown()
	defer db.clearLog()

	db.Put("Topic", []byte("Microservices"))
	value, ok := db.SilentGet("Topic")
	require.True(t, ok)
	require.Equal(t, string(value), "Microservices")
}

func TestSilentGetANonExistentKey(t *testing.T) {
	config := config.NewConfig(".", 8, 16, config.NewMergeConfig(2, keyMapper))
	db, _ := NewDB(config)
	defer db.Shutdown()
	defer db.clearLog()

	_, ok := db.SilentGet("Topic")
	require.False(t, ok)
}

func TestPutAndDoAGet(t *testing.T) {
	config := config.NewConfig(".", 8, 16, config.NewMergeConfig(2, keyMapper))
	db, _ := NewDB(config)
	defer db.Shutdown()
	defer db.clearLog()

	db.Put("Topic", []byte("microservices"))
	value, _ := db.Get("Topic")
	require.Equal(t, string(value), "microservices")
}

func TestGetANonExistent(t *testing.T) {
	config := config.NewConfig(".", 8, 16, config.NewMergeConfig(2, keyMapper))
	db, _ := NewDB(config)
	defer db.Shutdown()
	defer db.clearLog()

	_, err := db.Get("Topic")
	require.Error(t, err)
}

func TestUpdateAndDoASilentGet(t *testing.T) {
	config := config.NewConfig(".", 8, 16, config.NewMergeConfig(2, keyMapper))
	db, _ := NewDB(config)
	defer db.Shutdown()
	defer db.clearLog()

	db.Put("Topic", []byte("Microservices"))
	db.Update("Topic", []byte("Databases"))

	value, ok := db.SilentGet("Topic")

	require.True(t, ok)
	require.Equal(t, string(value), "Databases")
}

func TestUpdateAndDoAGet(t *testing.T) {
	config := config.NewConfig(".", 8, 16, config.NewMergeConfig(2, keyMapper))
	db, _ := NewDB(config)
	defer db.Shutdown()
	defer db.clearLog()

	db.Put("Topic", []byte("Microservices"))
	db.Update("Topic", []byte("Databases"))

	value, err := db.Get("Topic")

	require.NoError(t, err)
	require.Equal(t, string(value), "Databases")
}

func TestDeleteAndDoASilentGet(t *testing.T) {
	config := config.NewConfig(".", 8, 16, config.NewMergeConfig(2, keyMapper))
	db, _ := NewDB(config)
	defer db.Shutdown()
	defer db.clearLog()

	db.Put("Topic", []byte("Microservices"))
	db.Delete("Topic")
	_, ok := db.SilentGet("Topic")
	require.False(t, ok)
}

func TestDeleteAndDoAGet(t *testing.T) {
	config := config.NewConfig(".", 8, 16, config.NewMergeConfig(2, keyMapper))
	db, _ := NewDB(config)
	defer db.Shutdown()
	defer db.clearLog()

	db.Put("Topic", []byte("Microservices"))
	db.Delete("Topic")
	_, err := db.Get("Topic")
	require.Error(t, err)
}

func TestReloadDb(t *testing.T) {
	config := config.NewConfig(".", 8, 16, config.NewMergeConfig(2, keyMapper))
	db, _ := NewDB(config)

	for count := 1; count <= 100; count++ {
		key := strconv.Itoa(count)
		db.Put(serializableKey(key), []byte(key))
	}

	newDb, _ := NewDB(config)
	defer newDb.Shutdown()
	defer newDb.clearLog()

	for count := 1; count <= 100; count++ {
		key := strconv.Itoa(count)
		value, err := db.Get(serializableKey(key))
		require.NoError(t, err)
		require.Equal(t, value, []byte(key))
	}
}
