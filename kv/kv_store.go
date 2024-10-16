package kv

import (
	"ashishkujoy/bitcask/config"
	kvlog "ashishkujoy/bitcask/kv/log"
	"sync"
)

// KVStore encapsulates append-only log segments and KeyDirectory which is an in-memory hashmap
// Segments is an abstraction that manages the active and K inactive segments.
// KVStore also maintains a RWLock that allows an exclusive writer and N readers
type KVStore[Key config.BitcaskKey] struct {
	segments     *kvlog.Segments[Key]
	keyDirectory *KeyDirectory[Key]
	rwlock       sync.RWMutex
}

// NewKVStore creates a new instance of KVStore
// It also performs a reload operation `store.reload(config)` that is responsible for reloading the state of KeyDirectory from inactive segments
func NewKVStore[Key config.BitcaskKey](config config.Config[Key]) (*KVStore[Key], error) {
	segments, err := kvlog.NewSegments[Key](
		config.Directory(),
		config.MaxSegmentSizeInBytes(),
		config.Clock(),
	)

	if err != nil {
		return nil, err
	}
	store := &KVStore[Key]{
		segments:     segments,
		keyDirectory: NewKeyDirectory[Key](config.KeyDirectoryCapacity()),
	}

	if err := store.reload(config); err != nil {
		return nil, err
	}
	return store, nil
}

// Put puts the key and the value in bitcask. Put operations consists of the following steps:
// 1.Append the key and the value in the append-only active segment using `kv.segments.Append(key, value)`.
// - Segments abstraction will append the key and the value to the active segment if the size of the active segment is less than the threshold, else it will perform a rollover of the active segment
// 2.Once the append operation is successful, it will write the key and the Entry to the KeyDirectory, which is an in-memory representation of the key and its position in an append-only segment
func (store *KVStore[Key]) Put(key Key, value []byte) error {
	appendResponse, err := store.segments.Append(key, value)
	if err != nil {
		return err
	}

	store.keyDirectory.Put(key, NewEntryFrom(appendResponse))
	return nil
}

// Update is very much similar to Put. It appends the key and the value to the log and performs an in-place update in the KeyDirectory
func (store *KVStore[Key]) Update(key Key, value []byte) error {
	return store.Put(key, value)
}

func (store *KVStore[Key]) Delete(key Key) error {
	_, err := store.segments.AppendDelete(key)
	if err != nil {
		return err
	}
	store.keyDirectory.Delete(key)
	return nil
}

// reload the entire state during start-up.
func (store *KVStore[Key]) reload(config config.Config[Key]) error {
	store.rwlock.Lock()
	defer store.rwlock.Unlock()

	for fileId, segment := range store.segments.AllInactiveSegments() {
		entries, err := segment.ReadFull(config.MergeConfig().KeyMapper())
		if err != nil {
			return err
		}
		store.keyDirectory.Reload(fileId, entries)
	}

	return nil
}
