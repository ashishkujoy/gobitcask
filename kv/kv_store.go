package kv

import (
	"ashishkujoy/bitcask/config"
	kvlog "ashishkujoy/bitcask/kv/log"
	"fmt"
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
func NewKVStore[Key config.BitcaskKey](config *config.Config[Key]) (*KVStore[Key], error) {
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

// SilentGet Gets the value corresponding to the key. Returns value and true if the value is found, else returns nil and false
// In order to perform SilentGet, a Get operation is performed in the KeyDirectory which returns an Entry indicating the fileId containing the key, offset of the key and the entry length
// If an Entry corresponding to the key is found, a Read operation is performed in the Segments abstraction, which performs an in-memory lookup to identify the segment based on the fileId, and then a Read operation is performed in that Segment
func (store *KVStore[Key]) SilentGet(key Key) ([]byte, bool) {
	store.rwlock.Lock()
	defer store.rwlock.Unlock()

	entry, ok := store.keyDirectory.Get(key)
	if !ok {
		return nil, false
	}

	storedEntry, err := store.segments.Read(entry.FileId, entry.Offset, entry.EntryLength)
	if err != nil {
		return nil, false
	}

	return storedEntry.Value, true
}

// Get gets the value corresponding to the key. Returns value and nil if the value is found, else returns nil and error
// In order to perform Get, a Get operation is performed in the KeyDirectory which returns an Entry indicating the fileId, offset of the key and the entry length
// If an Entry corresponding to the key is found, a Read operation is performed in the Segments abstraction, which performs an in-memory lookup to identify the segment based on the fileId, and then a Read operation is performed in that Segment
func (store *KVStore[Key]) Get(key Key) ([]byte, error) {
	store.rwlock.Lock()
	defer store.rwlock.Unlock()

	entry, ok := store.keyDirectory.Get(key)
	if !ok {
		return nil, fmt.Errorf("key %v not present in store", key)
	}
	storedEntry, err := store.segments.Read(entry.FileId, entry.Offset, entry.EntryLength)

	if err != nil {
		return nil, err
	}

	return storedEntry.Value, nil
}

// ReadInactiveSegments reads inactive segments identified by `totalSegments`. This operation is performed during merge.
// keyMapper is used to map a byte slice Key to a generically typed Key. keyMapper is basically a means to perform deserialization of keys which is necessary to update the state in KeyDirectory after the merge operation is done, more on this is mentioned in KeyDirectory.go
func (store *KVStore[Key]) ReadInactiveSegments(
	totalSegments int,
	keyMapper func([]byte) Key,
) ([]uint64, [][]*kvlog.MappedStoredEntry[Key], error) {
	store.rwlock.Lock()
	defer store.rwlock.Unlock()

	return store.segments.ReadInactiveSegments(totalSegments, keyMapper)
}

// ReadAllInactiveSegments reads all the inactive segments. This operation is performed during merge.
// keyMapper is used to map a byte slice Key to a generically typed Key. keyMapper is basically a means to perform deserialization of keys which is necessary to update the state in KeyDirectory after the merge operation is done, more on this is mentioned in KeyDirectory.go and Worker.go inside merge/ package.
func (store *KVStore[Key]) ReadAllInactiveSegments(
	keyMapper func([]byte) Key,
) ([]uint64, [][]*kvlog.MappedStoredEntry[Key], error) {
	store.rwlock.Lock()
	defer store.rwlock.Unlock()

	return store.segments.ReadAllInactiveSegments(keyMapper)
}

// WriteBack writes back the changes (merged changes) to new inactive segments. This operation is performed during merge.
// It writes all the changes into M new inactive segments and once those changes are written to the new inactive segment(s), the state of the keys present in the `changes` parameter is updated in the KeyDirectory. More on this is mentioned in Worker.go inside merge/ package.
// Once the state is updated in the KeyDirectory, the old segments identified by `fileIds` are removed from disk.
func (store *KVStore[Key]) WriteBack(fileIds []uint64, changes map[Key]*kvlog.MappedStoredEntry[Key]) error {
	store.rwlock.Lock()
	defer store.rwlock.Unlock()

	writeBackResponse, err := store.segments.WriteBack(changes)
	if err != nil {
		return err
	}
	store.keyDirectory.BulkUpdate(writeBackResponse)
	store.segments.Remove(fileIds)
	return nil
}

// ClearLog removes all the log files
func (store *KVStore[Key]) Clear() {
	store.rwlock.Lock()
	defer store.rwlock.Unlock()

	store.segments.RemoveAllInactive()
	store.segments.RemoveActive()
}

// Sync performs a sync of all the active and inactive segments. This implementation uses the Segment vocabulary over DataFile vocabulary
func (store *KVStore[Key]) Sync() {
	store.rwlock.Lock()
	defer store.rwlock.Unlock()

	store.segments.Sync()
}

// Shutdown performs a shutdown of the segments which involves setting the active segment to nil and removing the entire in-memory representation of the inactive segments
func (store *KVStore[Key]) Shutdown() {
	store.rwlock.Lock()
	defer store.rwlock.Unlock()

	store.segments.Shutdown()
}

// reload the entire state during start-up.
func (store *KVStore[Key]) reload(config *config.Config[Key]) error {
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
