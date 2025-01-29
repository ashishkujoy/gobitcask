package kv

import (
	"ashishkujoy/bitcask/config"
	log "ashishkujoy/bitcask/kv/log"
	iradix "github.com/hashicorp/go-immutable-radix/v2"
)

type KeyDirectory[Key config.BitcaskKey] struct {
	entryByKey *iradix.Tree[*Entry]
}

// NewKeyDirectory Creates a new instance of KeyDirectory
func NewKeyDirectory[Key config.BitcaskKey]() *KeyDirectory[Key] {
	return &KeyDirectory[Key]{
		entryByKey: iradix.New[*Entry](),
	}
}

// Reload reloads the state of the KeyDirectory during start-up. As a part of reloading the state in bitcask model, all the inactive segments are read,
// and the keys from all the inactive segments are stored in the KeyDirectory.
// Riak's paper optimizes reloading by creating small sized hint files during merge and compaction.
// Hint files contain the keys and the metadata fields like fileId, fileOffset and entryLength, these hint files are referred during reload. This implementation does not create Hint file
func (keyDirectory *KeyDirectory[Key]) Reload(fileId uint64, entries []*log.MappedStoredEntry[Key]) {
	for _, entry := range entries {
		keyDirectory.Put(entry.Key, NewEntry(fileId, int64(entry.KeyOffset), entry.EntryLength))
	}
}

// Put puts a key and its entry as the value in the KeyDirectory
func (keyDirectory *KeyDirectory[Key]) Put(key Key, value *Entry) {
	keyDirectory.entryByKey, _, _ = keyDirectory.entryByKey.Insert(key.Serialize(), value)
}

// BulkUpdate performs bulk changes to the KeyDirectory state. This method is called during merge and compaction from KeyStore.
func (keyDirectory *KeyDirectory[Key]) BulkUpdate(changes []*log.WriteBackResponse[Key]) {
	for _, change := range changes {
		keyDirectory.Put(change.Key, NewEntryFrom(change.AppendEntryResponse))
	}
}

// Delete removes the key from the KeyDirectory
func (keyDirectory *KeyDirectory[Key]) Delete(key Key) {
	keyDirectory.entryByKey, _, _ = keyDirectory.entryByKey.Delete(key.Serialize())
}

// Get returns the Entry and a boolean to indicate if the value corresponding to the key is present in the KeyDirectory.
// Get returns nil, false if the value corresponding to the key is not present
// Get returns a pointer to an Entry, true if the value corresponding to the key is present
func (keyDirectory *KeyDirectory[Key]) Get(key Key) (*Entry, bool) {
	value, ok := keyDirectory.entryByKey.Get(key.Serialize())
	return value, ok
}
