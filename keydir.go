package bitcask

import "time"

type KeyDirEntry struct {
	file_id    uint32
	value_size uint32
	value_pos  int64
	time_stamp time.Time
}

type KeyDir struct {
	// Should not be using string, as not all byte slices are valid strings
	entries map[string]KeyDirEntry
}

func NewKeyDir() *KeyDir {
	return &KeyDir{
		entries: make(map[string]KeyDirEntry),
	}
}

func (kd *KeyDir) Add(
	key *Key,
	file_id uint32,
	value_size uint32,
	value_pos int64,
) {
	kd.entries[string(*key)] = KeyDirEntry{
		file_id:    file_id,
		value_size: value_size,
		value_pos:  value_pos,
		time_stamp: time.Now(),
	}
}

func (kd *KeyDir) Get(key *Key) (*KeyDirEntry, bool) {
	entry, ok := kd.entries[string(*key)]
	return &entry, ok
}
