package kv

import (
	"ashishkujoy/bitcask/config"
	"fmt"
	"os"
	"path"
)

type AppendEntryResponse struct {
	FileId      uint64
	Offset      int64
	EntryLength uint32
}

type MappedStoredEntry[Key config.BitcaskKey] struct {
	Key         Key
	Value       []byte
	Deleted     bool
	Timestamp   uint32
	KeyOffset   uint32
	EntryLength uint32
}

type Segment[Key config.BitcaskKey] struct {
	fileId   uint64
	filePath string
	store    *Store
}

const segmentFilePrefix = "bitcask"
const segmentFileSuffix = "data"

// NewSegment represents an append-only log
func NewSegment[Key config.BitcaskKey](fileId uint64, directory string) (*Segment[Key], error) {
	filepath, err := createSegment(fileId, directory)
	if err != nil {
		return nil, err
	}
	store, err := NewStore(filepath)
	if err != nil {
		return nil, err
	}
	return &Segment[Key]{
		fileId:   fileId,
		filePath: filepath,
		store:    store,
	}, nil
}

// ReloadInactiveSegment reloads the inactive segment during start-up. As a part of ReloadInactiveSegment, we just create the in-memory representation of inactive segment and its store
func ReloadInactiveSegment[Key config.BitcaskKey](fileId uint64, directory string) (*Segment[Key], error) {
	filePath := segmentName(fileId, directory)
	store, err := ReloadStore(filePath)
	if err != nil {
		return nil, err
	}
	return &Segment[Key]{
		fileId:   fileId,
		filePath: filePath,
		store:    store,
	}, nil
}

func (segment *Segment[Key]) append(entry *Entry[Key]) (*AppendEntryResponse, error) {
	encoded := entry.encode()
	offset, err := segment.store.append(encoded)

	if err != nil {
		return nil, err
	}

	return &AppendEntryResponse{
		FileId:      segment.fileId,
		Offset:      offset,
		EntryLength: uint32(len(encoded)),
	}, nil
}

// read performs a read operation from the offset in the segment file. This method is invoked in the Get operation
func (segement *Segment[Key]) read(offset int64, size uint32) (*StoredEntry, error) {
	bytes, err := segement.store.read(offset, size)
	if err != nil {
		return nil, err
	}
	return decode(bytes), nil
}

func (segement *Segment[Key]) ReadFull(keyMapper func([]byte) Key) ([]*MappedStoredEntry[Key], error) {
	bytes, err := segement.store.readFull()
	if err != nil {
		return nil, err
	}
	return decodeMulti(bytes, keyMapper), nil
}

// sizeInBytes returns the segment file size in bytes
func (segment *Segment[Key]) sizeInBytes() int64 {
	return segment.store.sizeInBytes()
}

// sync Performs a file sync, ensures all the disk blocks (or pages) at the Kernel page cache are flushed to the disk
func (segment *Segment[Key]) sync() {
	segment.store.sync()
}

// stopWrites Closes the write file pointer. This operation is called when the active segment has reached its size threshold.
func (segment *Segment[Key]) stopWrites() {
	segment.store.stopWrites()
}

// remove Removes the file
func (segment *Segment[Key]) remove() {
	segment.store.remove()
}

func createSegment(fileId uint64, directory string) (string, error) {
	filepath := segmentName(fileId, directory)
	_, err := os.Create(filepath)
	if err != nil {
		return "", err
	}
	return filepath, err
}

func segmentName(fileId uint64, directory string) string {
	return path.Join(directory, fmt.Sprintf("%v_%v.%v", fileId, segmentFilePrefix, segmentFileSuffix))
}
