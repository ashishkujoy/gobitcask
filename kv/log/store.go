package kv

import (
	"fmt"
	"os"
)

// Store is an abstraction that encapsulate read, write, remove and sync operation on a file
type Store struct {
	writer             *os.File
	reader             *os.File
	currentWriteOffset int64
}

// NewStore creates an instance of Store from the filepath. It creates 2 file pointers:
// one for writing and other for reading. The reason for creating 2 file pointers is to let kernel
// perform the necessary optimizations like block prefetch while performing writes in the append-only mode.
// Read on the other handle is very much a random disk operation.
// This implementation "NEVER" closes the read file pointer, whereas the write file pointer is closed when the active segment has reached its size threshold.
// The advantage of not closing the read file pointer is the "reduced latency" (time saved in not invoking file.open) when performing a read from the inactive segment and the
// disadvantage is that it can very well result in too many open file descriptors (FDs) on the OS level.
func NewStore(filepath string) (*Store, error) {
	writer, err := os.OpenFile(filepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	reader, err := os.OpenFile(filepath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &Store{
		writer:             writer,
		reader:             reader,
		currentWriteOffset: 0,
	}, nil
}

// ReloadStore creates an instance of Store with only the read file pointer. This operation is executed only during the start-up to reload the state, if any from disk.
// This method creates only the read file pointer because reloading the state will only create inactive segment(s) and these will be used only for Get operation
func ReloadStore(filepath string) (*Store, error) {
	reader, err := os.OpenFile(filepath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &Store{
		writer:             nil,
		reader:             reader,
		currentWriteOffset: 0,
	}, nil
}

func (store *Store) append(bytes []byte) (int64, error) {
	n, err := store.writer.Write(bytes)
	if err != nil {
		return -1, err
	}
	offset := store.currentWriteOffset
	if n < len(bytes) {
		return -1, fmt.Errorf("could not append %v bytes", len(bytes))
	}
	store.currentWriteOffset += int64(n)
	return offset, nil
}

func (store *Store) read(offset int64, size uint32) ([]byte, error) {
	buf := make([]byte, size)
	n, err := store.reader.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	if n != int(size) {
		return nil, fmt.Errorf("unable to read %d bytes from offset %d", size, offset)
	}
	return buf, nil
}

func (store *Store) readFull() ([]byte, error) {
	return os.ReadFile(store.reader.Name())
}

// sizeInBytes Returns the file size in bytes.
func (store *Store) sizeInBytes() int64 {
	return store.currentWriteOffset
}

// sync Performs a file sync, ensures all the disk blocks (or pages) at the Kernel page cache are flushed to the disk
func (store *Store) sync() error {
	return store.writer.Sync()
}

// stopWrites Closes the write file pointer. This operation is called when the active segment has reached its size threshold.
func (store *Store) stopWrites() {
	store.writer.Close()
}

// remove Removes the file
func (store *Store) remove() {
	_ = os.RemoveAll(store.reader.Name())
}
