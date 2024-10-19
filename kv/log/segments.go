package kv

import (
	"ashishkujoy/bitcask/clock"
	"ashishkujoy/bitcask/config"
	"ashishkujoy/bitcask/kv/id"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Segments[Key config.BitcaskKey] struct {
	activeSegment      *Segment[Key]
	inactiveSegments   map[uint64]*Segment[Key]
	fileIdGenerator    *id.TimestampBasedFileIdGenerator
	clock              clock.Clock
	maxSegmentByteSize uint64
	directory          string
}

type WriteBackResponse[Key config.BitcaskKey] struct {
	Key                 Key
	AppendEntryResponse *AppendEntryResponse
}

func NewSegments[Key config.BitcaskKey](
	directory string,
	maxSegmentByteSize uint64,
	clock clock.Clock,
) (*Segments[Key], error) {
	idGenerator := id.NewTimestampBasedFileIdGenerator(clock)
	segmentId := idGenerator.Next()
	segment, err := NewSegment[Key](segmentId, directory)

	if err != nil {
		return nil, err
	}

	segments := Segments[Key]{
		activeSegment:      segment,
		clock:              clock,
		directory:          directory,
		maxSegmentByteSize: maxSegmentByteSize,
		inactiveSegments:   map[uint64]*Segment[Key]{},
		fileIdGenerator:    idGenerator,
	}

	if err := segments.reload(); err != nil {
		return nil, err
	}

	return &segments, nil
}

func (segments *Segments[Key]) reload() error {
	entries, err := os.ReadDir(segments.directory)

	if err != nil {
		return err
	}

	suffix := segmentFilePrefix + "." + segmentFileSuffix

	for _, entry := range entries {
		filepath := entry.Name()
		if strings.HasSuffix(filepath, suffix) {
			fileId, err := strconv.ParseUint(strings.Split(entry.Name(), "_")[0], 10, 64)
			if err != nil {
				return err
			}
			if fileId != segments.activeSegment.fileId {
				segment, err := ReloadInactiveSegment[Key](fileId, segments.directory)
				if err != nil {
					return err
				}
				segments.inactiveSegments[fileId] = segment
			}
		}
	}

	return nil
}

// Append performs an append operation in the active segment file.
// Before the append operation can be done, the size of the active segment is checked.
// If its size < the size of segment threshold, the key value pair is appended to the active segment, else the active segment is rolled-over
func (segments *Segments[Key]) Append(key Key, value []byte) (*AppendEntryResponse, error) {
	if err := segments.maybeRolloverActiveSegment(); err != nil {
		return nil, err
	}
	return segments.activeSegment.append(NewEntry(key, value, segments.clock))
}

// AppendDelete performs an append operation in the active segment file.
// Before the append operation can be done, the size of the active segment is checked.
// If its size < the size of segment threshold, the key value pair is appended to the active segment, else the active segment is rolled-over
func (segments *Segments[Key]) AppendDelete(key Key) (*AppendEntryResponse, error) {
	if err := segments.maybeRolloverActiveSegment(); err != nil {
		return nil, err
	}

	return segments.activeSegment.append(NewDeleteEntry(key, segments.clock))
}

// Read performs a read operation from the offset in the segment file. This method is invoked in the Get operation
func (segments *Segments[Key]) Read(fileId uint64, offset int64, size uint32) (*StoredEntry, error) {
	if segments.activeSegment.fileId == fileId {
		return segments.activeSegment.read(offset, size)
	}

	segment, ok := segments.inactiveSegments[fileId]

	if !ok {
		return nil, fmt.Errorf("invalid fileId %v", fileId)
	}

	return segment.read(offset, size)
}

// ReadInactiveSegments reads inactive segments identified by `totalSegments`. This operation is performed during merge.
// keyMapper is used to map a byte slice Key to a generically typed Key. keyMapper is basically a means to perform deserialization of keys which is necessary to update the state in KeyDirectory after the merge operation is done, more on this is mentioned in KeyDirectory.go
func (segments *Segments[Key]) ReadInactiveSegments(
	totalSegments int,
	keyMapper func([]byte) Key,
) ([]uint64, [][]*MappedStoredEntry[Key], error) {
	index := 0
	contents := make([][]*MappedStoredEntry[Key], totalSegments)
	fileIds := make([]uint64, totalSegments)

	for _, segment := range segments.inactiveSegments {
		if index >= totalSegments {
			break
		}

		mappedStoredEntry, err := segment.ReadFull(keyMapper)

		if err != nil {
			return nil, nil, err
		}

		contents[index] = mappedStoredEntry
		fileIds[index] = segment.fileId
		index = index + 1
	}

	return fileIds, contents, nil
}

// ReadAllInactiveSegments reads all the inactive segments. This operation is performed during merge.
// keyMapper is used to map a byte slice Key to a generically typed Key. keyMapper is basically a means to perform deserialization of keys which is necessary to update the state in KeyDirectory after the merge operation is done, more on this is mentioned in KeyDirectory.go and Worker.go inside merge/ package.
func (segments *Segments[Key]) ReadAllInactiveSegments(
	keyMapper func([]byte) Key,
) ([]uint64, [][]*MappedStoredEntry[Key], error) {
	return segments.ReadInactiveSegments(len(segments.inactiveSegments), keyMapper)
}

// WriteBack writes back the changes (merged changes) to new inactive segments. This operation is performed during merge.
// It writes all the changes into M new inactive segments and once those changes are written to the new inactive segment(s), the state of the keys present in the `changes` parameter is updated in the KeyDirectory. More on this is mentioned in Worker.go inside merge/ package.
func (segments *Segments[Key]) WriteBack(changes map[Key]*MappedStoredEntry[Key]) ([]*WriteBackResponse[Key], error) {
	segment, err := NewSegment[Key](segments.fileIdGenerator.Next(), segments.directory)

	if err != nil {
		return nil, err
	}
	segments.inactiveSegments[segment.fileId] = segment
	index := 0
	writeBackResponses := make([]*WriteBackResponse[Key], len(changes))

	for key, value := range changes {
		appendEntryResponse, err := segment.append(NewEntryPreservingTimestamp(
			value.Key,
			value.Value,
			value.Timestamp,
			segments.clock,
		))

		if err != nil {
			return nil, err
		}

		writeBackResponses[index] = &WriteBackResponse[Key]{
			Key:                 key,
			AppendEntryResponse: appendEntryResponse,
		}
		index = index + 1
		newSegment, err := segments.maybeRolloverSegment(segment)

		if err != nil {
			return nil, err
		}

		if newSegment != nil {
			segments.inactiveSegments[newSegment.fileId] = newSegment
			segment.stopWrites()
			segment = newSegment
		}
	}

	return writeBackResponses, nil
}

// RemoveActive removes the active segment file from disk
func (segments *Segments[Key]) RemoveActive() {
	segments.activeSegment.remove()
}

// RemoveAllInactive removes all the inactive segment files from disk
func (segments *Segments[Key]) RemoveAllInactive() {
	for _, segment := range segments.inactiveSegments {
		segment.remove()
	}
}

// Remove removes all the inactive files identified by fileIds. This operation is called from WriteBack of KVStore which is called during merge operation
func (segments *Segments[Key]) Remove(fileIds []uint64) {
	for _, fileId := range fileIds {
		segment, ok := segments.inactiveSegments[fileId]
		if ok {
			segment.remove()
			delete(segments.inactiveSegments, fileId)
		}
	}
}

// AllInactiveSegments returns all the inactive segments
func (segments *Segments[Key]) AllInactiveSegments() map[uint64]*Segment[Key] {
	return segments.inactiveSegments
}

// Sync Performs a file sync, ensures all the disk blocks (or pages) at the Kernel page cache are flushed to the disk
func (segments *Segments[Key]) Sync() {
	segments.activeSegment.sync()
	for _, segment := range segments.inactiveSegments {
		segment.sync()
	}
}

// Shutdown sets the active segment to nil and deletes all the keys from the inactive segments
func (segments *Segments[Key]) Shutdown() {
	segments.activeSegment = nil
	for fileId := range segments.inactiveSegments {
		delete(segments.inactiveSegments, fileId)
	}
}

func (segments *Segments[Key]) maybeRolloverActiveSegment() error {
	newSegment, err := segments.maybeRolloverSegment(segments.activeSegment)
	if err != nil {
		return err
	}
	if newSegment != nil {
		segments.inactiveSegments[segments.activeSegment.fileId] = segments.activeSegment
		segments.activeSegment = newSegment
	}
	return nil
}

func (segments *Segments[Key]) maybeRolloverSegment(segment *Segment[Key]) (*Segment[Key], error) {
	if segments.maxSegmentByteSize <= uint64(segment.sizeInBytes()) {
		segment.stopWrites()
		id := segments.fileIdGenerator.Next()
		newSegment, err := NewSegment[Key](id, segments.directory)
		if err != nil {
			return nil, err
		}
		return newSegment, nil
	}
	return nil, nil
}
