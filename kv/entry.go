package kv

import kv "ashishkujoy/bitcask/kv/log"

type Entry struct {
	FileId      uint64
	Offset      int64
	EntryLength uint32
}

func NewEntryFrom(appendResponse *kv.AppendEntryResponse) *Entry {
	return NewEntry(appendResponse.FileId, appendResponse.Offset, appendResponse.EntryLength)
}

func NewEntry(fileId uint64, offset int64, size uint32) *Entry {
	return &Entry{
		FileId:      fileId,
		Offset:      offset,
		EntryLength: size,
	}
}
