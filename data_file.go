package bitcask

import (
	"encoding/binary"
	"os"
)

type DataFile struct {
	id   uint32
	file *os.File
	size int64
}

func OpenDataFile(path string, id uint32) (*DataFile, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	return &DataFile{
		file: file,
		size: stat.Size(),
		id:   id,
	}, nil
}

func (df *DataFile) Close() error {
	df.file.Sync()
	return df.file.Close()
}

func (df *DataFile) Write(data *PersistantKV) (int64, int, error) {
	offset := df.size
	bytes := data.asBytes()
	entry_size_info := make([]byte, int(reservedKeySize))
	binary.BigEndian.PutUint32(entry_size_info, uint32(len(bytes)))
	_, err := df.file.Write(entry_size_info)
	if err != nil {
		return 0, 0, err
	}
	n, err := df.file.Write(bytes)
	if err != nil {
		return 0, 0, err
	}

	df.size += int64(n) + int64(reservedKeySize)
	return offset, n, nil
}

func (df *DataFile) Read(offset int64) (*PersistantKV, error) {
	entry_size_info := make([]byte, int(reservedKeySize))
	_, err := df.file.ReadAt(entry_size_info, offset)

	if err != nil {
		return &PersistantKV{}, err
	}
	data_size := binary.BigEndian.Uint32(entry_size_info)
	data := make([]byte, data_size)
	_, err = df.file.ReadAt(data, offset+int64(reservedKeySize))
	if err != nil {
		return &PersistantKV{}, err
	}

	return PersistantKVFromBytes(data)
}
