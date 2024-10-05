package bitcask

import "errors"

type Bitcask struct {
	data_dir   string
	active_df  *DataFile
	data_files map[uint32]*DataFile
	key_dir    *KeyDir
}

func NewBitcask(data_dir string) (*Bitcask, error) {
	df, err := OpenDataFile(data_dir+"_1", 1)
	if err != nil {
		return &Bitcask{}, nil
	}
	data_files := make(map[uint32]*DataFile)
	data_files[1] = df
	key_dir := NewKeyDir()

	return &Bitcask{
		active_df:  df,
		data_dir:   data_dir,
		data_files: data_files,
		key_dir:    key_dir,
	}, nil
}

func (bc *Bitcask) Put(kv *PersistantKV) error {
	offset, size, err := bc.active_df.Write(kv)
	if err != nil {
		return err
	}
	bc.key_dir.Add(&kv.key, bc.active_df.id, uint32(size), offset)
	return nil
}

func (bc *Bitcask) Get(key *Key) (*PersistantKV, error) {
	entry, ok := bc.key_dir.Get(key)
	if !ok {
		return &PersistantKV{}, errors.New("key not found")
	}
	df, ok := bc.data_files[entry.file_id]
	if !ok {
		return &PersistantKV{}, errors.New("data not found")
	}
	return df.Read(entry.value_pos)
}
