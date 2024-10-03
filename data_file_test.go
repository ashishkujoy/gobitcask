package bitcask

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenDataFile(t *testing.T) {
	temp_dir := os.TempDir()
	path := temp_dir + "/test_data_file1"
	defer os.Remove(path)

	df, err := OpenDataFile(path)
	require.NoError(t, err)
	require.Equal(t, int64(0), df.size)
}

func TestOpenAlreadyWritenDataFile(t *testing.T) {
	temp_dir := os.TempDir()
	path := temp_dir + "/test_data_file2"
	defer os.Remove(path)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	require.NoError(t, err)
	file.Write([]byte("hello"))
	file.Sync()

	df, err := OpenDataFile(path)
	require.NoError(t, err)
	require.Equal(t, int64(5), df.size)
}

func TestReadAndWrites(t *testing.T) {
	temp_dir := os.TempDir()
	path := path.Join(temp_dir, "test_data_file3")
	defer os.Remove(path)

	df, err := OpenDataFile(path)
	require.NoError(t, err)

	kv := NewPersistantKV([]byte("hello"), []byte("Hello, World!"))
	offset, _, _ := df.Write(kv)

	read_kv, _ := df.Read(offset)
	require.Equal(t, kv, read_kv)

	kv = NewPersistantKV([]byte("hello1"), []byte("Hello, World2!"))
	offset, _, _ = df.Write(kv)

	read_kv, _ = df.Read(offset)
	require.Equal(t, kv, read_kv)
}
