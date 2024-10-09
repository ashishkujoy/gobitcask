package kv

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func getTempFileName() string {
	dir := os.TempDir()
	return fmt.Sprintf("%v/store_%v", dir, time.Now().UnixMilli())
}

func TestReadAndWriteSingleEntry(t *testing.T) {
	temp_file := getTempFileName()
	defer os.Remove(temp_file)

	store, err := NewStore(temp_file)
	require.NoError(t, err)

	message := []byte("Welcome to new world!")
	offset, err := store.append(message)
	require.NoError(t, err)
	require.Equal(t, offset, int64(0))

	actual_message, err := store.read(offset, uint32(len(message)))
	require.NoError(t, err)
	require.Equal(t, string(message), string(actual_message))
}

func TestMultipleReadAndWrite(t *testing.T) {
	temp_file := getTempFileName()
	defer os.Remove(temp_file)

	store, err := NewStore(temp_file)
	require.NoError(t, err)

	welcome_msg := []byte("Welcome to new world!")
	hello_msg := []byte("Hello world")
	bye_msg := []byte("Bye bye world")

	welcome_msg_offset, _ := store.append(welcome_msg)
	hello_msg_offset, _ := store.append(hello_msg)
	bye_msg_offset, _ := store.append(bye_msg)

	actual_welcome_msg, _ := store.read(welcome_msg_offset, uint32(len(welcome_msg)))
	actual_hello_msg, _ := store.read(hello_msg_offset, uint32(len(hello_msg)))
	actual_bye_msg, _ := store.read(bye_msg_offset, uint32(len(bye_msg)))

	require.Equal(t, string(welcome_msg), string(actual_welcome_msg))
	require.Equal(t, string(hello_msg), string(actual_hello_msg))
	require.Equal(t, string(bye_msg), string(actual_bye_msg))
}
