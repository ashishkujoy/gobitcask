package bitcask

import (
	"ashishkujoy/bitcask/config"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type benchmarkTestCase struct {
	name string
	size int
}

func BenchmarkGet(b *testing.B) {
	kernalPageSize := os.Getpagesize()
	dir, err := os.MkdirTemp(os.TempDir(), fmt.Sprintf("%v", time.Now().UnixMilli()))
	require.NoError(b, err)
	defer os.RemoveAll(dir)

	mergeConfig := config.NewMergeConfig(2, keyMapper)

	config := config.NewConfig(".", uint64(kernalPageSize-100), 100, mergeConfig)

	tests := []benchmarkTestCase{
		{"128B", 128},
		{"256B", 256},
		{"512B", 512},
		{"1K", 1024},
		{"2K", 2048},
		{"4K", 4096},
		{"8K", 8192},
		{"16K", 16384},
		{"32K", 32768},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			b.SetBytes(int64(test.size))

			key := serializableKey("Foo")
			value := []byte(strings.Repeat(" ", test.size))

			db, err := NewDB(config)
			require.NoError(b, err)

			err = db.Put(key, value)
			require.NoError(b, err)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				val, err := db.Get(key)
				require.NoError(b, err)
				require.Equal(b, val, value)
			}
			b.StopTimer()
		})
	}
}

func BenchmarkPut(b *testing.B) {
	kernalPageSize := os.Getpagesize()
	dir, err := os.MkdirTemp(os.TempDir(), fmt.Sprintf("%v", time.Now().UnixMilli()))
	require.NoError(b, err)
	defer os.RemoveAll(dir)

	mergeConfig := config.NewMergeConfig(2, keyMapper)

	config := config.NewConfig(".", uint64((kernalPageSize-100)*10), 100, mergeConfig)

	tests := []benchmarkTestCase{
		{"128B", 128},
		{"256B", 256},
		{"512B", 512},
		{"1K", 1024},
		{"2K", 2048},
		{"4K", 4096},
		{"8K", 8192},
		{"16K", 16384},
		{"32K", 32768},
	}

	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			b.SetBytes(int64(test.size))

			key := serializableKey("Foo")
			value := []byte(strings.Repeat(" ", test.size))

			db, err := NewDB(config)
			require.NoError(b, err)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				err := db.Put(key, value)
				require.NoError(b, err)
			}
			// b.StopTimer()
		})
	}
}
