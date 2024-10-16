package config

import "time"

type MergeConfig[Key BitcaskKey] struct {
	totalSegmentsToRead   int
	shouldReadAllSegments bool
	keyMapper             func([]byte) Key
	runMergeEvery         time.Duration
}

func NewMergeConfig[Key BitcaskKey](totalSegmentsToRead int, keyMapper func([]byte) Key) *MergeConfig[Key] {
	return &MergeConfig[Key]{
		totalSegmentsToRead:   totalSegmentsToRead,
		shouldReadAllSegments: false,
		keyMapper:             keyMapper,
		runMergeEvery:         5 * time.Minute,
	}
}

func NewMergeConfigWithDuration[Key BitcaskKey](totalSegmentsToRead int, runMergeEvery time.Duration, keyMapper func([]byte) Key) *MergeConfig[Key] {
	return &MergeConfig[Key]{
		totalSegmentsToRead:   totalSegmentsToRead,
		shouldReadAllSegments: false,
		keyMapper:             keyMapper,
		runMergeEvery:         runMergeEvery,
	}
}

func NewMergeConfigWithAllSegmentsToRead[Key BitcaskKey](keyMapper func([]byte) Key) *MergeConfig[Key] {
	return &MergeConfig[Key]{
		shouldReadAllSegments: true,
		keyMapper:             keyMapper,
		runMergeEvery:         5 * time.Minute,
	}
}

func NewMergeConfigWithAllSegmentsToReadEveryFixedDuration[Key BitcaskKey](runMergeEvery time.Duration, keyMapper func([]byte) Key) *MergeConfig[Key] {
	return &MergeConfig[Key]{
		shouldReadAllSegments: true,
		keyMapper:             keyMapper,
		runMergeEvery:         runMergeEvery,
	}
}

func (mergeConfig *MergeConfig[Key]) TotalSegmentsToRead() int {
	return mergeConfig.totalSegmentsToRead
}

func (mergeConfig *MergeConfig[Key]) ShouldReadAllSegments() bool {
	return mergeConfig.shouldReadAllSegments
}

func (mergeConfig *MergeConfig[Key]) KeyMapper() func([]byte) Key {
	return mergeConfig.keyMapper
}

func (mergeConfig *MergeConfig[Key]) RunMergeEvery() time.Duration {
	return mergeConfig.runMergeEvery
}
