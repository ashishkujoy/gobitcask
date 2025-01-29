package config

import "ashishkujoy/bitcask/clock"

type Config[Key BitcaskKey] struct {
	directory           string
	maxSegmentSizeBytes uint64
	mergeConfig         *MergeConfig[Key]
	clock               clock.Clock
}

func NewConfig[Key BitcaskKey](directory string, maxSegmentSizeBytes uint64, mergeConfig *MergeConfig[Key]) *Config[Key] {
	return NewConfigWithClock[Key](directory, maxSegmentSizeBytes, mergeConfig, clock.NewSystemClock())
}

func NewConfigWithClock[Key BitcaskKey](directory string, maxSegmentSizeBytes uint64, mergeConfig *MergeConfig[Key], clock clock.Clock) *Config[Key] {
	return &Config[Key]{
		directory:           directory,
		maxSegmentSizeBytes: maxSegmentSizeBytes,
		mergeConfig:         mergeConfig,
		clock:               clock,
	}
}

func (config *Config[Key]) Directory() string {
	return config.directory
}

func (config *Config[Key]) MaxSegmentSizeInBytes() uint64 {
	return config.maxSegmentSizeBytes
}

func (config *Config[Key]) Clock() clock.Clock {
	return config.clock
}

func (config *Config[Key]) MergeConfig() *MergeConfig[Key] {
	return config.mergeConfig
}
