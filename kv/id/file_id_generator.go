package id

import "ashishkujoy/bitcask/clock"

type TimestampBasedFileIdGenerator struct {
	clock clock.Clock
}

func NewTimestampBasedFileIdGenerator(clock clock.Clock) *TimestampBasedFileIdGenerator {
	return &TimestampBasedFileIdGenerator{
		clock: clock,
	}
}

func (t TimestampBasedFileIdGenerator) Next() uint64 {
	return uint64(t.clock.Now())
}
