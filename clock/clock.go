package clock

import "time"

type Clock interface {
	Now() int64
}

type SystemClock struct{}

func NewSystemClock() *SystemClock {
	return &SystemClock{}
}

func (sc *SystemClock) Now() int64 {
	return time.Now().UnixNano()
}
