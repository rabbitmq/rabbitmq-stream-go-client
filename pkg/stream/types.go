package stream

import (
	"fmt"
	"time"
)

const (
	Kilobyte = 1_000
	Megabyte = Kilobyte * 1_000
	Gigabyte = Megabyte * 1_000
	Terabyte = Gigabyte * 1_000
)

var (
	ErrNoLocators = fmt.Errorf("no locators configured")
)

type ByteCapacity uint64

func (b ByteCapacity) String() string {
	return fmt.Sprintf("%d", b)
}

// StreamOptions is an alias for backwards compatibility with v1 of this client.
type StreamOptions = CreateStreamOptions

type CreateStreamOptions struct {
	MaxAge         time.Duration
	MaxLength      ByteCapacity
	MaxSegmentSize ByteCapacity
}
