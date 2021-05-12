package stream

import (
	"fmt"
	"time"
)

type StreamOptions struct {
	MaxAge         time.Duration
	MaxLengthBytes *ByteCapacity
}

func (s *StreamOptions) SetMaxAge(maxAge time.Duration) *StreamOptions {
	s.MaxAge = maxAge
	return s
}

func (s *StreamOptions) SetMaxLengthBytes(maxLength *ByteCapacity) *StreamOptions {
	s.MaxLengthBytes = maxLength
	return s
}

func (s StreamOptions) buildParameters() (map[string]string, error) {
	res := map[string]string{"queue-leader-locator": "least-leaders"}

	if s.MaxLengthBytes != nil {
		if s.MaxLengthBytes.error != nil {
			return nil, s.MaxLengthBytes.error
		}

		if s.MaxLengthBytes.bytes > 0 {
			res["max-length-bytes"] = fmt.Sprintf("%d", s.MaxLengthBytes.bytes)
		}
	}

	if s.MaxAge > 0 {
		res["max-age"] = s.MaxAge.String()
	}
	return res, nil
}

func NewStreamOptions() *StreamOptions {
	return &StreamOptions{}
}
