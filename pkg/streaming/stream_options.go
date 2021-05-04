package streaming

import (
	"fmt"
	"time"
)

type StreamOptions struct {
	streamName         string
	maxAge             time.Duration
	maxLenByteCapacity *ByteCapacity
}

func (s *StreamOptions) Stream(streamName string) *StreamOptions {
	s.streamName = streamName
	return s
}

func (s *StreamOptions) MaxAge(maxAge time.Duration) *StreamOptions {
	s.maxAge = maxAge
	return s
}

func (s *StreamOptions) MaxLengthBytes(maxLength *ByteCapacity) *StreamOptions {
	s.maxLenByteCapacity = maxLength
	return s
}

func (s StreamOptions) buildParameters() (map[string]string, error) {
	res := map[string]string{"queue-leader-locator": "least-leaders"}

	if s.maxLenByteCapacity != nil {
		if s.maxLenByteCapacity.error != nil {
			return nil, s.maxLenByteCapacity.error
		}

		if s.maxLenByteCapacity.bytes > 0 {
			res["max-length-bytes"] = fmt.Sprintf("%d", s.maxLenByteCapacity.bytes)
		}
	}

	if s.maxAge > 0 {
		res["max-age"] = s.maxAge.String()
	}
	return res, nil
}

func NewStreamOptions() *StreamOptions {
	return &StreamOptions{}
}
