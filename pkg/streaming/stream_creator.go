package streaming

import (
	"bytes"
	"fmt"
	"time"
)

type StreamCreator struct {
	streamName         string
	maxAge             time.Duration
	maxLenByteCapacity *ByteCapacity
	client             *Client
}

func (s StreamCreator) Stream(streamName string) StreamCreator {
	s.streamName = streamName
	return s
}

func (s StreamCreator) MaxAge(maxAge time.Duration) StreamCreator {
	s.maxAge = maxAge
	return s
}

func (s StreamCreator) MaxLengthBytes(maxLength *ByteCapacity) StreamCreator {
	s.maxLenByteCapacity = maxLength
	return s
}

func (s StreamCreator) buildParameters() (map[string]string, error) {
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

func (s StreamCreator) Create() error {
	resp := s.client.coordinator.NewResponse()
	length := 2 + 2 + 4 + 2 + len(s.streamName) + 4
	correlationId := resp.correlationid
	args, err := s.buildParameters()
	if err != nil {
		_ = s.client.coordinator.RemoveResponseById(resp.correlationid)
		return err
	}
	for key, element := range args {
		length = length + 2 + len(key) + 2 + len(element)
	}
	var b = bytes.NewBuffer(make([]byte, 0, length))
	WriteInt(b, length)
	WriteShort(b, CommandCreateStream)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteString(b, s.streamName)
	WriteInt(b, len(args))

	for key, element := range args {
		WriteString(b, key)
		WriteString(b, element)
	}

	return s.client.HandleWrite(b.Bytes(), resp)

}
func (c *Client) StreamCreator() *StreamCreator {
	return &StreamCreator{client: c}
}
