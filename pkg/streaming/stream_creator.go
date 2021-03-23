package streaming

import (
	"bytes"
	"fmt"
	"strconv"
	"time"
)

type StreamCreator struct {
	streamName     string
	maxAge         time.Duration
	maxLengthBytes int
	client         *Client
}

func (s StreamCreator) Stream(streamName string) StreamCreator {
	s.streamName = streamName
	return s
}

func (s StreamCreator) MaxAge(maxAge time.Duration) StreamCreator {
	s.maxAge = maxAge
	return s
}

func (s StreamCreator) MaxLengthBytes(maxLength int) StreamCreator {
	s.maxLengthBytes = maxLength
	return s
}

func (s StreamCreator) buildParameters() map[string]string {
	res := map[string]string{"queue-leader-locator": "least-leaders"}

	if s.maxLengthBytes > 0 {
		res["max-length-bytes"] = strconv.Itoa(s.maxLengthBytes)
	}

	if s.maxAge > 0 {
		res["max-age"] = fmt.Sprintf("%s", s.maxAge)
	}
	return res
}

func (s StreamCreator) Create() error {
	resp := s.client.responses.New()
	length := 2 + 2 + 4 + 2 + len(s.streamName) + 4
	correlationId := resp.subId
	args := s.buildParameters()
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
func (client *Client) StreamCreator() *StreamCreator {
	return &StreamCreator{client: client}
}
