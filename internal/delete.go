package internal

import (
	"bufio"
	"bytes"
)

type DeleteRequest struct {
	correlationId uint32
	stream        string
}

func (c *DeleteRequest) Stream() string {
	return c.stream
}

func (c *DeleteRequest) UnmarshalBinary(data []byte) error {
	buff := bytes.NewReader(data)
	rd := bufio.NewReader(buff)
	return readMany(rd, &c.correlationId, &c.stream)
}

func NewDeleteRequest(stream string) *DeleteRequest {
	return &DeleteRequest{stream: stream}
}

func (c *DeleteRequest) Write(writer *bufio.Writer) (int, error) {
	return writeMany(writer, c.correlationId, c.stream)
}

func (c *DeleteRequest) Key() uint16 {
	return CommandDelete
}

func (c *DeleteRequest) SizeNeeded() int {
	return streamProtocolKeySizeBytes +
		streamProtocolVersionSizeBytes +
		streamProtocolCorrelationIdSizeBytes +
		streamProtocolStringLenSizeBytes + len(c.stream)
}

func (c *DeleteRequest) SetCorrelationId(id uint32) {
	c.correlationId = id
}

func (c *DeleteRequest) CorrelationId() uint32 {
	return c.correlationId
}

func (c *DeleteRequest) Version() int16 {
	return Version1
}
