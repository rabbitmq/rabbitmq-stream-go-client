package internal

import (
	"bufio"
	"bytes"
)

type DeletePublisherRequest struct {
	correlationId uint32
	publisherId   uint8
}

func (c *DeletePublisherRequest) PublisherId() uint8 {
	return c.publisherId
}

func (c *DeletePublisherRequest) UnmarshalBinary(data []byte) error {
	buff := bytes.NewReader(data)
	rd := bufio.NewReader(buff)
	return readMany(rd, &c.correlationId, &c.publisherId)
}

func NewDeletePublisherRequest(publisherId uint8) *DeletePublisherRequest {
	return &DeletePublisherRequest{publisherId: publisherId}
}

func (c *DeletePublisherRequest) Write(writer *bufio.Writer) (int, error) {
	return writeMany(writer, c.correlationId, c.publisherId)
}

func (c *DeletePublisherRequest) Key() uint16 {
	return CommandDeletePublisher
}

func (c *DeletePublisherRequest) SizeNeeded() int {
	return streamProtocolHeader + streamProtocolKeySizeUint8
}

func (c *DeletePublisherRequest) SetCorrelationId(id uint32) {
	c.correlationId = id
}

func (c *DeletePublisherRequest) CorrelationId() uint32 {
	return c.correlationId
}

func (c *DeletePublisherRequest) Version() int16 {
	return Version1
}
