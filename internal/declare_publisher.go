package internal

import (
	"bufio"
	"bytes"
)

type DeclarePublisherRequest struct {
	correlationId      uint32
	publisherId        uint8  // the client must provide a unique publisher id
	publisherReference string // max 256 characters
	stream             string
}

func (c *DeclarePublisherRequest) Stream() string {
	return c.stream
}

func (c *DeclarePublisherRequest) Reference() string {
	return c.publisherReference
}

func (c *DeclarePublisherRequest) PublisherId() uint8 {
	return c.publisherId
}

func (c *DeclarePublisherRequest) UnmarshalBinary(data []byte) error {
	buff := bytes.NewReader(data)
	rd := bufio.NewReader(buff)
	return readMany(rd, &c.correlationId, &c.publisherId, &c.publisherReference, &c.stream)
}

func NewDeclarePublisherRequest(publisherId uint8, publisherReference string, stream string) *DeclarePublisherRequest {
	return &DeclarePublisherRequest{publisherId: publisherId, publisherReference: publisherReference, stream: stream}
}

func (c *DeclarePublisherRequest) Write(writer *bufio.Writer) (int, error) {
	return writeMany(writer, c.correlationId, c.publisherId, c.publisherReference, c.stream)
}

func (c *DeclarePublisherRequest) Key() uint16 {
	return CommandDeclarePublisher
}

func (c *DeclarePublisherRequest) SizeNeeded() int {
	return streamProtocolHeader +
		streamProtocolKeySizeUint8 + // publisher id
		streamProtocolStringLenSizeBytes + len(c.publisherReference) + // publisher reference
		streamProtocolStringLenSizeBytes + len(c.stream) // stream
}

func (c *DeclarePublisherRequest) SetCorrelationId(id uint32) {
	c.correlationId = id
}

func (c *DeclarePublisherRequest) CorrelationId() uint32 {
	return c.correlationId
}

func (c *DeclarePublisherRequest) Version() int16 {
	return Version1
}
