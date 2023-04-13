package internal

import (
	"bufio"
	"bytes"
)

// QueryOffsetRequest is used to query the offset of a stream for a given consumer reference.
// ref: https://github.com/rabbitmq/rabbitmq-server/blob/main/deps/rabbitmq_stream/docs/PROTOCOL.adoc#queryoffset
type QueryOffsetRequest struct {
	correlationId     uint32
	consumerReference string // max 256 characters
	stream            string
}

func (c *QueryOffsetRequest) Stream() string {
	return c.stream
}

func (c *QueryOffsetRequest) ConsumerReference() string {
	return c.consumerReference
}

func (c *QueryOffsetRequest) UnmarshalBinary(data []byte) error {
	buff := bytes.NewReader(data)
	rd := bufio.NewReader(buff)
	return readMany(rd, &c.correlationId, &c.consumerReference, &c.stream)
}

func NewQueryOffsetRequest(consumerReference string, stream string) *QueryOffsetRequest {
	return &QueryOffsetRequest{consumerReference: consumerReference, stream: stream}
}

func (c *QueryOffsetRequest) Write(writer *bufio.Writer) (int, error) {
	return writeMany(writer, c.correlationId, c.consumerReference, c.stream)
}

func (c *QueryOffsetRequest) Key() uint16 {
	return CommandQueryOffset
}

func (c *QueryOffsetRequest) SizeNeeded() int {
	return streamProtocolHeader +
		streamProtocolStringLenSizeBytes + len(c.consumerReference) + // consumer reference
		streamProtocolStringLenSizeBytes + len(c.stream) // stream
}

func (c *QueryOffsetRequest) SetCorrelationId(id uint32) {
	c.correlationId = id
}

func (c *QueryOffsetRequest) CorrelationId() uint32 {
	return c.correlationId
}

func (c *QueryOffsetRequest) Version() int16 {
	return Version1
}

type QueryOffsetResponse struct {
	correlationId uint32
	responseCode  uint16
	offset        uint64
}

func (c *QueryOffsetResponse) UnmarshalBinary(data []byte) error {
	buff := bytes.NewReader(data)
	rd := bufio.NewReader(buff)
	return readMany(rd, &c.correlationId, &c.responseCode, &c.offset)
}

func (c *QueryOffsetResponse) Read(reader *bufio.Reader) error {
	return readMany(reader, &c.correlationId, &c.responseCode, &c.offset)
}

func (c *QueryOffsetResponse) CorrelationId() uint32 {
	return c.correlationId
}

func NewQueryOffsetResponseWith(correlationId uint32, responseCode uint16, offset uint64) *QueryOffsetResponse {
	return &QueryOffsetResponse{correlationId: correlationId, responseCode: responseCode, offset: offset}
}
