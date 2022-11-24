package internal

import (
	"bufio"
	"bytes"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/common"
)

type CreateRequest struct {
	correlationId uint32
	stream        string
	arguments     common.StreamConfiguration
}

func (c *CreateRequest) Arguments() common.StreamConfiguration {
	return c.arguments
}

func (c *CreateRequest) Stream() string {
	return c.stream
}

func (c *CreateRequest) UnmarshalBinary(data []byte) error {
	buff := bytes.NewReader(data)
	rd := bufio.NewReader(buff)
	return readMany(rd, &c.correlationId, &c.stream, &c.arguments)
}

func NewCreateRequest(stream string, configuration map[string]string) *CreateRequest {
	return &CreateRequest{stream: stream, arguments: configuration}
}

func (c *CreateRequest) Write(writer *bufio.Writer) (int, error) {
	return writeMany(writer, c.correlationId, c.stream, c.arguments)
}

func (c *CreateRequest) Key() uint16 {
	return CommandCreate
}

func (c *CreateRequest) SizeNeeded() int {
	return streamProtocolKeySizeBytes + streamProtocolVersionSizeBytes + streamProtocolCorrelationIdSizeBytes +
		streamProtocolStringLenSizeBytes + len(c.stream) +
		sizeNeededForMap(c.arguments)
}

func (c *CreateRequest) SetCorrelationId(id uint32) {
	c.correlationId = id
}

func (c *CreateRequest) CorrelationId() uint32 {
	return c.correlationId
}

func (c *CreateRequest) Version() int16 {
	return Version1
}
