package internal

import (
	"bufio"
	"bytes"
	"fmt"
)

type CreateRequest struct {
	correlationId uint32
	stream        string
	arguments     map[string]string
}

func (c *CreateRequest) Arguments() map[string]string {
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

type CreateResponse struct {
	correlationId uint32
	responseCode  uint16
}

func (c *CreateResponse) MarshalBinary() (data []byte, err error) {
	buff := new(bytes.Buffer)
	wr := bufio.NewWriter(buff)
	n, err := writeMany(wr, c.correlationId, c.responseCode)
	if err != nil {
		return nil, err
	}
	if n != (streamProtocolCorrelationIdSizeBytes + streamProtocolResponseCodeSizeBytes) {
		return nil, fmt.Errorf("error marshalling create response: wrote %d, expected: 6", n)
	}
	if err = wr.Flush(); err != nil {
		return nil, err
	}
	data = buff.Bytes()
	return
}

func NewCreateResponseWith(id uint32, responseCode uint16) *CreateResponse {
	return &CreateResponse{id, responseCode}
}

func (c *CreateResponse) Read(reader *bufio.Reader) error {
	return readMany(reader, &c.correlationId, &c.responseCode)
}

func (c *CreateResponse) CorrelationId() uint32 {
	return c.correlationId
}

func (c *CreateResponse) ResponseCode() uint16 {
	return c.responseCode
}
