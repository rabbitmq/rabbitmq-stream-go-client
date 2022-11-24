package internal

import (
	"bufio"
	"bytes"
	"fmt"
)

// SimpleResponse is a response that only contains a correlation ID and a response code.
// Create commands usually return this type of response.
// like CreateStream etc..
type SimpleResponse struct {
	correlationId uint32
	responseCode  uint16
}

func (c *SimpleResponse) MarshalBinary() (data []byte, err error) {
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

func NewSimpleResponseWith(id uint32, responseCode uint16) *SimpleResponse {
	return &SimpleResponse{id, responseCode}
}

func (c *SimpleResponse) Read(reader *bufio.Reader) error {
	return readMany(reader, &c.correlationId, &c.responseCode)
}

func (c *SimpleResponse) CorrelationId() uint32 {
	return c.correlationId
}

func (c *SimpleResponse) ResponseCode() uint16 {
	return c.responseCode
}
