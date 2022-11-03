package internal

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
)

// CloseRequest can be initiated from the Client or from the Server. This struct must implement both internal.CommandRead
// and internal.CommandWrite
type CloseRequest struct {
	correlationId uint32
	closingCode   uint16
	closingReason string
}

func NewCloseRequest(closingCode uint16, closingReason string) *CloseRequest {
	return &CloseRequest{closingCode: closingCode, closingReason: closingReason}
}

func (c *CloseRequest) ClosingCode() uint16 {
	return c.closingCode
}

func (c *CloseRequest) ClosingReason() string {
	return c.closingReason
}

func (c *CloseRequest) Read(reader *bufio.Reader) error {
	return readMany(reader, &c.correlationId, &c.closingCode, &c.closingReason)
}

func (c *CloseRequest) Write(writer *bufio.Writer) (int, error) {
	n, err := writeMany(writer, c.correlationId, c.closingCode, c.closingReason)
	if err != nil {
		return n, err
	}

	if n != (c.SizeNeeded() - 4) {
		return n, fmt.Errorf(
			"write did not write expected amount of bytes: expected %d wrote %d",
			c.SizeNeeded()-4,
			n,
		)
	}
	return n, nil
}

func (c *CloseRequest) Key() uint16 {
	return CommandClose
}

func (c *CloseRequest) SizeNeeded() int {
	return streamProtocolKeySizeBytes +
		streamProtocolVersionSizeBytes +
		streamProtocolCorrelationIdSizeBytes +
		streamProtocolClosingCodeSizeBytes +
		streamProtocolStringLenSizeBytes +
		len(c.closingReason)
}

func (c *CloseRequest) SetCorrelationId(correlationId uint32) {
	c.correlationId = correlationId
}

func (c *CloseRequest) CorrelationId() uint32 {
	return c.correlationId
}

func (c *CloseRequest) Version() int16 {
	return Version1
}

type CloseResponse struct {
	correlationId uint32
	responseCode  uint16
}

func NewCloseResponse(correlationId uint32, responseCode uint16) *CloseResponse {
	return &CloseResponse{correlationId: correlationId, responseCode: responseCode}
}

func (c *CloseResponse) MarshalBinary() (data []byte, err error) {
	buff := new(bytes.Buffer)
	wr := bufio.NewWriter(buff)
	n, err := writeMany(wr, c.correlationId, c.responseCode)
	if err != nil {
		return nil, err
	}
	if n != 6 {
		return nil, errors.New("did not write expected number of bytes")
	}

	err = wr.Flush()
	if err != nil {
		return nil, err
	}
	data = buff.Bytes()
	return
}

func (c *CloseResponse) Write(writer *bufio.Writer) (int, error) {
	n, err := writeMany(writer, c.correlationId, c.responseCode)
	if err != nil {
		return n, err
	}
	if n != (c.SizeNeeded() - 4) {
		return n, fmt.Errorf("short write: expected %d wrote %d", c.SizeNeeded()-4, n)
	}
	return n, nil
}

func (c *CloseResponse) Key() uint16 {
	return EncodeResponseCode(CommandClose)
}

func (c *CloseResponse) SizeNeeded() int {
	return streamProtocolKeySizeBytes +
		streamProtocolVersionSizeBytes +
		streamProtocolCorrelationIdSizeBytes +
		streamProtocolResponseCodeSizeBytes
}

func (c *CloseResponse) SetCorrelationId(id uint32) {
	c.correlationId = id
}

func (c *CloseResponse) Version() int16 {
	return Version1
}

func (c *CloseResponse) Read(reader *bufio.Reader) error {
	return readMany(reader, &c.correlationId, &c.responseCode)
}

func (c *CloseResponse) CorrelationId() uint32 {
	return c.correlationId
}

func (c *CloseResponse) ResponseCode() uint16 {
	return c.responseCode
}
