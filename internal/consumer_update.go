package internal

import (
	"bufio"
	"bytes"
	"fmt"
)

type ConsumerUpdateQuery struct {
	correlationId  uint32
	subscriptionId uint8
	active         uint8
}

func NewConsumerUpdateQuery(subscriptionId, active uint8) *ConsumerUpdateQuery {
	return &ConsumerUpdateQuery{
		subscriptionId: subscriptionId,
		active:         active,
	}
}

func (c *ConsumerUpdateQuery) SetCorrelationId(id uint32) {
	c.correlationId = id
}

func (c *ConsumerUpdateQuery) CorrelationId() uint32 {
	return c.correlationId
}

func (c *ConsumerUpdateQuery) Key() uint16 {
	return CommandConsumerUpdate
}

func (c *ConsumerUpdateQuery) Version() int16 {
	return Version1
}

func (c *ConsumerUpdateQuery) SizeNeeded() int {
	return streamProtocolHeader +
		(streamProtocolKeySizeUint8 * 2) // subscription Id and active are same size
}

func (c *ConsumerUpdateQuery) Write(w *bufio.Writer) (int, error) {
	n, err := writeMany(w, c.correlationId, c.subscriptionId, c.active)

	if err != nil {
		return n, err
	}

	expectedBytesWritten := streamProtocolCorrelationIdSizeBytes +
		(streamProtocolKeySizeUint8 * 2)

	if n != expectedBytesWritten {
		return n, fmt.Errorf("did not write expected amount of bytes: wrote %d expected %d", n, expectedBytesWritten)
	}

	return n, nil
}

func (c *ConsumerUpdateQuery) UnmarshalBinary(data []byte) error {
	return readMany(bytes.NewReader(data), &c.correlationId, &c.subscriptionId, &c.active)
}

type ConsumerUpdateResponse struct {
	correlationId       uint32
	responseCode        uint16
	offsetSpecification OffsetSpecification
}

type OffsetSpecification struct {
	offsetType uint16
	offset     uint64
}

func NewConsumerUpdateResponse(correlationId uint32, responseCode, offsetType uint16, offset uint64) *ConsumerUpdateResponse {
	return &ConsumerUpdateResponse{
		correlationId: correlationId,
		responseCode:  responseCode,
		offsetSpecification: OffsetSpecification{
			offsetType: offsetType,
			offset:     offset,
		},
	}
}

func (c *ConsumerUpdateResponse) CorrelationId() uint32 {
	return c.correlationId
}

func (c *ConsumerUpdateResponse) ResponseCode() uint16 {
	return c.responseCode
}

func (c *ConsumerUpdateResponse) OffsetType() uint16 {
	return c.offsetSpecification.offsetType
}

func (c *ConsumerUpdateResponse) Offset() uint64 {
	return c.offsetSpecification.offset
}

func (c *ConsumerUpdateResponse) Read(rd *bufio.Reader) error {
	err := readMany(rd, &c.correlationId, &c.responseCode,
		&c.offsetSpecification.offsetType, &c.offsetSpecification.offset)

	if err != nil {
		return err
	}

	return nil
}

func (c *ConsumerUpdateResponse) MarshalBinary() ([]byte, error) {
	var buff bytes.Buffer
	wr := bufio.NewWriter(&buff)
	_, err := writeMany(
		wr,
		c.correlationId,
		c.responseCode,
		c.offsetSpecification.offsetType,
		c.offsetSpecification.offset,
	)

	if err != nil {
		return nil, err
	}

	if err = wr.Flush(); err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}
