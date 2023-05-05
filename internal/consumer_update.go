package internal

import (
	"bufio"
	"bytes"
)

// ConsumerUpdateQuery is a frame received by the client to change the active state of a consumer.
// the consumer is identified by the subscriptionId.
type ConsumerUpdateQuery struct {
	correlationId  uint32
	subscriptionId uint8
	active         uint8
}

func NewConsumerUpdateQuery(correlationId uint32, subscriptionId, active uint8) *ConsumerUpdateQuery {
	return &ConsumerUpdateQuery{
		correlationId:  correlationId,
		subscriptionId: subscriptionId,
		active:         active,
	}
}

func (c *ConsumerUpdateQuery) Key() uint16 {
	return CommandConsumerUpdateQuery
}

func (c *ConsumerUpdateQuery) MinVersion() int16 {
	return Version1
}

func (c *ConsumerUpdateQuery) MaxVersion() int16 {
	return Version1
}

func (c *ConsumerUpdateQuery) SetCorrelationId(id uint32) {
	c.correlationId = id
}

func (c *ConsumerUpdateQuery) CorrelationId() uint32 {
	return c.correlationId
}

func (c *ConsumerUpdateQuery) ResponseCode() uint16 {
	return uint16(1)
}

func (c *ConsumerUpdateQuery) SubscriptionId() uint8 {
	return c.subscriptionId
}

func (c *ConsumerUpdateQuery) Active() uint8 {
	return c.active
}

func (c *ConsumerUpdateQuery) Read(rd *bufio.Reader) error {
	return readMany(rd, &c.correlationId, &c.subscriptionId, &c.active)
}

func (c *ConsumerUpdateQuery) MarshalBinary() (data []byte, err error) {
	buff := &bytes.Buffer{}
	_, err = writeMany(buff, c.correlationId, c.subscriptionId, c.active)
	if err != nil {
		return nil, err
	}
	data = buff.Bytes()
	return
}

// ConsumerUpdateResponse is a frame sent by the client to in response to an active state change of a
// consumer. The offsetType and offset are calculated by the client.
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
func (c *ConsumerUpdateResponse) Key() uint16 {
	return CommandConsumerUpdateResponse
}

func (c *ConsumerUpdateResponse) Version() int16 {
	return Version1
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

func (c *ConsumerUpdateResponse) SizeNeeded() int {
	return streamProtocolHeader +
		streamProtocolResponseCodeSizeBytes +
		streamProtocolKeySizeUint16 +
		streamProtocolKeySizeUint64
}

func (c *ConsumerUpdateResponse) Write(w *bufio.Writer) (int, error) {
	return writeMany(
		w,
		c.correlationId,
		c.responseCode,
		c.offsetSpecification.offsetType,
		c.offsetSpecification.offset,
	)
}

func (c *ConsumerUpdateResponse) UnmarshalBinary(data []byte) error {
	return readMany(bytes.NewReader(data),
		&c.correlationId,
		&c.responseCode,
		&c.offsetSpecification.offsetType,
		&c.offsetSpecification.offset,
	)
}
