package internal

import (
	"bufio"
	"bytes"
	"github.com/gsantomaggio/rabbitmq-stream-go-client/pkg/constants"
)

// SubscribeRequest is the command to subscribe to a stream

type SubscribeRequest struct {
	correlationId  uint32
	subscriptionId uint8  // the client must provide a unique subscription id
	offsetType     uint16 // 1 (first), 2 (last), 3 (next), 4 (offset), 5 (timestamp) see the constants
	// OffsetTypeFirst, OffsetTypeLast, OffsetTypeNext, OffsetTypeOffset, OffsetTypeTimestamp
	offset uint64 // (for offset) | int64 (for timestamp)
	stream string // the stream to subscribe
	credit uint16 // initial credit to be given to the client
	// (usually) that should be between 1 and 10
	properties constants.SubscribeProperties // map of properties (optional)
	// for example: "name": "my-subscription-name"
}

func (s *SubscribeRequest) Offset() uint64 {
	return s.offset
}

func (s *SubscribeRequest) Credit() uint16 {
	return s.credit
}

func (s *SubscribeRequest) Properties() constants.SubscribeProperties {
	return s.properties
}

func (s *SubscribeRequest) OffsetType() uint16 {
	return s.offsetType
}

func (s *SubscribeRequest) Stream() string {
	return s.stream
}

func (s *SubscribeRequest) SubscriptionId() uint8 {
	return s.subscriptionId
}

func (s *SubscribeRequest) UnmarshalBinary(data []byte) error {
	buff := bytes.NewReader(data)
	rd := bufio.NewReader(buff)
	return readMany(rd, &s.correlationId, &s.subscriptionId, &s.stream, &s.offsetType,
		&s.offset, &s.credit, &s.properties)
}

func NewSubscribeRequestRequest(subscriptionId uint8, stream string, offsetType uint16,
	offset uint64, credit uint16,
	properties constants.SubscribeProperties) *SubscribeRequest {
	return &SubscribeRequest{subscriptionId: subscriptionId, stream: stream, offsetType: offsetType,
		offset: offset, credit: credit,
		properties: properties}
}

func (s *SubscribeRequest) Write(writer *bufio.Writer) (int, error) {
	return writeMany(writer, s.correlationId, s.subscriptionId, s.stream, s.offsetType,
		s.offset, s.credit, s.properties)
}

func (s *SubscribeRequest) Key() uint16 {
	return CommandSubscribe
}

func (s *SubscribeRequest) SizeNeeded() int {
	return streamProtocolHeader +
		streamProtocolKeySizeUint8 + // subscriptionId id
		streamProtocolStringLenSizeBytes + len(s.stream) + // stream
		streamProtocolKeySizeUint16 + // offsetType
		streamProtocolKeySizeUint64 + // offset
		streamProtocolKeySizeUint16 + // credit
		sizeNeededForMap(s.properties)
}

func (s *SubscribeRequest) SetCorrelationId(id uint32) {
	s.correlationId = id
}

func (s *SubscribeRequest) CorrelationId() uint32 {
	return s.correlationId
}

func (s *SubscribeRequest) Version() int16 {
	return Version1
}
