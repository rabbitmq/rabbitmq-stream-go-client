package internal

import (
	"bufio"
	"bytes"
)

// StoreOffsetRequest sends the offset for a given stream.
// ref: https://github.com/rabbitmq/rabbitmq-server/blob/main/deps/rabbitmq_stream/docs/PROTOCOL.adoc#storeoffset
type StoreOffsetRequest struct {
	reference string // max 256 characters
	stream    string
	offset    uint64
}

func NewStoreOffsetRequest(reference, stream string, offset uint64) *StoreOffsetRequest {
	return &StoreOffsetRequest{
		reference: reference,
		stream:    stream,
		offset:    offset,
	}
}

func (s *StoreOffsetRequest) SizeNeeded() int {
	return streamProtocolKeySizeUint16 + // key
		streamProtocolVersionSizeBytes + // version
		streamProtocolStringLenSizeBytes + len(s.reference) + // reference
		streamProtocolStringLenSizeBytes + len(s.stream) + // stream
		streamProtocolKeySizeUint64
}

func (s *StoreOffsetRequest) Key() uint16 {
	return CommandStoreOffset
}

func (s *StoreOffsetRequest) Version() int16 {
	return Version1
}

func (s *StoreOffsetRequest) Stream() string {
	return s.stream
}

func (s *StoreOffsetRequest) Reference() string {
	return s.reference
}

func (s *StoreOffsetRequest) Offset() uint64 {
	return s.offset
}

func (s *StoreOffsetRequest) Write(writer *bufio.Writer) (int, error) {
	return writeMany(
		writer,
		s.reference,
		s.stream,
		s.offset,
	)
}

func (s *StoreOffsetRequest) UnmarshalBinary(data []byte) error {
	buff := bytes.NewReader(data)
	r := bufio.NewReader(buff)
	return readMany(r, &s.reference, &s.stream, &s.offset)
}
