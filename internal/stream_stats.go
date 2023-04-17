package internal

import "bufio"

type StreamStatsRequest struct {
	correlationId uint32
	stream        string // the stream to receive stats about
}

func NewStreamStatsRequest(stream string) *StreamStatsRequest {
	return &StreamStatsRequest{
		stream: stream,
	}
}

func (s *StreamStatsRequest) Key() uint16 {
	return CommandStreamStats
}

func (s *StreamStatsRequest) Version() int16 {
	return Version1
}

func (s *StreamStatsRequest) CorrelationId() uint32 {
	return s.correlationId
}

func (s *StreamStatsRequest) SetCorrelationId(id uint32) {
	s.correlationId = id
}

func (s *StreamStatsRequest) SizeNeeded() int {
	return streamProtocolKeySizeUint16 + // key
		streamProtocolVersionSizeBytes + // version
		streamProtocolCorrelationIdSizeBytes + // correlationId
		streamProtocolStringLenSizeBytes + len(s.stream) // stream
}

func (s *StreamStatsRequest) Write(writer *bufio.Writer) (int, error) {
	return writeMany(writer, s.correlationId, s.stream)
}
