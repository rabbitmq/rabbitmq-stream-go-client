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

type StreamStatsResponse struct {
	correlationId uint32
	responseCode  uint16
	Stats         map[string]int64
}

func NewStreamStatsResponseWith(correlationId uint32, responseCode uint16) *StreamStatsResponse {
	return &StreamStatsResponse{
		correlationId: correlationId,
		responseCode:  responseCode,
	}
}

func NewStreamStatsResponse() *StreamStatsResponse {
	return &StreamStatsResponse{}
}

func (sr *StreamStatsResponse) CorrelationId() uint32 {
	return sr.correlationId
}

func (sr *StreamStatsResponse) ResponseCode() uint16 {
	return sr.responseCode
}

func (sr *StreamStatsResponse) Read(reader *bufio.Reader) error {
	err := readMany(reader, &sr.correlationId, &sr.responseCode)
	if err != nil {
		return err
	}

	var statsLen uint32
	err = readAny(reader, &statsLen)
	if err != nil {
		return err
	}

	sr.Stats = make(map[string]int64, statsLen)

	for i := uint32(0); i < statsLen; i++ {
		key := readString(reader)
		value, err := readInt64(reader)
		if err != nil {
			return err
		}
		sr.Stats[key] = value
	}

	return nil
}
