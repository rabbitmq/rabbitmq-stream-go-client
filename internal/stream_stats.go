package internal

import (
	"bufio"
	"bytes"
	"fmt"
)

type StreamStatsRequest struct {
	correlationId uint32
	stream        string // the stream to receive stats about
}

func NewStreamStatsRequest(stream string) *StreamStatsRequest {
	return &StreamStatsRequest{stream: stream}
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

func (s *StreamStatsRequest) Stream() string {
	return s.stream
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

func (s *StreamStatsRequest) UnmarshalBinary(data []byte) error {
	return readMany(bytes.NewReader(data), &s.correlationId, &s.stream)
}

type StreamStatsResponse struct {
	correlationId uint32
	responseCode  uint16
	Stats         map[string]int64
}

func NewStreamStatsResponseWith(correlationId uint32, responseCode uint16, stats map[string]int64) *StreamStatsResponse {
	return &StreamStatsResponse{
		correlationId: correlationId,
		responseCode:  responseCode,
		Stats:         stats,
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

func (sr *StreamStatsResponse) MarshalBinary() (data []byte, err error) {
	buff := &bytes.Buffer{}
	wr := bufio.NewWriter(buff)

	n, err := writeMany(wr, sr.correlationId, sr.responseCode, uint32(len(sr.Stats)))
	if err != nil {
		return nil, err
	}

	for k, v := range sr.Stats {
		kBytes, err := writeString(wr, k)
		if err != nil {
			return nil, err
		}
		vBytes, err := writeMany(wr, v)
		if err != nil {
			return nil, err
		}

		n += kBytes + vBytes
	}

	expectedBytesWritten := streamProtocolCorrelationIdSizeBytes + streamProtocolResponseCodeSizeBytes +
		streamProtocolMapLenBytes +
		(streamProtocolStringLenSizeBytes * len(sr.Stats)) + // 2 bytes for each string length
		(streamProtocolKeySizeInt64 * len(sr.Stats)) // 8 bytes for each stat value

	for k, _ := range sr.Stats {
		expectedBytesWritten += len(k) // length of each string
	}

	if n != expectedBytesWritten {
		return nil, fmt.Errorf("did not write expected number of bytes: wanted %d, wrote %d", expectedBytesWritten, n)
	}

	if err = wr.Flush(); err != nil {
		return nil, err
	}
	data = buff.Bytes()
	return
}
