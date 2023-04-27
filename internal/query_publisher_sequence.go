package internal

import (
	"bufio"
	"bytes"
	"fmt"
)

type QueryPublisherSequenceRequest struct {
	correlationId      uint32
	publisherReference string
	stream             string
}

func NewQueryPublisherSequenceRequest(pubRef, stream string) *QueryPublisherSequenceRequest {
	return &QueryPublisherSequenceRequest{
		publisherReference: pubRef,
		stream:             stream,
	}
}

func (q *QueryPublisherSequenceRequest) Key() uint16 {
	return CommandQueryPublisherSequence
}

func (q *QueryPublisherSequenceRequest) Version() int16 {
	return Version1
}

func (q *QueryPublisherSequenceRequest) CorrelationId() uint32 {
	return q.correlationId
}

func (q *QueryPublisherSequenceRequest) PublisherReference() string {
	return q.publisherReference
}

func (q *QueryPublisherSequenceRequest) Stream() string {
	return q.stream
}

func (q *QueryPublisherSequenceRequest) SetCorrelationId(id uint32) {
	q.correlationId = id
}

func (q *QueryPublisherSequenceRequest) SizeNeeded() int {
	return streamProtocolKeySizeUint16 + // key
		streamProtocolVersionSizeBytes + // version
		streamProtocolCorrelationIdSizeBytes + // correlationId
		streamProtocolStringLenSizeBytes + len(q.publisherReference) + // publisherReference
		streamProtocolStringLenSizeBytes + len(q.stream) // stream
}

func (q *QueryPublisherSequenceRequest) Write(writer *bufio.Writer) (int, error) {
	return writeMany(writer, q.correlationId, q.publisherReference, q.stream)
}

func (q *QueryPublisherSequenceRequest) UnmarshalBinary(data []byte) error {
	return readMany(bytes.NewReader(data), &q.correlationId, &q.publisherReference, &q.stream)
}

type QueryPublisherSequenceResponse struct {
	correlationId uint32
	responseCode  uint16
	sequence      uint64
}

func NewQueryPublisherSequenceResponse() *QueryPublisherSequenceResponse {
	return &QueryPublisherSequenceResponse{}
}
func NewQueryPublisherSequenceResponseWith(correlationId uint32, responseCode uint16, sequence uint64) *QueryPublisherSequenceResponse {
	return &QueryPublisherSequenceResponse{
		correlationId: correlationId,
		responseCode:  responseCode,
		sequence:      sequence,
	}
}

func (qr *QueryPublisherSequenceResponse) Read(reader *bufio.Reader) error {
	err := readMany(reader, &qr.correlationId, &qr.responseCode, &qr.sequence)
	if err != nil {
		return err
	}

	return nil
}

func (qr *QueryPublisherSequenceResponse) CorrelationId() uint32 {
	return qr.correlationId
}

func (qr *QueryPublisherSequenceResponse) ResponseCode() uint16 {
	return qr.responseCode
}

func (qr *QueryPublisherSequenceResponse) Sequence() uint64 {
	return qr.sequence
}

func (qr *QueryPublisherSequenceResponse) MarshalBinary() (data []byte, err error) {
	buff := &bytes.Buffer{}
	wr := bufio.NewWriter(buff)

	n, err := writeMany(wr, qr.correlationId, qr.responseCode, qr.sequence)
	if err != nil {
		return nil, err
	}

	expectedBytesWritten := streamProtocolCorrelationIdSizeBytes + streamProtocolResponseCodeSizeBytes +
		streamProtocolKeySizeUint64 // sequence

	if n != expectedBytesWritten {
		return nil, fmt.Errorf("did not write expected number of bytes: wanted %d, wrote %d", expectedBytesWritten, n)
	}

	if err = wr.Flush(); err != nil {
		return nil, err
	}

	data = buff.Bytes()
	return
}
