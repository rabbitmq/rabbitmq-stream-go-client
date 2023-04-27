package internal

import "bufio"

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

type QueryPublisherSequenceResponse struct {
	correlationId uint32
	responseCode  int16
	sequence      uint64
}

func NewQueryPublisherResponse() *QueryPublisherSequenceResponse {
	return &QueryPublisherSequenceResponse{}
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

func (qr *QueryPublisherSequenceResponse) ResponseCode() int16 {
	return qr.responseCode
}

func (qr *QueryPublisherSequenceResponse) Sequence() uint64 {
	return qr.sequence
}
