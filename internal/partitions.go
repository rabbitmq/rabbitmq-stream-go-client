package internal

import (
	"bufio"
)

type PartitionsQuery struct {
	correlationId uint32
	superStream   string
}

func NewPartitionsQuery(superStream string) *PartitionsQuery {
	return &PartitionsQuery{
		superStream: superStream,
	}
}

func (p *PartitionsQuery) SetCorrelationId(id uint32) {
	p.correlationId = id
}

func (p *PartitionsQuery) Key() uint16 {
	return CommandPartitions
}

func (p *PartitionsQuery) Version() int16 {
	return Version1
}

func (p *PartitionsQuery) CorrelationId() uint32 {
	return p.correlationId
}

func (p *PartitionsQuery) SuperStream() string {
	return p.superStream
}

func (p *PartitionsQuery) SizeNeeded() int {
	return streamProtocolKeySizeUint16 + // key
		streamProtocolVersionSizeBytes + // version
		streamProtocolCorrelationIdSizeBytes + // correlationId
		streamProtocolStringLenSizeBytes + len(p.superStream) // superStream
}

func (p *PartitionsQuery) Write(writer *bufio.Writer) (int, error) {
	return writeMany(writer, p.correlationId, p.superStream)
}

type PartitionsResponse struct {
	correlationId uint32
	responseCode  int16
	streams       []string
}

func NewPartitionsResponse() *PartitionsResponse {
	return &PartitionsResponse{}
}

func (pr *PartitionsResponse) CorrelationId() uint32 {
	return pr.correlationId
}

func (pr *PartitionsResponse) ResponseCode() int16 {
	return pr.responseCode
}

func (pr *PartitionsResponse) Streams() []string {
	return pr.streams
}

func (pr *PartitionsResponse) Read(reader *bufio.Reader) error {
	err := readMany(reader, &pr.correlationId, &pr.responseCode)
	if err != nil {
		return err
	}

	sliceLen, err := readUInt(reader)
	if err != nil {
		return err
	}

	pr.streams = make([]string, sliceLen)
	for i := uint32(0); i < sliceLen; i++ {
		v := readString(reader)
		pr.streams[i] = v
	}

	return nil
}
