package internal

import (
	"bufio"
	"bytes"
	"fmt"
)

type MetadataQuery struct {
	correlationId uint32
	stream        string
}

const LenNilMetaDataResponse = 22

func NewMetadataQuery(stream string) *MetadataQuery {
	return &MetadataQuery{stream: stream}
}

func (m *MetadataQuery) UnmarshalBinary(data []byte) error {
	return readMany(bytes.NewReader(data), &m.correlationId, &m.stream)
}

func (m *MetadataQuery) CorrelationId() uint32 {
	return m.correlationId
}

func (m *MetadataQuery) SetCorrelationId(id uint32) {
	m.correlationId = id
}

func (m *MetadataQuery) Stream() string {
	return m.stream
}

func (m *MetadataQuery) Key() uint16 {
	return CommandMetadata
}

func (m *MetadataQuery) SizeNeeded() int {
	return streamProtocolKeySizeBytes +
		streamProtocolVersionSizeBytes +
		streamProtocolCorrelationIdSizeBytes +
		streamProtocolStringLenSizeBytes +
		len(m.stream)
}

func (m *MetadataQuery) Write(w *bufio.Writer) (int, error) {
	n, err := writeMany(w, m.correlationId, m.stream)
	if err != nil {
		return n, err
	}

	expectedBytesWritten := streamProtocolCorrelationIdSizeBytes +
		streamProtocolStringLenSizeBytes +
		len(m.stream)
	if n != expectedBytesWritten {
		return n, fmt.Errorf("did not write expected amount of bytes: wrote %d expected %d", n, expectedBytesWritten)
	}

	return n, nil
}

func (m *MetadataQuery) Version() int16 {
	return Version1
}

type MetadataResponse struct {
	correlationId  uint32
	broker         Broker
	streamMetadata StreamMetadata
}

type Broker struct {
	reference uint16
	host      string
	port      uint32
}

type StreamMetadata struct {
	streamName         string
	responseCode       uint16
	leaderReference    uint16
	replicasReferences []uint16
}

func NewMetadataResponse(correlationId,
	port uint32,
	brokerReference,
	responseCode,
	leaderReference uint16,
	host,
	streamName string,
	replicasReferences []uint16,
) *MetadataResponse {
	return &MetadataResponse{
		correlationId: correlationId,
		broker: Broker{
			reference: brokerReference,
			host:      host,
			port:      port,
		},
		streamMetadata: StreamMetadata{
			streamName:         streamName,
			responseCode:       responseCode,
			leaderReference:    leaderReference,
			replicasReferences: replicasReferences,
		},
	}
}

func (m *MetadataResponse) MarshalBinary() (data []byte, err error) {
	buff := &bytes.Buffer{}
	n, err := writeMany(buff,
		m.correlationId,
		m.broker.reference,
		m.broker.host,
		m.broker.port,
		m.streamMetadata.streamName,
		m.streamMetadata.responseCode,
		m.streamMetadata.leaderReference,
		uint32(len(m.streamMetadata.replicasReferences)),
		m.streamMetadata.replicasReferences)

	if err != nil {
		return nil, err
	}

	lenReplicasReferences := len(m.streamMetadata.replicasReferences) * 2
	if n != LenNilMetaDataResponse+len(m.broker.host)+len(m.streamMetadata.streamName)+lenReplicasReferences {
		return nil, errWriteShort
	}

	data = buff.Bytes()
	return
}

func (m *MetadataResponse) Read(reader *bufio.Reader) error {
	err := readMany(reader, &m.correlationId)
	if err != nil {
		return err
	}

	err = readMany(reader, &m.broker.reference, &m.broker.host, &m.broker.port)
	if err != nil {
		return err
	}

	err = readMany(reader,
		&m.streamMetadata.streamName, &m.streamMetadata.responseCode, &m.streamMetadata.leaderReference)
	if err != nil {
		return err
	}

	var refLen uint32
	err = readAny(reader, &refLen)
	if err != nil {
		return err
	}

	m.streamMetadata.replicasReferences = make([]uint16, refLen)

	for i := uint32(0); i < refLen; i++ {
		var value uint16
		err = readAny(reader, &value)
		if err != nil {
			return err
		}
		m.streamMetadata.replicasReferences[i] = value
	}
	return nil
}

func (m *MetadataResponse) CorrelationId() uint32 {
	return m.correlationId
}

func (m *MetadataResponse) ResponseCode() uint16 {
	return m.streamMetadata.responseCode
}
