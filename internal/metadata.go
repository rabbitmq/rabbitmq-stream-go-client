package internal

import (
	"bufio"
	"bytes"
	"fmt"
)

type MetadataQuery struct {
	correlationId uint32
	streams       []string
}

func NewMetadataQuery(streams []string) *MetadataQuery {
	return &MetadataQuery{streams: streams}
}

func (m *MetadataQuery) UnmarshalBinary(data []byte) error {
	return readMany(bytes.NewReader(data), &m.correlationId, &m.streams)
}

func (m *MetadataQuery) CorrelationId() uint32 {
	return m.correlationId
}

func (m *MetadataQuery) SetCorrelationId(id uint32) {
	m.correlationId = id
}

func (m *MetadataQuery) Streams() []string {
	return m.streams
}

func (m *MetadataQuery) Key() uint16 {
	return CommandMetadata
}

func (m *MetadataQuery) SizeNeeded() int {
	size := streamProtocolKeySizeBytes +
		streamProtocolVersionSizeBytes +
		streamProtocolCorrelationIdSizeBytes +
		streamProtocolSliceLenBytes

	for _, stream := range m.streams {
		size += streamProtocolStringLenSizeBytes +
			len(stream)
	}

	return size
}

func (m *MetadataQuery) Write(w *bufio.Writer) (int, error) {
	n, err := writeMany(w, m.correlationId, m.streams)
	if err != nil {
		return n, err
	}

	expectedBytesWritten := streamProtocolCorrelationIdSizeBytes +
		streamProtocolSliceLenBytes
	for _, stream := range m.streams {
		expectedBytesWritten += streamProtocolStringLenSizeBytes +
			len(stream)
	}

	if n != expectedBytesWritten {
		return n, fmt.Errorf("did not write expected amount of bytes: wrote %d expected %d", n, expectedBytesWritten)
	}

	return n, nil
}

func (m *MetadataQuery) Version() int16 {
	return Version1
}

type MetadataResponse struct {
	correlationId   uint32
	brokers         []Broker
	streamsMetadata []StreamMetadata
}

func (m *MetadataResponse) Brokers() []Broker {
	return m.brokers
}

func (m *MetadataResponse) StreamsMetadata() []StreamMetadata {
	return m.streamsMetadata
}

type Broker struct {
	reference uint16
	host      string
	port      uint32
}

func (b *Broker) Reference() uint16 {
	return b.reference
}

func (b *Broker) Host() string {
	return b.host
}

func (b *Broker) Port() uint32 {
	return b.port
}

type StreamMetadata struct {
	streamName         string
	responseCode       uint16
	leaderReference    uint16
	replicasReferences []uint16
}

func (s *StreamMetadata) StreamName() string {
	return s.streamName
}

func (s *StreamMetadata) ResponseCode() uint16 {
	return s.responseCode
}

func (s *StreamMetadata) LeaderReference() uint16 {
	return s.leaderReference
}

func (s *StreamMetadata) ReplicasReferences() []uint16 {
	return s.replicasReferences
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
		brokers: []Broker{
			{
				reference: brokerReference,
				host:      host,
				port:      port,
			},
		},
		streamsMetadata: []StreamMetadata{
			{
				streamName:         streamName,
				responseCode:       responseCode,
				leaderReference:    leaderReference,
				replicasReferences: replicasReferences,
			},
		},
	}
}

func (m *MetadataResponse) MarshalBinary() (data []byte, err error) {
	buff := new(bytes.Buffer)
	wr := bufio.NewWriter(buff)
	bytesWritten := 0

	n, err := writeMany(wr, m.correlationId)
	if err != nil {
		return nil, err
	}
	bytesWritten += n

	n, err = writeMany(wr, len(m.brokers))
	if err != nil {
		return nil, err
	}
	bytesWritten += n

	for _, broker := range m.brokers {
		n, err = writeMany(wr, broker.reference, broker.host, broker.port)
		if err != nil {
			return nil, err
		}
		bytesWritten += n
	}

	n, err = writeMany(wr, len(m.streamsMetadata))
	if err != nil {
		return nil, err
	}
	bytesWritten += n

	for _, streamMetadata := range m.streamsMetadata {
		n, err = writeMany(
			wr,
			streamMetadata.streamName,
			streamMetadata.responseCode,
			streamMetadata.leaderReference,
			uint32(len(streamMetadata.replicasReferences)),
			streamMetadata.replicasReferences,
		)
		if err != nil {
			return nil, err
		}
		bytesWritten += n
	}

	// calculate Bytes that were written and what we expect to write
	expectedBytesWritten := streamProtocolCorrelationIdSizeBytes + streamProtocolSliceLenBytes
	for _, broker := range m.brokers {
		expectedBytesWritten += streamProtocolKeySizeUint16 +
			streamProtocolStringLenSizeBytes +
			len(broker.host) +
			streamProtocolKeySizeUint32
	}

	expectedBytesWritten += streamProtocolSliceLenBytes
	for _, streamMetadata := range m.streamsMetadata {
		expectedBytesWritten += streamProtocolStringLenSizeBytes +
			len(streamMetadata.streamName) +
			streamProtocolKeySizeUint16 +
			streamProtocolKeySizeUint16 +
			streamProtocolSliceLenBytes +
			(len(streamMetadata.replicasReferences) * 2) // 2 bytes for each replicasReference
	}

	if bytesWritten != expectedBytesWritten {
		return nil, fmt.Errorf("did not write expected number of bytes: wanted %d, wrote %d", expectedBytesWritten, bytesWritten)
	}

	if err = wr.Flush(); err != nil {
		return nil, err
	}

	data = buff.Bytes()
	return
}

func (m *MetadataResponse) Read(reader *bufio.Reader) error {
	var lenBrokers uint32
	err := readMany(reader, &m.correlationId, &lenBrokers)
	if err != nil {
		return err
	}

	m.brokers = make([]Broker, lenBrokers)
	// iterate over the brokers and read
	for i := uint32(0); i < lenBrokers; i++ {
		var ref uint16
		var host string
		var port uint32
		err := readMany(reader, &ref, &host, &port)
		if err != nil {
			return err
		}
		m.brokers[i].reference = ref
		m.brokers[i].host = host
		m.brokers[i].port = port
	}

	// read len streamsMetadata
	lenMetadata, err := readUInt(reader)
	if err != nil {
		return err
	}

	m.streamsMetadata = make([]StreamMetadata, lenMetadata)
	// iterate over streamsMetadata and read
	for i := uint32(0); i < lenMetadata; i++ {
		var streamName string
		var responseCode uint16
		var leaderReference uint16
		var replicasLen uint32
		var replicasRefs []uint16

		err := readMany(reader, &streamName, &responseCode, &leaderReference, &replicasLen)
		if err != nil {
			return err
		}

		for j := uint32(0); j < replicasLen; j++ {
			var ref uint16
			ref, err = readUShort(reader)
			if err != nil {
				return err
			}
			replicasRefs = append(replicasRefs, ref)
		}
		m.streamsMetadata[i].streamName = streamName
		m.streamsMetadata[i].responseCode = responseCode
		m.streamsMetadata[i].leaderReference = leaderReference
		m.streamsMetadata[i].replicasReferences = replicasRefs
	}

	return nil
}

func (m *MetadataResponse) CorrelationId() uint32 {
	return m.correlationId
}

func (m *MetadataResponse) ResponseCode() uint16 {
	// ResponseCodeOK, means the request to fetch metadata was successful.
	// Determining if each individual stream is responding OK is the responsibility of the caller.
	return 1
}
