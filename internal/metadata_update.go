package internal

import "bufio"

// MetadataUpdateResponse contains a code to identify information, and the stream associated
// with the metadata.
type MetadataUpdateResponse struct {
	metadataInfo metadataInfo
}

type metadataInfo struct {
	code   uint16
	stream string
}

func (m *MetadataUpdateResponse) Key() uint16 {
	return CommandMetadataUpdate
}

func (m *MetadataUpdateResponse) MinVersion() int16 {
	return Version1
}

func (m *MetadataUpdateResponse) MaxVersion() int16 {
	return Version1
}

func (m *MetadataUpdateResponse) Read(rd *bufio.Reader) error {
	return readMany(rd, &m.metadataInfo.code, &m.metadataInfo.stream)
}
