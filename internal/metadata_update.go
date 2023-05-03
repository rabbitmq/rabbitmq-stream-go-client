package internal

import (
	"bufio"
	"bytes"
)

// MetadataUpdateResponse contains a code to identify information, and the stream associated
// with the metadata.
type MetadataUpdateResponse struct {
	metadataInfo metadataInfo
}

type metadataInfo struct {
	code   uint16
	stream string
}

func NewMetadataUpdateResponse(code uint16, stream string) *MetadataUpdateResponse {
	return &MetadataUpdateResponse{
		metadataInfo: metadataInfo{
			code:   code,
			stream: stream,
		},
	}
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

func (m *MetadataUpdateResponse) Code() uint16 {
	return m.metadataInfo.code
}

func (m *MetadataUpdateResponse) Stream() string {
	return m.metadataInfo.stream
}

func (m *MetadataUpdateResponse) Read(rd *bufio.Reader) error {
	return readMany(rd, &m.metadataInfo.code, &m.metadataInfo.stream)
}

func (m *MetadataUpdateResponse) MarshalBinary() (data []byte, err error) {
	buff := &bytes.Buffer{}
	_, err = writeMany(buff, m.metadataInfo.code, m.metadataInfo.stream)
	if err != nil {
		return nil, err
	}
	data = buff.Bytes()
	return
}
