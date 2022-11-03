package internal

import (
	"bufio"
	"bytes"
	"fmt"
)

type OpenRequest struct {
	correlationId uint32
	virtualHost   string
}

func NewOpenRequest(virtualHost string) *OpenRequest {
	return &OpenRequest{virtualHost: virtualHost}
}

func (o *OpenRequest) Write(writer *bufio.Writer) (int, error) {
	n, err := writeMany(writer, o.correlationId, o.virtualHost)
	if err != nil {
		return 0, err
	}
	expectedBytesWritten := streamProtocolCorrelationIdSizeBytes +
		streamProtocolStringLenSizeBytes +
		len(o.virtualHost)
	if n != expectedBytesWritten {
		return n, fmt.Errorf("did not write expected amount of bytes: wrote %d expected %d", n, expectedBytesWritten)
	}
	return n, nil
}

func (o *OpenRequest) Key() uint16 {
	return CommandOpen
}

func (o *OpenRequest) SizeNeeded() int {
	return streamProtocolKeySizeBytes +
		streamProtocolVersionSizeBytes +
		streamProtocolCorrelationIdSizeBytes +
		streamProtocolStringLenSizeBytes +
		len(o.virtualHost)
}

func (o *OpenRequest) SetCorrelationId(correlationId uint32) {
	o.correlationId = correlationId
}

func (o *OpenRequest) CorrelationId() uint32 {
	return o.correlationId
}

func (o *OpenRequest) Version() int16 {
	return Version1
}

type OpenResponse struct {
	correlationId        uint32
	responseCode         uint16
	connectionProperties map[string]string
}

func (o *OpenResponse) MarshalBinary() (data []byte, err error) {
	buff := new(bytes.Buffer)
	wr := bufio.NewWriter(buff)

	n, err := writeMany(wr, o.correlationId, o.responseCode)
	if err != nil {
		return nil, err
	}
	if n != 6 {
		return nil, fmt.Errorf("error in binary marshal: wrote %d expected %d", n, 6)
	}

	if o.connectionProperties != nil && len(o.connectionProperties) > 0 {
		// FIXME: check how many bytes were written
		_, err = writeMany(wr, uint32(len(o.connectionProperties)), o.connectionProperties)
	}

	err = wr.Flush()
	if err != nil {
		return nil, err
	}

	data = buff.Bytes()
	return
}

func NewOpenResponseWith(correlationId uint32, responseCode uint16, connectionProperties map[string]string) *OpenResponse {
	return &OpenResponse{correlationId: correlationId, responseCode: responseCode, connectionProperties: connectionProperties}
}

func (o *OpenResponse) ResponseCode() uint16 {
	return o.responseCode
}

func (o *OpenResponse) ConnectionProperties() map[string]string {
	return o.connectionProperties
}

func (o *OpenResponse) Read(reader *bufio.Reader) error {
	err := readMany(reader, &o.correlationId, &o.responseCode)
	if err != nil {
		return err
	}

	var mapLen uint32
	err = readAny(reader, &mapLen)
	if err != nil {
		return err
	}
	o.connectionProperties = make(map[string]string, mapLen)

	for i := uint32(0); i < mapLen; i++ {
		var key, value string
		err = readMany(reader, &key, &value)
		if err != nil {
			return err
		}
		o.connectionProperties[key] = value
	}

	return nil
}

func (o *OpenResponse) CorrelationId() uint32 {
	return o.correlationId
}
