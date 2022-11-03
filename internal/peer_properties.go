package internal

import (
	"bufio"
	"bytes"
	"fmt"
)

type PeerPropertiesRequest struct {
	clientProperties map[string]string
	correlationId    uint32
}

func NewPeerPropertiesRequest() *PeerPropertiesRequest {
	p := &PeerPropertiesRequest{
		clientProperties: make(map[string]string, 6),
	}
	p.clientProperties["connection_name"] = "go-stream-locator"
	p.clientProperties["product"] = "RabbitMQ Stream"
	p.clientProperties["copyright"] = "Copyright (c) 2021 VMware, Inc. or its affiliates."
	p.clientProperties["information"] = "Licensed under the MPL 2.0. See https://www.rabbitmq.com/"
	//c.clientProperties.items["version"] = ClientVersion
	p.clientProperties["platform"] = "Golang"
	return p
}

func (p *PeerPropertiesRequest) Write(writer *bufio.Writer) (int, error) {
	return writeMany(writer, p.CorrelationId(), len(p.clientProperties), p.clientProperties)

}

func (p *PeerPropertiesRequest) SizeNeeded() int {
	/* Map Encoded as follows:
	map = [key value]*
	key = key_length + key_content
	value = value_length + value_content
	_________________________________________
	| map_size		| 4 bytes				|
	| key_length	| 2 bytes				|
	| key_content	| key_length bytes		|
	| value_length	| 2 bytes				|
	| value_content	| value_length bytes	|
	-----------------------------------------
	*/
	size := streamProtocolMapLenBytes // size of the map, always there
	for key, element := range p.clientProperties {
		size += streamProtocolMapKeyLengthBytes + len(key) + streamProtocolMapValueLengthBytes + len(element)
	}
	size += streamProtocolKeySizeBytes + streamProtocolVersionSizeBytes + streamProtocolCorrelationIdSizeBytes
	return size
}

func (p *PeerPropertiesRequest) Version() int16 {
	return Version1
}

func (p *PeerPropertiesRequest) SetCorrelationId(id uint32) {
	p.correlationId = id
}

func (p *PeerPropertiesRequest) CorrelationId() uint32 {
	return p.correlationId
}

func (p *PeerPropertiesRequest) Key() uint16 {
	return CommandPeerProperties
}

type PeerPropertiesResponse struct {
	correlationId    uint32
	responseCode     uint16
	ServerProperties map[string]string
}

func (p *PeerPropertiesResponse) ResponseCode() uint16 {
	return p.responseCode
}

func (p *PeerPropertiesResponse) MarshalBinary() (data []byte, err error) {
	buff := new(bytes.Buffer)
	wr := bufio.NewWriter(buff)

	n, err := writeMany(wr, p.correlationId, p.responseCode, len(p.ServerProperties), p.ServerProperties)
	if err != nil {
		return nil, err
	}

	expectedBytesWritten := streamProtocolCorrelationIdSizeBytes + streamProtocolResponseCodeSizeBytes +
		streamProtocolMapLenBytes + (4 * len(p.ServerProperties)) // 2B for key len + 2B value len * num. of items
	for k, v := range p.ServerProperties {
		expectedBytesWritten += len(k) + len(v)
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

func NewPeerPropertiesResponseWith(correlationId uint32, responseCode uint16, serverProperties map[string]string) *PeerPropertiesResponse {
	return &PeerPropertiesResponse{correlationId: correlationId, responseCode: responseCode, ServerProperties: serverProperties}
}

func NewPeerPropertiesResponse() *PeerPropertiesResponse {
	return &PeerPropertiesResponse{ServerProperties: make(map[string]string)}
}

func (p *PeerPropertiesResponse) Read(reader *bufio.Reader) error {
	var serverPropertiesCount uint32
	err := readMany(reader, &p.correlationId, &p.responseCode, &serverPropertiesCount)
	if err != nil {
		return err
	}

	for i := uint32(0); i < serverPropertiesCount; i++ {
		key := readString(reader)
		value := readString(reader)
		p.ServerProperties[key] = value
	}
	return nil
}

func (p *PeerPropertiesResponse) CorrelationId() uint32 {
	return p.correlationId
}
