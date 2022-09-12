package internal

import (
	"bufio"
)

const CommandPeerProperties = 17

type PeerPropertiesRequest struct {
	clientProperties map[string]string
	key              uint16
	correlationId    uint32
}

func NewPeerPropertiesRequest() *PeerPropertiesRequest {
	p := &PeerPropertiesRequest{clientProperties: make(
		map[string]string,
		CommandPeerProperties,
	)}
	p.clientProperties["connection_name"] = "go-stream-locator"
	p.clientProperties["product"] = "RabbitMQ Stream"
	p.clientProperties["copyright"] = "Copyright (c) 2021 VMware, Inc. or its affiliates."
	p.clientProperties["information"] = "Licensed under the MPL 2.0. See https://www.rabbitmq.com/"
	//c.clientProperties.items["version"] = ClientVersion
	p.clientProperties["platform"] = "Golang"
	return p
}
func (p *PeerPropertiesRequest) Write(writer *bufio.Writer) (int, error) {
	return WriteMany(writer, p.GetCorrelationId(), len(p.clientProperties), p.clientProperties)

}

func (p *PeerPropertiesRequest) SizeNeeded() int {
	size := 4 // size of the map, always there
	for key, element := range p.clientProperties {
		size = size + 2 + len(key) + 2 + len(element)
	}
	size = size + 2 + 2 + 4
	return size
}

func (p *PeerPropertiesRequest) SetCorrelationId(id uint32) {
	p.correlationId = id
}

func (p *PeerPropertiesRequest) GetCorrelationId() uint32 {
	return p.correlationId
}

func (p *PeerPropertiesRequest) GetKey() uint16 {
	return CommandPeerProperties
}

type PeerPropertiesResponse struct {
	ServerProperties map[string]string
	correlationId    uint32
	responseCode     uint16
}

func NewPeerPropertiesResponse() *PeerPropertiesResponse {
	return &PeerPropertiesResponse{ServerProperties: make(
		map[string]string,
		CommandPeerProperties)}
}

func (p *PeerPropertiesResponse) Read(reader *bufio.Reader) {

	var serverPropertiesCount uint32
	err := ReadMany(reader, &p.correlationId, &p.responseCode, &serverPropertiesCount)
	p.responseCode = UShortExtractResponseCode(p.responseCode)
	MaybeLogError(err)

	for i := 0; i < int(serverPropertiesCount); i++ {
		key := ReadString(reader)
		value := ReadString(reader)
		p.ServerProperties[key] = value
	}
}

func (p *PeerPropertiesResponse) GetCorrelationId() uint32 {
	return p.correlationId
}
